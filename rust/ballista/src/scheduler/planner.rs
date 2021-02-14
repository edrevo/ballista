// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Distributed query execution
//!
//! This code is EXPERIMENTAL and still under development

use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::execution_plans::{self, QueryStageExec, ShuffleReaderExec, UnresovledShuffleExec};
use crate::client::BallistaClient;
use crate::context::DFTableAdapter;
use crate::error::{BallistaError, Result};
use crate::executor::collect::CollectExec;
use crate::serde::scheduler::ExecutorMeta;
use crate::serde::scheduler::PartitionId;
use crate::utils;

use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use log::{debug, info};
use uuid::Uuid;

type SendableExecutionPlan = Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send>>;
type PartialQueryStageResult = (Arc<dyn ExecutionPlan>, Vec<Arc<QueryStageExec>>);

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub(crate) partition_id: PartitionId,
    pub(crate) executor_meta: ExecutorMeta,
}

pub struct DistributedPlanner {
    executors: Vec<ExecutorMeta>,
    next_stage_id: usize,
}

impl DistributedPlanner {
    pub fn new(executors: Vec<ExecutorMeta>) -> Result<Self> {
        if executors.is_empty() {
            Err(BallistaError::General(
                "DistributedPlanner requires at least one executor".to_owned(),
            ))
        } else {
            Ok(Self {
                executors,
                next_stage_id: 0,
            })
        }
    }
}

impl DistributedPlanner {
    /// Execute a logical plan using distributed query execution and collect the results into a
    /// vector of [RecordBatch].
    pub async fn collect(
        &mut self,
        logical_plan: &LogicalPlan,
    ) -> Result<SendableRecordBatchStream> {
        let datafusion_ctx = ExecutionContext::new();
        let plan = datafusion_ctx.optimize(logical_plan)?;
        let plan = datafusion_ctx.create_physical_plan(&plan)?;
        let plan = self.execute_distributed_query(plan).await?;
        let plan = Arc::new(CollectExec::new(plan));
        plan.execute(0).await.map_err(|e| e.into())
    }

    /// Execute a distributed query against a cluster, leaving the final results on the
    /// executors. The [ExecutionPlan] returned by this method is guaranteed to be a
    /// [ShuffleReaderExec] that can be used to fetch the final results from the executors
    /// in parallel.
    pub async fn execute_distributed_query(
        &mut self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let job_uuid = Uuid::new_v4();

        let execution_plans = self.calculate_query_stages(&job_uuid, execution_plan)?;

        for plan in &execution_plans {
            pretty_print(plan.clone(), 0);
        }

        execute(execution_plans, self.executors.clone()).await
    }

    /// Returns a vector of ExecutionPlans, where the root node is a [QueryStageExec].
    /// Plans that depend on the input of other plans will have leaf nodes of type [UnresovledShuffleExec].
    /// A [QueryStageExec] is created whenever the partitioning changes.
    ///
    /// Returns an empty vector if the execution_plan doesn't need to be sliced into several stages.
    pub fn calculate_query_stages(
        &mut self,
        job_uuid: &Uuid,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Arc<QueryStageExec>>> {
        let (new_plan, mut stages) =
            self.calculate_query_stages_internal(job_uuid, execution_plan)?;
        stages.push(create_query_stage(
            job_uuid,
            self.next_stage_id(),
            new_plan,
        )?);
        Ok(stages)
    }

    /// Returns a potentially modified version of the input execution_plan along with the resulting query stages.
    /// This function is needed because the input execution_plan might need to be modified, but it might not hold a
    /// compelte query stage (its parent might also belong to the same stage)
    fn calculate_query_stages_internal(
        &mut self,
        job_uuid: &Uuid,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<PartialQueryStageResult> {
        // recurse down and replace children
        if execution_plan.children().is_empty() {
            return Ok((execution_plan, vec![]));
        }

        let mut stages = vec![];
        let mut children = vec![];
        for child in execution_plan.children() {
            let (new_child, mut child_stages) =
                self.calculate_query_stages_internal(&job_uuid, child)?;
            match child_stages.as_slice() {
                [] => children.push(new_child),
                stages => children.push(Arc::new(UnresovledShuffleExec::new(
                    stages.iter().map(|q| q.stage_id).collect(),
                    new_child.schema().clone(),
                    new_child.output_partitioning().partition_count(),
                ))),
            }
            stages.append(&mut child_stages);
        }

        if let Some(adapter) = execution_plan.as_any().downcast_ref::<DFTableAdapter>() {
            let ctx = ExecutionContext::new();
            Ok((ctx.create_physical_plan(&adapter.logical_plan)?, stages))
        } else if execution_plan.as_any().is::<MergeExec>() {
            Ok((children[0].clone(), stages))
        } else if let Some(agg) = execution_plan.as_any().downcast_ref::<HashAggregateExec>() {
            //TODO should insert query stages in more generic way based on partitioning metadata
            // and not specifically for this operator
            match agg.mode() {
                AggregateMode::Final => {
                    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
                    for child in &children {
                        let new_stage =
                            create_query_stage(job_uuid, self.next_stage_id(), child.clone())?;
                        new_children.push(Arc::new(UnresovledShuffleExec::new(
                            vec![new_stage.stage_id],
                            new_stage.schema().clone(),
                            new_stage.output_partitioning().partition_count(),
                        )));
                        stages.push(new_stage);
                    }
                    Ok((agg.with_new_children(new_children)?, stages))
                }
                AggregateMode::Partial => Ok((agg.with_new_children(children)?, stages)),
            }
        } else if let Some(join) = execution_plan.as_any().downcast_ref::<HashJoinExec>() {
            Ok((join.with_new_children(children)?, stages))
        } else {
            // TODO check for compatible partitioning schema, not just count
            if execution_plan.output_partitioning().partition_count()
                != children[0].output_partitioning().partition_count()
            {
                let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
                for child in &children {
                    let new_stage =
                        create_query_stage(job_uuid, self.next_stage_id(), child.clone())?;
                    new_children.push(Arc::new(UnresovledShuffleExec::new(
                        vec![new_stage.stage_id],
                        new_stage.schema().clone(),
                        new_stage.output_partitioning().partition_count(),
                    )));
                    stages.push(new_stage);
                }
                Ok((execution_plan.with_new_children(new_children)?, stages))
            } else {
                Ok((execution_plan.with_new_children(children)?, stages))
            }
        }
    }

    /// Generate a new stage ID
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

fn execute(
    stages: Vec<Arc<QueryStageExec>>,
    executors: Vec<ExecutorMeta>,
) -> SendableExecutionPlan {
    Box::pin(async move {
        let mut partition_locations: HashMap<usize, Vec<PartitionLocation>> = HashMap::new();
        let mut result_partition_locations = vec![];
        for stage in &stages {
            debug!("execute() {}", &format!("{:?}", stage)[0..60]);
            let stage = remove_unresolved_shuffles(stage.as_ref(), &partition_locations)?;
            let stage = stage.as_any().downcast_ref::<QueryStageExec>().unwrap();
            result_partition_locations = execute_query_stage(
                &stage.job_uuid.clone(),
                stage.stage_id,
                stage.children()[0].clone(),
                executors.clone(),
            )
            .await?;
            partition_locations.insert(stage.stage_id, result_partition_locations.clone());
        }

        let shuffle_reader: Arc<dyn ExecutionPlan> = Arc::new(ShuffleReaderExec::try_new(
            result_partition_locations,
            stages.last().unwrap().schema(),
        )?);
        Ok(shuffle_reader)
    })
}

fn remove_unresolved_shuffles(
    stage: &dyn ExecutionPlan,
    partition_locations: &HashMap<usize, Vec<PartitionLocation>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in stage.children() {
        if let Some(unresolved_shuffle) = child.as_any().downcast_ref::<UnresovledShuffleExec>() {
            let relevant_locations: Vec<_> = unresolved_shuffle
                .query_stage_ids
                .iter()
                .flat_map(|id| partition_locations[id].clone())
                .collect();
            new_children.push(Arc::new(ShuffleReaderExec::try_new(
                relevant_locations,
                unresolved_shuffle.schema().clone(),
            )?))
        } else {
            new_children.push(remove_unresolved_shuffles(
                child.as_ref(),
                partition_locations,
            )?);
        }
    }
    Ok(stage.with_new_children(new_children)?)
}

fn create_query_stage(
    job_uuid: &Uuid,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<QueryStageExec>> {
    Ok(Arc::new(QueryStageExec::try_new(
        *job_uuid, stage_id, plan,
    )?))
}

/// Execute a query stage by sending each partition to an executor
async fn execute_query_stage(
    job_uuid: &Uuid,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    executors: Vec<ExecutorMeta>,
) -> Result<Vec<PartitionLocation>> {
    info!("execute_query_stage() stage_id={}", stage_id);
    pretty_print(plan.clone(), 0);

    let _job_uuid = *job_uuid;
    let partition_count = plan.output_partitioning().partition_count();
    let mut meta = Vec::with_capacity(partition_count);
    for child_partition in 0..partition_count {
        debug!(
            "execute_query_stage() stage_id={}, partition_id={}",
            stage_id, child_partition
        );
        let executor_meta = &executors[child_partition % executors.len()];
        meta.push(PartitionLocation {
            partition_id: PartitionId::new(_job_uuid, stage_id, child_partition),
            executor_meta: executor_meta.clone(),
        });
    }

    let mut executions = Vec::with_capacity(partition_count);
    for child_partition in 0..partition_count {
        let _plan = plan.clone();
        let _executor_meta = executors[child_partition % executors.len()].clone();
        executions.push(tokio::spawn(async move {
            let mut client =
                BallistaClient::try_new(&_executor_meta.host, _executor_meta.port).await?;
            client
                .execute_partition(_job_uuid, stage_id, child_partition, _plan)
                .await
        }));
    }
    futures::future::join_all(executions).await;

    debug!(
        "execute_query_stage() stage_id={} produced {:?}",
        stage_id, meta
    );

    Ok(meta)
}

pub fn pretty_print(plan: Arc<dyn ExecutionPlan>, indent: usize) {
    let operator_str = format!("{:?}", plan);
    debug!("{}{:?}", "  ".repeat(indent), &operator_str[0..60]);
    plan.children()
        .iter()
        .for_each(|c| pretty_print(c.clone(), indent + 1));
}
