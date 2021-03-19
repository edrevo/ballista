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

//! Etcd config backend.

use std::time::Duration;

use crate::state::ConfigBackendClient;
use ballista_core::error::{ballista_error, Result};

use etcd_client::{GetOptions, LockResponse};
use log::{error, warn};
use rand::{Rng, distributions::Alphanumeric, thread_rng};

use super::Lock;

/// A [`ConfigBackendClient`] implementation that uses etcd to save cluster configuration.
#[derive(Clone)]
pub struct EtcdClient {
    etcd: etcd_client::Client,
    id: Vec<u8>
}

impl EtcdClient {
    pub fn new(etcd: etcd_client::Client) -> Self {
        let id: Vec<u8> = {
            let mut rng = thread_rng();
            std::iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .take(7)
                .collect()
        };
        Self { etcd, id }
    }
}

#[tonic::async_trait]
impl ConfigBackendClient for EtcdClient {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .etcd
            .clone()
            .get(key, None)
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .get(0)
            .map(|kv| kv.value().to_owned())
            .unwrap_or_default())
    }

    async fn get_from_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(self
            .etcd
            .clone()
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| (kv.key_str().unwrap().to_owned(), kv.value().to_owned()))
            .collect())
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<()> {
        let mut etcd = self.etcd.clone();
        etcd.put(key.clone(), value.clone(), None)
            .await
            .map_err(|e| {
                warn!("etcd put failed: {}", e);
                ballista_error("etcd put failed")
            })
            .map(|_| ())
    }

    async fn lock(&self) -> Result<Box<dyn Lock>> {
        let mut etcd = self.etcd.clone();
        // TODO: make this a namespaced-lock
        let lock = etcd
            .lock("/ballista_global_lock", None)
            .await
            .map_err(|e| {
                warn!("etcd lock failed: {}", e);
                ballista_error("etcd lock failed")
            })?;
        Ok(Box::new(EtcdLockGuard { etcd, lock }))
    }

    async fn leader(&self, campaign_name: &str) {
        let mut etcd = self.etcd.clone();
        let lease = match etcd.lease_grant(60, Default::default()).await {
            Err(e) => {
                error!("Error received while requesting etcd lease. This scheduler will never act as a leader. Error: {}", e);
                return;
            }
            Ok(lease) => lease,
        };
        loop {
            if let Err(e) = etcd
                .campaign(campaign_name, self.id.clone(), lease.id())
                .await
            {
                warn!("Campaign failed: {}", e);
                tokio::time::sleep(Duration::from_secs(15)).await;
                continue;
            }

            loop {
                tokio::time::sleep(Duration::from_secs(45)).await;
                if let Err(e) = etcd.lease_keep_alive(lease.id()).await {
                    warn!("Lease keep alive failed. Going back to campaign mode. Error: {}", e);
                    break;
                }
            }
        }
    }

    async fn is_leader(&self, campaign_name: &str) -> Result<bool> {
        let mut etcd = self.etcd.clone();
        let leader_response = etcd.leader(campaign_name).await.map_err(|e| {
            warn!("etcd leader query failed: {}", e);
            ballista_error("etcd leader query failed")
        })?;
        Ok(leader_response.kv().map(|kv| kv.value() == self.id).unwrap_or(false))
    }
}

struct EtcdLockGuard {
    etcd: etcd_client::Client,
    lock: LockResponse,
}

// Cannot use Drop because we need this to be async
#[tonic::async_trait]
impl Lock for EtcdLockGuard {
    async fn unlock(&mut self) {
        self.etcd.unlock(self.lock.key()).await.unwrap();
    }
}
