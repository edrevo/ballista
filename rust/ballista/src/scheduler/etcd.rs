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

use crate::error::{ballista_error, Result};
use crate::scheduler::ConfigBackendClient;

use etcd_client::GetOptions;
use log::warn;

/// A [`ConfigBackendClient`] implementation that uses etcd to save cluster configuration.
#[derive(Clone)]
pub struct EtcdClient {
    etcd: etcd_client::Client,
}

impl EtcdClient {
    pub fn new(etcd: etcd_client::Client) -> Self {
        Self { etcd }
    }
}

#[tonic::async_trait]
impl ConfigBackendClient for EtcdClient {
    async fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .etcd
            .get(key, None)
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .get(0)
            .map(|kv| kv.value().to_owned())
            .unwrap_or_default())
    }

    async fn get_from_prefix(&mut self, prefix: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .etcd
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| kv.value().to_owned())
            .collect())
    }

    async fn put(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        self.etcd
            .put(key.clone(), value.clone(), None)
            .await
            .map_err(|e| {
                warn!("etcd put failed: {}", e);
                ballista_error("etcd put failed")
            })
            .map(|_| ())
    }
}
