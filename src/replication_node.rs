use crate::Database;
use replication::replicator_client::ReplicatorClient;
use replication::{UpdateAck, UpdateRequest};
use std::collections::HashSet;
use tokio::sync::Mutex;
use tonic::Status;

use crate::NodeConfig;

pub mod replication {
    tonic::include_proto!("replication");
}

pub struct ReplicationNode {
    pub node: Database,
    next_addr: Option<String>,
    prev_addr: Option<String>,
    next_client: Mutex<Option<ReplicatorClient<tonic::transport::Channel>>>,
    prev_client: Mutex<Option<ReplicatorClient<tonic::transport::Channel>>>,
    sent: Mutex<HashSet<String>>,
    pending: Mutex<HashSet<String>>,
}

impl ReplicationNode {
    pub fn default(node_config: NodeConfig, node: Database) -> ReplicationNode {
        let sent = Mutex::new(HashSet::new());
        let pending = Mutex::new(HashSet::new());

        ReplicationNode {
            next_client: Mutex::new(None),
            prev_client: Mutex::new(None),
            next_addr: node_config.next_addr,
            prev_addr: node_config.prev_addr,
            node,
            sent,
            pending,
        }
    }

    pub async fn replicate(&self, id: String, key: String, value: String) -> Result<(), Status> {
        let request = tonic::Request::new(UpdateRequest {
            id: id.clone(),
            key: key.clone(),
            value,
        });

        {
            let client = self.initialize_next_client().await;
            match client {
                Ok(Some(mut c)) => {
                    c.update(request).await?;
                    println!("replicating key {}...", key.clone());
                    self.add_pending(id.clone()).await;
                    self.add_sent(id.clone()).await;
                    Ok(())
                }
                Ok(None) => Ok(()),

                Err(e) => Err(Status::internal(e.to_string())),
            }
        }
    }

    pub async fn ack(&self, id: String) -> Result<(), Status> {
        let request = tonic::Request::new(UpdateAck { id: id.clone() });

        {
            self.remove_pending(id.clone()).await;
            let client = self.initialize_prev_client().await;

            match client {
                Ok(Some(mut c)) => {
                    c.ack(request).await?;
                    Ok(())
                }
                Ok(None) => Ok(()),

                Err(e) => Err(Status::internal(e.to_string())),
            }
        }
    }

    async fn remove_pending(&self, id: String) {
        let mut pending = self.pending.lock().await;
        println!("removing pending");
        pending.remove(id.as_str());
        println!("pending {:?}", pending);
    }

    async fn add_sent(&self, id: String) {
        let mut sent = self.sent.lock().await;
        sent.insert(id.clone());
        println!("adding sent");
        println!("sent {:?}", sent);
    }
    async fn add_pending(&self, id: String) {
        let mut pending = self.pending.lock().await;
        println!("adding pending");
        pending.insert(id);
        println!("pending {:?}", pending);
    }

    async fn initialize_prev_client(
        &self,
    ) -> Result<Option<ReplicatorClient<tonic::transport::Channel>>, tonic::transport::Error> {
        let mut client = self.prev_client.lock().await;

        match &*client {
            Some(c) => Ok(Some(c.clone())),
            None => match self.prev_addr.clone() {
                Some(addr) => {
                    println!("initializing previous client");
                    let prev_client = ReplicatorClient::connect(addr).await;
                    match prev_client {
                        Ok(c) => {
                            *client = Some(c.clone());
                            Ok(Some(c.clone()))
                        }
                        Err(e) => Err(e),
                    }
                }
                None => Ok(None),
            },
        }
    }

    async fn initialize_next_client(
        &self,
    ) -> Result<Option<ReplicatorClient<tonic::transport::Channel>>, tonic::transport::Error> {
        let mut client = self.next_client.lock().await;

        match &*client {
            Some(c) => Ok(Some(c.clone())),
            None => match self.next_addr.clone() {
                Some(addr) => {

                    println!("initializing next client");
                    let next_client = ReplicatorClient::connect(addr).await;
                    match next_client {
                        Ok(c) => {
                            *client = Some(c.clone());
                            Ok(Some(c.clone()))
                        }
                        Err(e) => Err(e),
                    }
                }
                None => Ok(None),
            },
        }
    }
}
