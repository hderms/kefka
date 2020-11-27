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
        println!("replicating key {}...", key.clone());
        {
            let mut sent = self.sent.lock().await;
            sent.insert(id.clone());
            println!("sent {:?}", sent);
        }

        {
            let mut pending = self.pending.lock().await;
            pending.insert(id.clone());
            println!("pending {:?}", pending);
        }
        {
            let mut client = self.next_client.lock().await;

            match *client {
                Some(ref mut c) => {
                    c.update(request).await?;
                    Ok(())
                }
                None => match self.next_addr.clone() {
                    Some(addr) => {
                        let next_client = ReplicatorClient::connect(addr).await;
                        match next_client {
                            Ok(c) => {
                                c.clone().update(request).await?;
                                *client = Some(c.clone());
                                Ok(())
                            }
                            Err(e) => Err(Status::internal(e.to_string())),
                        }
                    }
                    None => Ok(()),
                },
            }
        }
    }

    pub async fn ack(&self, id: String) -> Result<(), Status> {
        let request = tonic::Request::new(UpdateAck { id: id.clone() });

        {
            let mut pending = self.pending.lock().await;
            pending.remove(id.clone().as_str());
            println!("pending {:?}", pending);
        }

        {
            let mut client = self.prev_client.lock().await;

            match *client {
                Some(ref mut c) => {
                    c.ack(request).await?;
                    Ok(())
                }
                None => match self.prev_addr.clone() {
                    Some(addr) => {
                        let prev_client = ReplicatorClient::connect(addr).await;
                        match prev_client {
                            Ok(c) => {
                                *client = Some(c.clone());
                                c.clone().ack(request).await?;
                                Ok(())
                            }
                            Err(e) => Err(Status::internal(e.to_string())),
                        }
                    }
                    None => Ok(()),
                },
            }
        }
    }
}
