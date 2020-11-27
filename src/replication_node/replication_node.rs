use replication::replicator_client::ReplicatorClient;
use replication::{UpdateAck, UpdateRequest};
use tokio::sync::Mutex;
use tonic::Status;

use crate::NodeConfig;
use crate::Database;
use super::messages::Messages;

pub mod replication {
    tonic::include_proto!("replication");
}

pub struct ReplicationNode {
    pub node: Database,
    next_addr: Option<String>,
    prev_addr: Option<String>,
    next_client: Mutex<Option<ReplicatorClient<tonic::transport::Channel>>>,
    prev_client: Mutex<Option<ReplicatorClient<tonic::transport::Channel>>>,
    message_base: Messages
}


impl ReplicationNode {
    pub fn default(node_config: NodeConfig, node: Database) -> ReplicationNode {
        let message_base = Messages::default();

        ReplicationNode {
            next_client: Mutex::new(None),
            prev_client: Mutex::new(None),
            next_addr: node_config.next_addr,
            prev_addr: node_config.prev_addr,
            node,
            message_base
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
                    self.message_base.add_pending(id.clone()).await;
                    self.message_base.add_sent(id.clone()).await;
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
            self.message_base.remove_pending(id.clone()).await;
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
