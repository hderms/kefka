use serde::Deserialize;
use sled::{IVec, Result as SledResult};

use replication::replicator_client::ReplicatorClient;
use replication::{UpdateAck, UpdateRequest};
use std::collections::HashSet;
use tokio::sync::Mutex;
use tonic::Status;
pub mod replication {
    tonic::include_proto!("replication");
}
#[derive(Clone)]
pub struct DbNode {
    pub db: sled::Db,
}

impl DbNode {
    pub fn default(node_config: NodeConfig) -> DbNode {
        let db_path = node_config.db_path;
        let config = sled::Config::default()
            .path(db_path)
            .cache_capacity(10_000_000_000)
            .flush_every_ms(Some(1000))
            .mode(sled::Mode::HighThroughput);
        let tree = config.open().expect("open");

        DbNode { db: tree }
    }
    pub fn query(&self, key: &[u8]) -> SledResult<Option<IVec>> {
        self.db.get(key)
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> SledResult<Option<IVec>> {
        self.db.insert(key, value)
    }
}
#[derive(Deserialize, Clone, Debug)]
pub struct NodeConfig {
    pub next_addr: Option<String>,
    pub prev_addr: Option<String>,
    pub bind_addr: String,
    pub db_path: String,
}
//next_client: Option<Mutex<ReplicatorClient<tonic::transport::Channel>>>,

pub struct ReplicationNode {
    pub node: DbNode,
    next_addr: Option<String>,
    prev_addr: Option<String>,
    next_client: Mutex<Option<ReplicatorClient<tonic::transport::Channel>>>,
    prev_client: Mutex<Option<ReplicatorClient<tonic::transport::Channel>>>,
    sent: Mutex<HashSet<String>>,
    pending: Mutex<HashSet<String>>,
}

impl ReplicationNode {
    pub fn default(node_config: NodeConfig, node: DbNode) -> ReplicationNode {
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

pub struct QueryNode {
    pub node: DbNode,
}
