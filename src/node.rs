use std::net::SocketAddr;

use serde::Deserialize;
use sled::{IVec, Result as SledResult};

use replication::replicator_client::ReplicatorClient;
use replication::{UpdateReply, UpdateRequest};
use std::error::Error;
pub mod replication {
    tonic::include_proto!("replication");
}
#[derive(Clone)]
pub struct Node {
    pub db: sled::Db,
    pub next: Option<String>,
}

impl Node {
    pub fn default(node_config: NodeConfig) -> Node {
        let db_path = node_config.db_path;
        let tree = sled::open(db_path).expect("open");

        return Node {
            db: tree,
            next: node_config.next_addr,
        };
    }
    pub fn query(&self, key: &[u8]) -> SledResult<Option<IVec>> {
        return self.db.get(key);
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> SledResult<Option<IVec>> {
        return self.db.insert(key, value);
    }

    pub fn replicate(&self, key: &[u8], value: &[u8]) -> SledResult<Option<IVec>> {
        return self.db.insert(key, value);
    }
}
#[derive(Deserialize, Clone,Debug)]
pub struct NodeConfig {
    pub next_addr: Option<String>,
    pub bind_addr: String,
    pub db_path: String
}

pub struct ReplicationNode {
    pub node: Node,
    client: Option<ReplicatorClient<tonic::transport::Channel>>,
}
impl ReplicationNode {
    pub async fn default(node: Node) -> Result<ReplicationNode, tonic::transport::Error> {
        let next = node.next.clone();
        match next {
            Some(addr) => {
                let mut client: ReplicatorClient<tonic::transport::Channel> =
                    ReplicatorClient::connect(addr).await?;

                return Ok(ReplicationNode {
                    client: Some(client),
                    node: node.clone(),
                });
            }
            None => {
                return Ok(ReplicationNode {
                    client: None,
                    node: node,
                });
            }
        }
    }
}

pub struct QueryNode {
    pub node: Node,
}
