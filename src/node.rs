use std::net::SocketAddr;

use serde::Deserialize;
use sled::{IVec, Result};

use replication::replicator_client::ReplicatorClient;
use replication::{UpdateRequest, UpdateReply};
pub mod replication {
    tonic::include_proto!("replication");
}
#[derive(Clone)]
pub struct Node {
    pub db: sled::Db,
    pub next: Option<SocketAddr>,
}

impl Node {
    pub fn default(node_config: NodeConfig) -> Node {
        let tree = sled::open("/tmp/kefka.db").expect("open");
        info!("Node starting with config:  {:?}", node_config);

        return Node {
            db: tree,
            next: node_config.next_addr,
        };
    }
    pub fn query(&self, key: &[u8]) -> Result<Option<IVec>> {
        return self.db.get(key);
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<IVec>> {
        return self.db.insert(key, value);
    }
}
#[derive(Deserialize, Debug)]
pub struct NodeConfig {
    pub next_addr: Option<Ipv4Addr>,
}
