use sled::{IVec, Result as SledResult};

use crate::NodeConfig;
#[derive(Clone)]
pub struct Database {
    pub db: sled::Db,
}

impl Database {
    pub fn default(node_config: NodeConfig) -> Database {
        let db_path = node_config.db_path;
        let config = sled::Config::default()
            .path(db_path)
            .cache_capacity(10_000_000_000)
            .flush_every_ms(Some(1000))
            .mode(sled::Mode::HighThroughput);
        let tree = config.open().expect("open");

        Database { db: tree }
    }
    pub fn query(&self, key: &[u8]) -> SledResult<Option<IVec>> {
        self.db.get(key)
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> SledResult<Option<IVec>> {
        self.db.insert(key, value)
    }
}
