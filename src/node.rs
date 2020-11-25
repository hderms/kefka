use sled::{IVec, Result};

#[derive(Clone)]
pub struct Node {
    pub db: sled::Db,
}
impl Node {
    pub fn default() -> Node {
        let tree = sled::open("/tmp/welcome-to-sled").expect("open");

        return Node { db: tree };
    }
    pub fn query(&self, key: &[u8]) -> Result<Option<IVec>> {
        return self.db.get(key);
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<IVec>> {
        return self.db.insert(key, value);
    }
}
