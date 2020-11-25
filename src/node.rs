#[derive(Clone)]
pub struct Node {
    pub db: sled::Db
}
impl Node {
    pub fn default() -> Node {
        let tree = sled::open("/tmp/welcome-to-sled").expect("open");

        return Node{
            db: tree

        }
    }
}