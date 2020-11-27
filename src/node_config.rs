use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct NodeConfig {
    pub next_addr: Option<String>,
    pub prev_addr: Option<String>,
    pub bind_addr: String,
    pub db_path: String,
}
