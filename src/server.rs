extern crate pretty_env_logger;
use replication::querier_server::{Querier, QuerierServer};
use replication::replicator_server::{Replicator, ReplicatorServer};
use replication::{QueryReply, QueryRequest};
use replication::{UpdateReply, UpdateRequest};
use tonic::{transport::Server, Request, Response, Status};

use std::borrow::Borrow;
mod node;
pub use node::NodeConfig;
pub use node::{DbNode, QueryNode, ReplicationNode};

pub mod replication {
    tonic::include_proto!("replication");
}
#[tonic::async_trait]
impl Replicator for ReplicationNode {
    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateReply>, Status> {
        let message = request.into_inner();
        let id = message.id;
        let key = message.key;
        let value = message.value;
        if (key.is_empty() || value.is_empty()) {
            return Result::Err(Status::invalid_argument(
                "empty value provided for key or value",
            ));
        }
        let result = self.node.insert(key.as_bytes(), value.as_bytes());
        self.replicate(id.clone(), key.clone(), value.clone())
            .await?;

        match result {
            Ok(_) => {
                let reply = replication::UpdateReply { id: id };
                Ok(Response::new(reply))
            }
            Err(e) => Result::Err(Status::internal(e.to_string())),
        }
    }
}
#[tonic::async_trait]
impl Querier for QueryNode {
    async fn get(&self, request: Request<QueryRequest>) -> Result<Response<QueryReply>, Status> {
        let message = request.into_inner();
        let id = message.id;
        let key = message.key;
        if (key.is_empty()) {
            return Result::Err(Status::invalid_argument("empty value provided for key "));
        }
        let result = self.node.query(key.as_bytes());

        return match result {
            Ok(Some(value)) => {
                let value_bytes: &[u8] = value.borrow();
                let value_string = String::from_utf8(value_bytes.to_vec());
                match value_string {
                    Ok(s) => reply_success(id, key, s),
                    Err(_) => Result::Err(Status::internal(
                        "Could not convert data from DB to utf8 string",
                    )),
                }
            }

            Ok(None) => Result::Err(Status::not_found("key not found")),
            Err(e) => Result::Err(Status::internal(e.to_string())),
        };
    }
}

fn reply_success(id: String, key: String, value: String) -> Result<Response<QueryReply>, Status> {
    let reply = replication::QueryReply {
        id: id,
        key: key,
        value: value,
    };
    return Ok(Response::new(reply));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let node_config = match envy::from_env::<NodeConfig>() {
        Ok(config) => config,
        Err(error) => panic!("{:#?}", error),
    };
    let addr = node_config.bind_addr.parse().unwrap();

    let node = DbNode::default(node_config);

    println!("ReplicatorServer listening on {}", addr);
    let replication_node = ReplicationNode::default(node.clone()).await?;
    let query_node = QueryNode { node: node.clone() };

    Server::builder()
        .add_service(ReplicatorServer::new(replication_node))
        .add_service(QuerierServer::new(query_node))
        .serve(addr)
        .await?;

    Ok(())
}
