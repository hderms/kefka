
use uuid::Uuid;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tonic::{transport::Server, Request, Response, Status};
use replication::replicator_server::{Replicator,ReplicatorServer};
use replication::{UpdateRequest, UpdateReply};
use replication::querier_server::{Querier,QuerierServer};
use replication::{QueryRequest, QueryReply};
use sled::IVec;
use std::borrow::Borrow;
use sled::Db;
mod node;
pub use node::Node;


pub mod replication {
    tonic::include_proto!("replication");
}
#[tonic::async_trait]
impl Replicator for Node {
    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());
        let message = request.into_inner();
        let id = message.id;
        let key = message.key;
        let value = message.value;
        if (key.is_empty() || value.is_empty()) {
            return Result::Err( Status::invalid_argument("empty value provided for key or value"));
        } 
          let result = self.db.insert(key.as_bytes(), value.as_bytes());


          match result {
              Ok(_) => {
                let reply = replication::UpdateReply {
                    id: id
                };
                Ok(Response::new(reply))

              },
              Err(e) => {
                  Result::Err(Status::internal(e.to_string()))
              }
          }

        

    }
}

#[tonic::async_trait]
impl Querier for Node {
    async fn get(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());
        let message = request.into_inner();
        let id = message.id;
        let key = message.key;
        if (key.is_empty() ) {
            return Result::Err( Status::invalid_argument("empty value provided for key "));
        } 
          let result = self.db.get(key.as_bytes());


          match result {
              Ok(Some(value)) => {
                let valueBytes: &[u8]= value.borrow();
                let valueString = String::from_utf8(valueBytes.to_vec());
                match valueString {
                    Ok(s) => {

                        let reply = replication::QueryReply {
                            id: id,
                            key: key,
                            value: s
                        };
                        Ok(Response::new(reply))
                    },
                    Err(e) => {
                        println!("could not get data from DB");
                        println!("{}", e.to_string());
                        Result::Err(Status::internal("Could not convert data from DB to utf8 string"))
                    }
                }

              },

              Ok(None) => {
                        println!("key not found");
                  Result::Err(Status::not_found("key not found"))
              }
              Err(e) => {
                        println!("total error");
                        println!("{}", e.to_string());
                  Result::Err(Status::internal(e.to_string()))
              }
          }

        

    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();
    let node = Node::default();

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(ReplicatorServer::new(node.clone()))
        .add_service(QuerierServer::new(node.clone()))
        .serve(addr)
        .await?;

    Ok(())
}
