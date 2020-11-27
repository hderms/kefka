use replication::replicator_client::ReplicatorClient;
use replication::{UpdateRequest, UpdateReply};

pub mod replication {
    tonic::include_proto!("replication");
}
#[tokio_main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

  let mut client = ReplicatorClient::connect("http://[::1]:50051").await?;
}
