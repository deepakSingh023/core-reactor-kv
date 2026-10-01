use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::transport::Server;

use crate::raft_proto::raft_service_server::RaftServiceServer;
use crate::raft::service::RaftServiceImpl;
use crate::raft::state::RaftState;

pub async fn run_server(
    address: SocketAddr,
    state: Arc<Mutex<RaftState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = RaftServiceImpl { state };

    println!("Raft gRPC server listening on {}", address);

    Server::builder()
        .add_service(RaftServiceServer::new(service))
        .serve(address)
        .await?;

    Ok(())
}