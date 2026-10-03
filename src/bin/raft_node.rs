#[path = "../raft/mod.rs"]
mod raft;

#[path = "../node/mod.rs"]
mod node;

pub mod raft_proto {
    tonic::include_proto!("raft");
}

use std::sync::Arc;

use tokio::sync::Mutex;

use raft::config::cluster_config;
use raft::server::run_server;
use raft::state::RaftState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id: u64 = std::env::args()
       .nth(1)
       .expect("usage: raft_node <node_id>")
       .parse()
       .expect("node_id must be a number");

    let cluster = cluster_config();

    let config = cluster
        .iter()
        .find(|node| node.id == node_id)
        .unwrap();

    let state = RaftState {
        node_id,
        current_term: 0,
        voted_for: None,
        role: raft::state::Role::Follower,
        leader_id: None,
        votes_received: std::collections::HashSet::new(),
        election_elapsed: 0,
        election_timeout: 10,
        heartbeat_elapsed: 0,
        heartbeat_interval: 3,
    };

    let state = Arc::new(Mutex::new(state));

    run_server(config.address, state).await?;

    Ok(())
}