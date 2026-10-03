use std::net::SocketAddr;

use super::state::NodeId;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub address: SocketAddr,
}

pub fn cluster_config() -> Vec<NodeConfig> {
    vec![
        NodeConfig {
            id: 0,
            address: "127.0.0.1:9000".parse().unwrap(),
        },
        NodeConfig {
            id: 1,
            address: "127.0.0.1:9001".parse().unwrap(),
        },
        NodeConfig {
            id: 2,
            address: "127.0.0.1:9002".parse().unwrap(),
        },
        NodeConfig {
            id: 3,
            address: "127.0.0.1:9003".parse().unwrap(),
        },
    ]
}