use std::collections::HashSet;

pub type NodeId = u64;
pub type Term = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct RaftState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub votes_received: HashSet<NodeId>,
    pub election_elapsed: u64,
    pub election_timeout: u64,
    pub heartbeat_elapsed: u64,
    pub heartbeat_interval: u64,
}

impl RaftState {
    pub fn new(node_id: NodeId, core_id: usize) -> Self {
        // Construct and return the RaftState instance
        Self {
            node_id, // Short for node_id: node_id
            current_term: 0,
            voted_for: None,
            role: Role::Follower,
            leader_id: None,
            votes_received: HashSet::new(),
            election_elapsed: 0,
            // Every core shard gets a unique timeout window to ensure one node wins clearly
            election_timeout: 15 + (core_id as u64 * 5), 
            heartbeat_elapsed: 0,
            heartbeat_interval: 3, // Leaders pulse a heartbeat every 3 ticks (300ms)
        }
    }
}