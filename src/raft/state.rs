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