use std::collections::HashSet;

use crate::raft::state::{NodeId, RaftState, Role};

pub struct Node {
    pub id: NodeId,
    pub raft: RaftState,
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            raft: RaftState {
                node_id: id,

                current_term: 0,

                voted_for: None,

                role: Role::Follower,

                leader_id: None,

                votes_received: HashSet::new(),

                election_elapsed: 0,

                election_timeout: 10,

                heartbeat_elapsed: 0,

                heartbeat_interval: 3,
            },
        }
    }
}