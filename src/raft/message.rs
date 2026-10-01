use super::state::{NodeId, Term};

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
}