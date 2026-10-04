// src/message.rs

pub type NodeId = u64;
pub type Term = u64;

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

// Global Unique Packet Identifiers
pub const TYPE_REQ_VOTE: u8 = 1;
pub const TYPE_VOTE_RESP: u8 = 2;
pub const TYPE_APP_ENTRIES: u8 = 3;
pub const TYPE_APP_RESP: u8 = 4;

impl RequestVote {
    /// Turns a RequestVote struct into 17 raw binary network bytes.
    pub fn to_bytes(&self, buf: &mut [u8]) -> usize {
        buf[0] = TYPE_REQ_VOTE;
        buf[1..9].copy_from_slice(&self.term.to_be_bytes());
        buf[9..17].copy_from_slice(&self.candidate_id.to_be_bytes());
        17
    }

    /// Parses 17 raw network bytes straight back into a clean RequestVote struct.
    pub fn from_bytes(buf: &[u8]) -> Self {
        let term = u64::from_be_bytes(buf[1..9].try_into().unwrap());
        let candidate_id = u64::from_be_bytes(buf[9..17].try_into().unwrap());
        RequestVote { term, candidate_id }
    }
}

impl VoteResponse {
    /// Turns a VoteResponse struct into 10 raw binary network bytes.
    pub fn to_bytes(&self, buf: &mut [u8]) -> usize {
        buf[0] = TYPE_VOTE_RESP;
        buf[1..9].copy_from_slice(&self.term.to_be_bytes());
        buf[9..17].copy_from_slice(&self.voter_id.to_be_bytes());
        buf[17] = if self.vote_granted { 1 } else { 0 };
        18
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        let term = u64::from_be_bytes(buf[1..9].try_into().unwrap());
        let voter_id = u64::from_be_bytes(buf[9..17].try_into().unwrap());
        let vote_granted = buf[17] == 1;
        VoteResponse { term, voter_id, vote_granted }
    }
}

impl AppendEntries {
    /// Turns a Heartbeat struct into 17 raw binary network bytes.
    pub fn to_bytes(&self, buf: &mut [u8]) -> usize {
        buf[0] = TYPE_APP_ENTRIES;
        buf[1..9].copy_from_slice(&self.term.to_be_bytes());
        buf[9..17].copy_from_slice(&self.leader_id.to_be_bytes());
        17
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        let term = u64::from_be_bytes(buf[1..9].try_into().unwrap());
        let leader_id = u64::from_be_bytes(buf[9..17].try_into().unwrap());
        AppendEntries { term, leader_id }
    }
}

impl AppendEntriesResponse {
    /// Turns a Heartbeat Response into 18 raw binary network bytes.
    pub fn to_bytes(&self, buf: &mut [u8]) -> usize {
        buf[0] = TYPE_APP_RESP;
        buf[1..9].copy_from_slice(&self.term.to_be_bytes());
        buf[9..17].copy_from_slice(&self.follower_id.to_be_bytes());
        buf[17] = if self.success { 1 } else { 0 };
        18
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        let term = u64::from_be_bytes(buf[1..9].try_into().unwrap());
        let follower_id = u64::from_be_bytes(buf[9..17].try_into().unwrap());
        let success = buf[17] == 1;
        AppendEntriesResponse { term, follower_id, success }
    }
}
