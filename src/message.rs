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

pub struct ReplicateData{
    pub is_delete:bool,
    pub key: String,
    pub value: String,
}

// Global Unique Packet Identifiers
pub const TYPE_REQ_VOTE: u8 = 1;
pub const TYPE_VOTE_RESP: u8 = 2;
pub const TYPE_APP_ENTRIES: u8 = 3;
pub const TYPE_APP_RESP: u8 = 4;
pub const TYPE_REPLICATE_DATA: u8 = 5;
pub const TYPE_REPLICATE_ACK: u8 = 6;

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
        // ✨ FIX: Read voter_id from indices 9 to 17, then read the granted flag at index 17
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
        // ✨ FIX: Read follower_id from indices 9 to 17, then read the success flag at index 17
        let follower_id = u64::from_be_bytes(buf[9..17].try_into().unwrap());
        let success = buf[17] == 1;
        AppendEntriesResponse { term, follower_id, success }
    }
}

impl ReplicateData {
    pub fn to_bytes(&self, buf: &mut [u8]) -> usize {
        buf[0] = TYPE_REPLICATE_DATA;
        buf[1] = if self.is_delete { 1 } else { 0 };
        
        let key_bytes = self.key.as_bytes();
        let val_bytes = self.value.as_bytes();
        
        buf[2] = key_bytes.len() as u8;
        buf[3] = val_bytes.len() as u8;
        
        buf[4..4 + key_bytes.len()].copy_from_slice(key_bytes);
        let val_start = 4 + key_bytes.len();
        buf[val_start..val_start + val_bytes.len()].copy_from_slice(val_bytes);
        
        4 + key_bytes.len() + val_bytes.len()
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        let is_delete = buf[1] == 1;
        let key_len = buf[2] as usize;
        let val_len = buf[3] as usize;
        
        // Extract raw string slices from their precise bounds
        let raw_key = String::from_utf8(buf[4..4 + key_len].to_vec())
            .expect("Failed to parse replication key");
            
        let val_start = 4 + key_len;
        let raw_value = String::from_utf8(buf[val_start..val_start + val_len].to_vec())
            .expect("Failed to parse replication value");
            
        // ✨ THE SOLID FIX: Strip away any invisible trailing network null padding characters
        // to ensure database keys and values match with 100% precision on all nodes!
        let clean_key = raw_key.trim_matches('\0').to_string();
        let clean_value = raw_value.trim_matches('\0').to_string();
        
        ReplicateData { is_delete, key: clean_key, value: clean_value }
    }



}