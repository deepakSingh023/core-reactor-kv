use super::message::{
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    VoteResponse,
};

use super::state::{RaftState, Role};

impl RaftState {

    // ==========================================
    // ELECTION
    // ==========================================

    pub fn start_election(&mut self) -> RequestVote {
        self.current_term += 1;

        self.role = Role::Candidate;

        self.voted_for = Some(self.node_id);

        self.leader_id = None;

        self.votes_received.clear();
        self.votes_received.insert(self.node_id);

        self.election_elapsed = 0;

        RequestVote {
            term: self.current_term,
            candidate_id: self.node_id,
        }
    }

    pub fn handle_request_vote(
        &mut self,
        request: &RequestVote,
    ) -> VoteResponse {

        if request.term < self.current_term {
            return VoteResponse {
                term: self.current_term,
                voter_id: self.node_id,
                vote_granted: false,
            };
        }

        if request.term > self.current_term {
            self.current_term = request.term;

            self.role = Role::Follower;

            self.voted_for = None;

            self.leader_id = None;
        }

        let can_vote = match self.voted_for {
            None => true,

            Some(voted_for) => {
                voted_for == request.candidate_id
            }
        };

        if can_vote {
            self.voted_for = Some(request.candidate_id);

            self.election_elapsed = 0;

            return VoteResponse {
                term: self.current_term,
                voter_id: self.node_id,
                vote_granted: true,
            };
        }

        VoteResponse {
            term: self.current_term,
            voter_id: self.node_id,
            vote_granted: false,
        }
    }

    pub fn handle_vote_response(
        &mut self,
        response: &VoteResponse,
        cluster_size: usize,
    ) -> bool {

        if self.role != Role::Candidate {
            return false;
        }

        if response.term > self.current_term {
            self.current_term = response.term;

            self.role = Role::Follower;

            self.voted_for = None;

            self.leader_id = None;

            self.votes_received.clear();

            return false;
        }

        if response.term < self.current_term {
            return false;
        }

        if !response.vote_granted {
            return false;
        }

        self.votes_received.insert(response.voter_id);

        let majority = cluster_size / 2 + 1;

        if self.votes_received.len() >= majority {
            self.become_leader();

            return true;
        }

        false
    }

    pub fn become_leader(&mut self) {
        self.role = Role::Leader;

        self.leader_id = Some(self.node_id);

        self.heartbeat_elapsed = 0;
    }


    // ==========================================
    // HEARTBEAT
    // ==========================================

    pub fn create_heartbeat(&mut self) -> AppendEntries {
        AppendEntries {
            term: self.current_term,

            leader_id: self.node_id,
        }
    }

    pub fn handle_append_entries(
        &mut self,
        request: &AppendEntries,
    ) -> AppendEntriesResponse {

        if request.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,

                follower_id: self.node_id,

                success: false,
            };
        }

        if request.term > self.current_term {
            self.current_term = request.term;

            self.voted_for = None;
        }

        self.role = Role::Follower;

        self.leader_id = Some(request.leader_id);

        self.election_elapsed = 0;

        AppendEntriesResponse {
            term: self.current_term,

            follower_id: self.node_id,

            success: true,
        }
    }


    // ==========================================
    // TIMER
    // ==========================================

    pub fn tick(&mut self) -> bool {

        match self.role {

            Role::Leader => {

                self.heartbeat_elapsed += 1;

                if self.heartbeat_elapsed >= self.heartbeat_interval {

                    self.heartbeat_elapsed = 0;

                    return true;
                }
            }

            Role::Follower | Role::Candidate => {

                self.election_elapsed += 1;

                if self.election_elapsed >= self.election_timeout {

                    return true;
                }
            }
        }

        false
    }
}