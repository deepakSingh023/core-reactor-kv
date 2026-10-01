use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::raft_proto::{
    raft_service_server::RaftService,
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
};

use crate::raft::state::{RaftState, Role};

pub struct RaftServiceImpl {
    pub state: Arc<Mutex<RaftState>>,
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let request = request.into_inner();

        let mut state = self.state.lock().await;

        println!(
            "Node {} received RequestVote from Node {} | term={}",
            state.node_id,
            request.candidate_id,
            request.term
        );

        let vote_granted =
            if request.term < state.current_term {
                false
            } else {
                if request.term > state.current_term {
                    state.current_term = request.term;
                    state.role = Role::Follower;
                    state.voted_for = None;
                    state.leader_id = None;
                }

                match state.voted_for {
                    None => {
                        state.voted_for = Some(request.candidate_id);
                        state.election_elapsed = 0;
                        true
                    }

                    Some(voted_for) => {
                        voted_for == request.candidate_id
                    }
                }
            };

        Ok(Response::new(RequestVoteResponse {
            term: state.current_term,
            voter_id: state.node_id,
            vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let request = request.into_inner();

        let mut state = self.state.lock().await;

        println!(
            "Node {} received AppendEntries from Node {} | term={}",
            state.node_id,
            request.leader_id,
            request.term
        );

        let success = if request.term < state.current_term {
            false
        } else {
            if request.term > state.current_term {
                state.current_term = request.term;
                state.voted_for = None;
            }

            state.role = Role::Follower;
            state.leader_id = Some(request.leader_id);
            state.election_elapsed = 0;

            true
        };

        Ok(Response::new(AppendEntriesResponse {
            term: state.current_term,
            follower_id: state.node_id,
            success,
        }))
    }
}