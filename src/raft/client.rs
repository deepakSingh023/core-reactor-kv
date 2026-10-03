use tonic::transport::Channel;

use crate::raft_proto::{
    raft_service_client::RaftServiceClient,
    RequestVoteRequest,
    RequestVoteResponse,
};

use super::state::{NodeId, Term};

pub async fn request_vote(
    address: String,
    term: Term,
    candidate_id: NodeId,
) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = format!("http://{}", address);

    let mut client = RaftServiceClient::connect(endpoint).await?;

    let request = RequestVoteRequest {
        term,
        candidate_id,
    };

    let response = client.request_vote(request).await?;

    Ok(response.into_inner())
}