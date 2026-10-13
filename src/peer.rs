use std::collections::HashMap;
use std::os::fd::RawFd;

use crate::message::{self, AppendEntries, AppendEntriesResponse, RequestVote, VoteResponse};
use crate::raft_net::CoreRaftNetwork;
use crate::state::{RaftState, Role};
use crate::types::PendingWrite;


pub fn tick_raft(raft_state: &mut RaftState, net: &mut CoreRaftNetwork,
    core_id: usize, my_node_id: u64) {
    if raft_state.tick() {
        match raft_state.role {
            Role::Follower | Role::Candidate => {
                println!("[Core {}] Election timeout breached! Starting election for Term {}", core_id, raft_state.current_term + 1);
                
                // Trigger your custom election state transitions
                let vote_req = raft_state.start_election();
                
                // Pack the structural layout into a raw wire byte buffer
                let mut payload_buf = [0u8; 32];
                let size = vote_req.to_bytes(&mut payload_buf);
                // Broadcast the byte stream across our network mesh array
                for peer_id in 0..4 {
                    if peer_id == my_node_id { continue; }
                    net.send_packet(peer_id, message::TYPE_REQ_VOTE, &payload_buf[1..size]);
                }
            }
            Role::Leader => {
                // We are the cluster leader! A tripped timer means it is time for a heartbeat
                let heartbeat = raft_state.create_heartbeat();
                
                let mut payload_buf = [0u8; 32];
                let size = heartbeat.to_bytes(&mut payload_buf);
                for peer_id in 0..4 {
                    if peer_id == my_node_id { continue; }
                    net.send_packet(peer_id, message::TYPE_APP_ENTRIES, &payload_buf[1..size]);
                }
            }
        }
    }
}

pub fn handle_peer_message(
    msg_type: u8, payload: &[u8],
    raft_state: &mut RaftState,
    net: &mut CoreRaftNetwork,
    kv: &mut HashMap<String, String>,
    pending: &mut HashMap<u64, PendingWrite>,
    to_reply: &mut Vec<RawFd>,
    core_id: usize, 
    my_node_id: u64,
)
{

    let mut wire_buffer = vec![msg_type];
    wire_buffer.extend_from_slice(payload);

    match msg_type {
        // CASE 1: AN OUTSIDE NODE IS REQUESTING OUR VOTE
        message::TYPE_REQ_VOTE => {
            let req = RequestVote::from_bytes(&wire_buffer);
            let vote_response = raft_state.handle_request_vote(&req);
            
            let mut resp_buf = [0u8; 32];
            let size = vote_response.to_bytes(&mut resp_buf);
            net.send_packet(req.candidate_id, message::TYPE_VOTE_RESP, &resp_buf[1..size]);
        }
                        
        // CASE 2: A NODE REPLIED TO OUR ELECTION CALL
        message::TYPE_VOTE_RESP => {
            let resp = VoteResponse::from_bytes(&wire_buffer);
            
            // Pass the response to your state machine. We look for a majority of 4 nodes!
            let won_election = raft_state.handle_vote_response(&resp, 4);
            
            if won_election && raft_state.votes_received.len() >= 3  {
                println!("[Core {}] Majority Quorum secured! I am now the active LEADER.", core_id);
                
                // Send an immediate authority asserting heartbeat down the network grid
                let heartbeat = raft_state.create_heartbeat();
                let mut hb_buf = [0u8; 32];
                let size = heartbeat.to_bytes(&mut hb_buf);
                
                for peer_id in 0..4 {
                    if peer_id == my_node_id { continue; }
                    net.send_packet(peer_id, message::TYPE_APP_ENTRIES, &hb_buf[1..size]);
                }
            }
        }
                        
        message::TYPE_APP_ENTRIES => {
            let hb_req = AppendEntries::from_bytes(&wire_buffer);
            let ack_response = raft_state.handle_append_entries(&hb_req);
            
            let mut ack_buf = [0u8; 32];
            let size = ack_response.to_bytes(&mut ack_buf);
            net.send_packet(hb_req.leader_id, message::TYPE_APP_RESP, &ack_buf[1..size]);
        }
        
        // CASE 4: FOLLOWER ACKNOWLEDGED OUR HEARTBEAT PULSE
        message::TYPE_APP_RESP => {
            // Convert wire buffer bytes back into our response struct frame
            let resp = AppendEntriesResponse::from_bytes(&wire_buffer);
            
            if resp.success {
                // Quiet verification trace showing this follower node is synced up
                println!("[Core {}] Follower Node {} confirmed active heartbeat sync alignment.", core_id, resp.follower_id);
            } else {
                // Term mismatch or partition recovery indicator trace
                println!("[Core {}] Follower Node {} rejected heartbeat. Term out of sync.", core_id, resp.follower_id);
            }
        }
        message::TYPE_REPLICATE_DATA => {
            let id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        
            let mut data_buffer = vec![msg_type];
            data_buffer.extend_from_slice(&payload[8..]);      // skip the id
            let incoming_data = message::ReplicateData::from_bytes(&data_buffer);
        
            if incoming_data.is_delete {
                kv.remove(&incoming_data.key);
            } else {
                kv.insert(incoming_data.key.clone(), incoming_data.value.clone());
            }
        
            if let Some(leader) = raft_state.leader_id {
                let mut ack = [0u8; 16];
                ack[0..8].copy_from_slice(&id.to_be_bytes());
                ack[8..16].copy_from_slice(&my_node_id.to_be_bytes());
                net.send_packet(leader, message::TYPE_REPLICATE_ACK, &ack);
            }
        }
        message::TYPE_REPLICATE_ACK => {
            let id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
            let from = u64::from_be_bytes(payload[8..16].try_into().unwrap()) as usize;
        
            if from < 4 {
                if let Some(w) = pending.get_mut(&id) {
                    w.acked[from] = true;
                    if w.acked.iter().filter(|&&a| a).count() >= 3 {
                        let w = pending.remove(&id).unwrap();
                        if w.is_delete {
                            kv.remove(&w.key);
                        } else {
                            kv.insert(w.key, w.value);
                        }
                        to_reply.push(w.client_fd);
                    }
                }
            }
        }
        _ => {}
    
    }
}