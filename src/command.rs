use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::time::Instant;

use crate::message;
use crate::raft_net::CoreRaftNetwork;
use crate::state::{RaftState, Role};
use crate::types::PendingWrite;

pub fn execute_command(
    cmd: &str, key: &str, value: &str, client_fd: RawFd,
    raft_state: &RaftState,
    kv: &HashMap<String, String>,
    net: &mut CoreRaftNetwork,
    pending: &mut HashMap<u64, PendingWrite>,
    timeout_queue: &mut VecDeque<u64>,
    next_id: &mut u64,
    my_node_id: u64,
) -> String {
    match cmd {
        "SET" | "DELETE" => {
            if raft_state.role == Role::Leader {
                let is_delete = cmd == "DELETE";
                let sync_msg = message::ReplicateData {
                    is_delete, key: key.to_string(), value: value.to_string(),
                };
                let mut wire_buf = [0u8; 128];
                let payload_size = sync_msg.to_bytes(&mut wire_buf);

                let mut acked = [false; 4];
                acked[my_node_id as usize] = true;     // leader counts itself

                let id = *next_id;                     // `*` = the number next_id points to
                *next_id += 1;

                pending.insert(id, PendingWrite {
                    key: key.to_string(), value: value.to_string(),
                    is_delete, client_fd, acked, started_at: Instant::now(),
                });
                timeout_queue.push_back(id);

                let mut payload = id.to_be_bytes().to_vec();
                payload.extend_from_slice(&wire_buf[1..payload_size]);
                for peer_node_id in 0..4 {
                    if peer_node_id == my_node_id { continue; }
                    net.send_packet(peer_node_id, message::TYPE_REPLICATE_DATA, &payload);
                }
                String::new()                          // empty = answer later
            } else {
                match raft_state.leader_id {
                    Some(leader_id) => format!("-ERR MOVED TO NODE {}\r\n", leader_id),
                    None => "-ERR LEADER_UNAVAILABLE_TRY_AGAIN\r\n".to_string(),
                }
            }
        }
        "GET" => match kv.get(key) {
            Some(val) => format!("+{}\r\n", val),
            None => "-ERR KEY_NOT_FOUND\r\n".to_string(),
        },
        _ => "-ERR UNKNOWN_COMMAND\r\n".to_string(),
    }
}