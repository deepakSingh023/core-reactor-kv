// src/raft_net.rs

use std::net::{TcpListener,TcpStream};
use std::collections::HashMap;
use std::os::fd::{RawFd,AsRawFd};
use socket2;
use super::message::{AppendEntries, AppendEntriesResponse, RequestVote, VoteResponse};
use super::state::{RaftState, Role};
use std::io::{Read, Write};

// Bring in our packet type identifiers
const TYPE_REQ_VOTE: u8 = 1;
const TYPE_VOTE_RESP: u8 = 2;
const TYPE_APP_ENTRIES: u8 = 3;
const TYPE_APP_RESP: u8 = 4;

pub struct CoreRaftNetwork {
    pub peer_connections: HashMap<u64,TcpStream>,
    pub listener_fd : RawFd,
    pub _listener : TcpListener,
}

impl CoreRaftNetwork {

    pub fn calculate_peer_port(node_id : u64 , core_id: usize)-> u16 {
        9000 + (node_id as u16 * 10) + (core_id as u16) 
    }

    pub fn new(my_node_id : u64 , core_id:usize , total_nodes:u64) -> Self {
        let mut peer_connections = HashMap::new();
        let my_raft_port = Self::calculate_peer_port(my_node_id,core_id);

        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP)
        ).unwrap();

        let address: std::net::SocketAddr = format!("127.0.0.1:{}", my_raft_port).parse().unwrap();
        socket.bind(&socket2::SockAddr::from(address)).unwrap();
        socket.listen(128).unwrap();
        socket.set_nonblocking(true).unwrap(); // Must be non-blocking for our epoll engine

        let listener: TcpListener = socket.into();
        let listener_fd = listener.as_raw_fd();

        println!("[Core {}] Raft Consensus Port bound to: {}", core_id, address);

        for target_node_id in 0..total_nodes {
            if target_node_id == my_node_id { continue; } // Skip ourselves

            let peer_port = Self::calculate_peer_port(target_node_id, core_id);
            let peer_addr = format!("127.0.0.1:{}", peer_port);
            
            println!("[Core {}] Pre-calculated target peer address for Node {}: {}", core_id, target_node_id, peer_addr);

            if let Ok(stream) = TcpStream::connect(&peer_addr) {
                let _ = stream.set_nonblocking(true);
                peer_connections.insert(target_node_id, stream);
                println!("[Core {}] Connected successfully to Node {}!", core_id, target_node_id);
            } else {
                println!("[Core {}] Failed to connect to Node {} (It might not be online yet).", core_id, target_node_id);
            }
        }

        CoreRaftNetwork { peer_connections , listener_fd , _listener: listener }
    }

    pub fn send_packet(&mut self, target_node_id: u64, msg_type: u8, payload: &[u8]) {
        if let Some(stream) = self.peer_connections.get_mut(&target_node_id) {
            let mut wire_buffer = [0u8; 32];
            wire_buffer[0] = msg_type; // Inject Type ID byte at position 0
            
            let data_length = payload.len();
            wire_buffer[1..1 + data_length].copy_from_slice(payload);
            
            let total_bytes = 1 + data_length;
            let _ = stream.write_all(&wire_buffer[0..total_bytes]);
        }
    }

    pub fn receive_packet(&mut self, stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
        let mut header_buf = [0u8; 1];
        
        match stream.read(&mut header_buf) {
            Ok(1) => {
                let msg_type = header_buf[0];
                
                let expected_payload_size = match msg_type {
                    TYPE_REQ_VOTE => 16,   // term (8b) + candidate_id (8b) = 16
                    
                    // ✨ FIX 1: Change from 9 to 17! term (8b) + voter_id (8b) + granted (1b) = 17
                    TYPE_VOTE_RESP => 17,  
                    
                    TYPE_APP_ENTRIES => 16, // term (8b) + leader_id (8b) = 16
                    
                    // ✨ FIX 2: Change from 9 to 17! term (8b) + follower_id (8b) + success (1b) = 17
                    TYPE_APP_RESP => 17,   
                    
                    5 => {
                        // 1. Read the next 3 structural bytes directly (is_delete, key_len, val_len)
                        let mut data_header = [0u8; 3];
                        match stream.read_exact(&mut data_header) {
                            Ok(()) => {
                                let key_size = data_header[1] as usize;
                                let val_size = data_header[2] as usize;
                                let remaining_payload_size = key_size + val_size;
                    
                                // 2. Read the remaining string payloads
                                let mut body_buf = vec![0u8; remaining_payload_size];
                                match stream.read_exact(&mut body_buf) {
                                    Ok(()) => {
                                        // 3. Assemble a uniform buffer that matches what `from_bytes` expects:
                                        // [msg_type (omitted since from_bytes starts at index 1), is_delete, key_len, val_len, payload...]
                                        let mut full_payload = Vec::with_capacity(3 + remaining_payload_size);
                                        full_payload.extend_from_slice(&data_header);
                                        full_payload.extend_from_slice(&body_buf);
                                        
                                        // Return it. Note: receive_packet returns Option<(u8, Vec<u8>)>
                                        return Some((msg_type, full_payload));
                                    }
                                    Err(_) => return None,
                                }
                            }
                            Err(_) => return None,
                        }
                    }
                    6 => 9, 
                    _ => return None,
                };


                let mut payload_buf = vec![0u8; expected_payload_size];
                match stream.read_exact(&mut payload_buf) {
                    Ok(()) => Some((msg_type, payload_buf)),
                    Err(_) => None,
                }
            }
            _ => None,
        }
    }

    pub fn retry_missing_connections(&mut self, my_node_id: u64, core_id: usize, total_nodes: u64) {
        for target_node_id in 0..total_nodes {
            if target_node_id == my_node_id { continue; } // Skip ourselves

            // If we are ALREADY connected to this node, do nothing!
            if self.peer_connections.contains_key(&target_node_id) { continue; }

            let peer_port = Self::calculate_peer_port(target_node_id, core_id);
            let peer_addr = format!("127.0.0.1:{}", peer_port);

            // Attempt to connect over standard TCP
            if let Ok(stream) = TcpStream::connect(&peer_addr) {
                let _ = stream.set_nonblocking(true);
                
                // Save it into our struct backpack!
                self.peer_connections.insert(target_node_id, stream);
                println!("[Core {}] Connected dynamically to Node {} on port {}!", core_id, target_node_id, peer_port);
            }
        }
    }
}
