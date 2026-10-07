use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use nix::sys::epoll::{
    Epoll,
    EpollCreateFlags,
    EpollEvent,
    EpollFlags,
};

mod election;
use sharding::calculate_shard;
use inter_core::InterCoreMessage;
mod state;      // Points directly to your src/state.rs file
      // Points directly to your src/raft.rs file
mod message;    // Points directly to your src/message.rs file
mod sharding;
mod inter_core;
use crossbeam_channel::{unbounded, Sender, Receiver};

use state::{RaftState, Role};
use message::{RequestVote, VoteResponse, AppendEntries, AppendEntriesResponse};
mod raft_net;
// In main.rs
#[derive(Debug)]
enum ConnType {
    Listener,
    Client, 
    Doorbell,
    RaftServer, // Listens for new incoming peer network dials
    RaftPeer,   // An active data channel link with a peer node core
}


#[derive(Debug)]
struct Conn {
    fd: RawFd,
    stream: Option<TcpStream>, 
    input_buffer: Vec<u8>,
    output_buffer: Vec<u8>,
    conn_type: ConnType, 
}
struct CoreLocalReactor {
    connections: HashMap<i32, Conn>,
}

fn main() -> Result<(), std::io::Error> {

    // Collect command line arguments: cargo run -- <node_id>
    let args: Vec<String> = std::env::args().collect();
    let my_node_id: u64 = if args.len() > 1 {
        args[1].parse().unwrap_or(0)
    } else {
        0 // Fallback default to Node 0 if no argument passed
    };

    let total_cores = 4;


    // Number of cores/threads used by the reactor.
    let total_cores = 4;

    
    let mut senders_mesh: Vec<Vec<crossbeam_channel::Sender<InterCoreMessage>>> = vec![vec![]; total_cores];
    let mut receivers_mesh: Vec<Option<crossbeam_channel::Receiver<InterCoreMessage>>> = vec![None; total_cores];

    let mut eventfds_pool: Vec<RawFd> = vec![];

        // 1. First loop: Create an eventfd doorbell and an inbound mailbox queue for every single core
    for core_idx in 0..total_cores {
        // EFD_NONBLOCK ensures our eventfd read/write loops never lock up the threads
        let efd = unsafe { nix::libc::eventfd(0, nix::libc::EFD_NONBLOCK) };
        if efd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        eventfds_pool.push(efd);

        let (s, r) = crossbeam_channel::unbounded::<InterCoreMessage>();
        receivers_mesh[core_idx] = Some(r);
        
        // Every core thread maps an outbound pipe connecting straight into this target mailbox queue
        for src_idx in 0..total_cores {
            senders_mesh[src_idx].push(s.clone());
        }
    }

    let shared_eventfds = std::sync::Arc::new(eventfds_pool);
    let mut thread_handles = vec![];

    // Store thread handles so the main thread can join them.    

    for core_id in 0..total_cores{

        // --- EXTRACT CORE TOOLS BEFORE SPAWNING THE THREAD ---
        // We pull this specific core's values out of the shared matrix so the thread can capture them.
        let my_eventfd = shared_eventfds[core_id];
        let peer_eventfds = shared_eventfds.to_vec();
        let my_receiver = receivers_mesh[core_id].take().unwrap();
        let my_senders = senders_mesh[core_id].clone();

        // Assemble them into a single, dedicated CoreChannels package
        let core_channels = inter_core::CoreChannels {
            senders: my_senders,
            receiver: my_receiver,
            my_eventfd,
            peer_eventfds,
        };

        // Spawn one reactor thread for this core.
        let handle = std::thread::spawn(move || {
 

            // Get available CPU cores to pin reactor threads.
            let core_ids = core_affinity::get_core_ids().unwrap();


            if let Some(target_core) = core_ids.get(core_id) {
                core_affinity::set_for_current(*target_core);
            }

            // Store connections by file descriptor for quick lookup.
            let mut connections = HashMap::new();

            let mut local_kv_store: HashMap<String,String> = HashMap::new();

            let mut raft_state = RaftState {
                node_id: my_node_id,
                current_term: 0,
                voted_for: None,
                role: Role::Follower,
                leader_id: None,
                votes_received: std::collections::HashSet::new(),
                election_elapsed: 0,
                // Every core shard gets a unique timeout window to ensure one node wins clearly
                election_timeout: 15 + (core_id as u64 * 5), 
                heartbeat_elapsed: 0,
                heartbeat_interval: 3, // Leaders pulse a heartbeat every 3 ticks (300ms)
            };



            // A single call boots up the whole sharded Raft network stack for this thread!
            let mut raft_network = raft_net::CoreRaftNetwork::new(my_node_id, core_id, 4);


            //Instead of blindly opening a listener it goes into linux kernel and get a raw unconfigured network file descriptor
            let socket = socket2::Socket::new(socket2::Domain::IPV4,socket2::Type::STREAM, Some(socket2::Protocol::TCP)).unwrap();
            
            // Allow multiple reactor threads to bind the same address.
            socket.set_reuse_port(true).unwrap();

       
            let current_client_port = 8080 + my_node_id; 
            let address : std::net::SocketAddr = format!("127.0.0.1:{}", current_client_port).parse().unwrap();
            
            socket.bind(&socket2::SockAddr::from(address)).unwrap();

            socket.listen(128).unwrap();

            //It makes the listener non blocking
            socket.set_nonblocking(true).unwrap();

            // Convert the configured socket into a TcpListener.            
            let listener: std::net::TcpListener = socket.into();
        
            //Creating new epoll
            let epoll: Epoll = match Epoll::new(EpollCreateFlags::empty()) {
                Ok(epoll) => epoll,
                Err(e) => {
                    eprintln!("[Core {}] Failed to create epoll: {}", core_id, e);
                    return;
                }
            };        
            let event = EpollEvent::new(
                EpollFlags::EPOLLIN,
                listener.as_raw_fd() as u64,
            );
             
            //Adding the fd of the listener to the epoll
            if let Err(e) = epoll.add(&listener, event) {
                eprintln!("[Core {}] Failed to add listener to epoll: {}", core_id, e);
                return;
            }
        
            let conn = Conn{
                fd:listener.as_raw_fd(),
                stream:None,
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                conn_type:ConnType::Listener,
            };
        
            connections.insert(listener.as_raw_fd() as i32,conn);

            // --- REGISTER YOUR DOORBELL INTO THE EPOLL INSTANCE ---
            let doorbell_event = EpollEvent::new(EpollFlags::EPOLLIN, my_eventfd as u64);
            unsafe {
                // Borrow the raw file descriptor safely so epoll can trace it
                let borrowed_efd = std::os::fd::BorrowedFd::borrow_raw(my_eventfd);
                if let Err(e) = epoll.add(&borrowed_efd, doorbell_event) {
                    eprintln!("[Core {}] Failed to add eventfd to epoll: {}", core_id, e);
                    return;
                }
            }
            
            // Map the doorbell into our connections directory so epoll_wait can identify it
            let doorbell_conn = Conn {
                fd: my_eventfd,
                stream: None,
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                conn_type: ConnType::Doorbell,
            };
            connections.insert(my_eventfd, doorbell_conn);

            // --- REGISTER YOUR PRIVATE CONSENSUS SERVER PORT INTO THE SAME EPOLL ENGINE ---
            let raft_server_event = EpollEvent::new(EpollFlags::EPOLLIN, raft_network.listener_fd as u64);
            unsafe {
                let borrowed_raft_fd = std::os::fd::BorrowedFd::borrow_raw(raft_network.listener_fd);
                if let Err(e) = epoll.add(&borrowed_raft_fd, raft_server_event) {
                    eprintln!("[Core {}] Failed to add Raft server fd to epoll: {}", core_id, e);
                    return;
                }
            }
            
            // Map it into our connections map using our new ConnType tag variant
            let raft_server_conn = Conn {
                fd: raft_network.listener_fd,
                stream: None,
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                conn_type: ConnType::RaftServer, // <--- Tells epoll: "This is a peer node knocking"
            };
            connections.insert(raft_network.listener_fd, raft_server_conn);
            
            
        
            let mut events = [EpollEvent::empty(); 64];
        
            loop {
                
      
                let timeout = nix::sys::epoll::EpollTimeout::try_from(100).unwrap();

                let epoll_count = match epoll.wait(
                    &mut events,
                    timeout // ✨ Fixed cleanly!
                ) {
                    Ok(count) => count,
                    Err(e) => {
                        eprintln!("[Core {}] epoll_wait error: {}", core_id, e);
                        continue;
                    }
                };




                if epoll_count == 0 {
                    raft_network.retry_missing_connections(my_node_id, core_id, 4);
                    
                    // Call your authentic timer logic method!
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
                                    raft_network.send_packet(peer_id, message::TYPE_REQ_VOTE, &payload_buf[1..size]);
                                }
                            }
                            Role::Leader => {
                                // We are the cluster leader! A tripped timer means it is time for a heartbeat
                                let heartbeat = raft_state.create_heartbeat();
                                
                                let mut payload_buf = [0u8; 32];
                                let size = heartbeat.to_bytes(&mut payload_buf);
            
                                for peer_id in 0..4 {
                                    if peer_id == my_node_id { continue; }
                                    raft_network.send_packet(peer_id, message::TYPE_APP_ENTRIES, &payload_buf[1..size]);
                                }
                            }
                        }
                    }
                }

        
                for i in 0..epoll_count {
        
                    let fd = events[i].data();
                    let fd2 = fd as i32;
                    let Some(con) = connections.get_mut(&fd2) else {continue;};
        
                    match con.conn_type{
                        ConnType::Listener=>{
        
                            // Keep it flat, but skip if there's nothing to read right now
                            let Ok((stream, addr)) = listener.accept() else { continue; };
    
                            if let Err(e) = stream.set_nonblocking(true) {
                                eprintln!(
                                    "[Core {}] Failed to make client nonblocking: {}",
                                    core_id,
                                    e
                                );
                                continue;
                            }
        
                            let client_fd = stream.as_raw_fd() as u64;
        
                             let event2 = EpollEvent::new(EpollFlags::EPOLLIN | EpollFlags::EPOLLET,client_fd);
                             
                             if let Err(e) = epoll.add(&stream, event2) {
                                 eprintln!(
                                     "[Core {}] Failed to add client fd {} to epoll: {}",
                                     core_id,
                                     client_fd,
                                     e
                                 );
                                 continue;
                             }
                            
                            let conn2 = Conn{
                                fd:client_fd as i32,
                                stream:Some(stream),
                                input_buffer: Vec::new(),
                                output_buffer:Vec::new(),
                                conn_type: ConnType::Client
                            };
        
                            connections.insert(client_fd as i32, conn2);
        
                        }
                        ConnType::Client=>{
        
                            if events[i].events().contains(EpollFlags::EPOLLIN){
        
                                if let Some(ref mut stream) = con.stream{
                                    let mut disconnected = false;
        
                                    loop {
        
                                        let mut scratch_buf = [0u8; 4096];
        
                                        match stream.read(&mut scratch_buf){
        
                                            Ok(0) =>{
                                                println!("no data here to read");
                                                disconnected = true;
                                                break;
                                            }
                                            Ok(bytes_read) =>{
                                                //appends to the input buffer by taking bytes from the buffer , also only count bytes present with help of 0..bytes_read
                                                con.input_buffer.extend_from_slice(&scratch_buf[0..bytes_read]);
                                                println!("Appended {} bytes to input_buffer.", bytes_read);
                                                if bytes_read < 4096 {
                                                    break;
                                                }
                                            }
        
                                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                                break; // Drained the kernel buffer
                                            }
                                            Err(e) => {
                                                println!("Read error on fd {}: {}", con.fd, e);
                                                disconnected = true;
                                                break;
                                            }
        
                                        }
        
                                    }
        
                                    if !disconnected && !con.input_buffer.is_empty() {
                                        if con.input_buffer.contains(&b'\n') {
                                            if let Ok(payload_str) = std::str::from_utf8(&con.input_buffer) {
                                                let clean_command = payload_str.trim();
                                                
                                                // --- 1. ROUTING DECISION: Extract the key to check ownership ---
                                                // If the command is "SET score 100", the key is "score"
                                                let mut parts = clean_command.split_whitespace();
                                                let cmd_type = parts.next().unwrap_or("");
                                                let key = parts.next().unwrap_or("");
                                                
                                                let assigned_core = calculate_shard(key);
                                                
                                                if assigned_core != core_id {
                                                    // IT DOES NOT BELONG TO US: Forward the whole Conn struct away immediately!
                                                    println!("[Core {}] Mismatch! Forwarding connection to Core {}", core_id, assigned_core);
                                                    if let Some(ref stream) = con.stream { let _ = epoll.delete(stream); }
                                                    if let Some(removed_conn) = connections.remove(&fd2) {
                                                        let parcel = InterCoreMessage { client_fd: fd2, conn: removed_conn };
                                                        let _ = core_channels.senders[assigned_core].try_send(parcel);
                                                        let _ = unsafe { nix::libc::write(core_channels.peer_eventfds[assigned_core], 1u64.to_ne_bytes().as_ptr() as *const _, 8) };
                                                    }
                                                    continue; // Connection successfully pushed out. Stop processing locally!
                                                }
                                                

                                                let response_string = match cmd_type {
                                                    "SET" | "DELETE" => {
                                                        if raft_state.role == Role::Leader {
                                                            let is_delete = cmd_type == "DELETE";
                                                            let value = parts.next().unwrap_or("");

                                                            // 1. Package your structural replication data payload
                                                            let sync_msg = message::ReplicateData {
                                                                is_delete,
                                                                key: key.to_string(),
                                                                value: value.to_string(),
                                                            };

                                                            let mut wire_buf = [0u8; 128];
                                                            let payload_size = sync_msg.to_bytes(&mut wire_buf);

                                                            // 2. Broadcast vertically to the exact same core index across all other nodes
                                                            for peer_node_id in 0..4 {
                                                                if peer_node_id == my_node_id { continue; }
                                                                // Skip byte index 0 (Type Header) as send_packet appends its own
                                                                raft_network.send_packet(peer_node_id, message::TYPE_REPLICATE_DATA, &wire_buf[1..payload_size]);
                                                            }

                                                            // 3. Complete mutation locally on this initial machine node core
                                                            if is_delete {
                                                                if local_kv_store.remove(key).is_some() { "+OK\r\n".to_string() } else { "-ERR KEY_NOT_FOUND\r\n".to_string() }
                                                            } else {
                                                                local_kv_store.insert(key.to_string(), value.to_string());
                                                                "+OK\r\n".to_string()
                                                            }
                                                        } else {
                                                            // Follower rejection track
                                                            match raft_state.leader_id {
                                                                Some(leader_id) => format!("-ERR MOVED TO NODE {}\r\n", leader_id),
                                                                None => "-ERR LEADER_UNAVAILABLE_TRY_AGAIN\r\n".to_string(),
                                                            }
                                                        }
                                                    }
                                                    "GET" => {
                                                        // Blazing fast local read since data is fully replicated across all matching levels!
                                                        match local_kv_store.get(key) {
                                                            Some(val) => format!("+{}\r\n", val),
                                                            None => "-ERR KEY_NOT_FOUND\r\n".to_string(),
                                                        }
                                                    }
                                                    _ => "-ERR UNKNOWN_COMMAND\r\n".to_string(),
                                                };


                                                con.input_buffer.clear(); // We fully processed the input command!
                                                con.output_buffer.extend_from_slice(response_string.as_bytes()); // Stage response bytes
                                            }
                                        }
                                    }

                                    if !disconnected && !con.output_buffer.is_empty() {
                                        if let Some(ref mut stream) = con.stream {
                                            loop {
                                                // Try to write the whole remaining output buffer
                                                match stream.write(&con.output_buffer) {
                                                    Ok(0) => {
                                                        // Kernel write buffer is broken or socket dropped
                                                        disconnected = true;
                                                        break;
                                                    }
                                                    Ok(bytes_written) => {
                                                        println!("Sent {} bytes back to client.", bytes_written);
                                                        // Remove the sent bytes from the front of the queue
                                                        con.output_buffer.drain(0..bytes_written);
                                                        
                                                        // If everything is sent, we are done!
                                                        if con.output_buffer.is_empty() {
                                                            break;
                                                        }
                                                    }
                                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                                        // Kernel write buffer is completely full. 
                                                        // Stop writing, keep the remaining data in output_buffer, and yield back to epoll!
                                                        break;
                                                    }
                                                    Err(e) => {
                                                        println!("Write error on fd {}: {}", con.fd, e);
                                                        disconnected = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                
                                    if disconnected {
                                        connections.remove(&fd2); 
        
                                    }
        
                                }
        
                            }
        
                        }
                        ConnType::Doorbell => {
                            let mut ack_buf = [0u8; 8];
                            unsafe {
                                nix::libc::read(fd2, ack_buf.as_mut_ptr() as *mut std::ffi::c_void, 8);
                            }

                            while let Ok(parcel) = core_channels.receiver.try_recv() {
                                let adopted_fd = parcel.client_fd;
                                let mut adopted_conn = parcel.conn;
                                
                                adopted_conn.conn_type = ConnType::Client;
                        
                                if let Some(ref stream) = adopted_conn.stream {
                                    let adopted_event = EpollEvent::new(EpollFlags::EPOLLIN | EpollFlags::EPOLLET, adopted_fd as u64);
                                    epoll.add(stream, adopted_event).expect("Failed to register adopted client stream");
                                }

                                if !adopted_conn.input_buffer.is_empty() && adopted_conn.input_buffer.contains(&b'\n') {
                                    if let Ok(payload_str) = std::str::from_utf8(&adopted_conn.input_buffer) {
                                        let clean_command = payload_str.trim();
                                        let mut parts = clean_command.split_whitespace();
                                        let cmd_type = parts.next().unwrap_or("");
                                        let key = parts.next().unwrap_or("");

                                        let response_string = match cmd_type {
                                            "SET" | "DELETE" => {
                                                if raft_state.role == Role::Leader {
                                                    if cmd_type == "SET" {
                                                        let value = parts.next().unwrap_or("");
                                                        local_kv_store.insert(key.to_string(), value.to_string());
                                                        "+OK\r\n".to_string()
                                                    } else {
                                                        if local_kv_store.remove(key).is_some() { "+OK\r\n".to_string() } else { "-ERR KEY_NOT_FOUND\r\n".to_string() }
                                                    }
                                                } else {
                                                    match raft_state.leader_id {
                                                        Some(leader_node_idx) => format!("-ERR MOVED TO NODE {}\r\n", leader_node_idx),
                                                        None => "-ERR LEADER_UNAVAILABLE_TRY_AGAIN\r\n".to_string(),
                                                    }
                                                }
                                            }
                                            "GET" => {
                                                match local_kv_store.get(key) {
                                                    Some(val) => format!("+{}\r\n", val),
                                                    None => "-ERR KEY_NOT_FOUND\r\n".to_string(),
                                                }
                                            }
                                            _ => "-ERR UNKNOWN_COMMAND\r\n".to_string(),
                                        };

                                        // Clear the input space and stage the database response bytes
                                        adopted_conn.input_buffer.clear();
                                        adopted_conn.output_buffer.extend_from_slice(response_string.as_bytes());
                                    }
                                }

                                // Securely insert into our connections map registry
                                connections.insert(adopted_fd as i32, adopted_conn);
                                println!("[Core {}] Successfully adopted and flushed SPSC descriptor pipeline for fd: {}", core_id, adopted_fd);

                                // If an output response was generated during immediate adoption, write it out instantly
                                if let Some(con_ref) = connections.get_mut(&(adopted_fd as i32)) {
                                    if !con_ref.output_buffer.is_empty() {
                                        if let Some(ref mut stream) = con_ref.stream {
                                            let _ = stream.write_all(&con_ref.output_buffer);
                                            con_ref.output_buffer.clear();
                                        }
                                    }
                                }
                            }
                        }


                        ConnType::RaftServer => {
                            // FIX: Access the underlying Raft listener object inside your raft_network struct!
                            let Ok((peer_stream, _)) = raft_network._listener.accept() else { continue; };
                            
                            if let Err(e) = peer_stream.set_nonblocking(true) {
                                eprintln!("[Core {}] Failed to set peer stream nonblocking: {}", core_id, e);
                                continue;
                            }
                            
                            let peer_fd = peer_stream.as_raw_fd();

                            let peer_event = EpollEvent::new(EpollFlags::EPOLLIN | EpollFlags::EPOLLET, peer_fd as u64);
                            if let Err(e) = epoll.add(&peer_stream, peer_event) {
                                eprintln!("[Core {}] Failed to add peer stream to epoll: {}", core_id, e);
                                continue;
                            }

                            let peer_conn = Conn {
                                fd: peer_fd,
                                stream: Some(peer_stream),
                                input_buffer: Vec::new(),
                                output_buffer: Vec::new(),
                                conn_type: ConnType::RaftPeer,
                            };
                            connections.insert(peer_fd, peer_conn);
                            println!("[Core {}] Accepted incoming peer consensus line connection.", core_id);
                        }


                        ConnType::RaftPeer => {
                               if let Some(ref mut peer_stream) = con.stream {
                                   // Read raw wire byte arrays out of our non-blocking peer cable channel
                                   if let Some((msg_type, payload)) = raft_network.receive_packet(peer_stream) {
                                       
                                       // Reconstruct the 1 byte header prefix into your type allocations
                                       let mut wire_buffer = vec![msg_type];
                                       wire_buffer.extend_from_slice(&payload);
                           
                                       match msg_type {
                                           // CASE 1: AN OUTSIDE NODE IS REQUESTING OUR VOTE
                                           message::TYPE_REQ_VOTE => {
                                               let req = RequestVote::from_bytes(&wire_buffer);
                                               let vote_response = raft_state.handle_request_vote(&req);
                                               
                                               let mut resp_buf = [0u8; 32];
                                               let size = vote_response.to_bytes(&mut resp_buf);
                                               raft_network.send_packet(req.candidate_id, message::TYPE_VOTE_RESP, &resp_buf[1..size]);
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
                                                       raft_network.send_packet(peer_id, message::TYPE_APP_ENTRIES, &hb_buf[1..size]);
                                                   }
                                               }
                                           }
                           
                                            message::TYPE_APP_ENTRIES => {
                                                let hb_req = AppendEntries::from_bytes(&wire_buffer);
                                                let ack_response = raft_state.handle_append_entries(&hb_req);
                                                
                                                let mut ack_buf = [0u8; 32];
                                                let size = ack_response.to_bytes(&mut ack_buf);
                                                raft_network.send_packet(hb_req.leader_id, message::TYPE_APP_RESP, &ack_buf[1..size]);
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
                                                // Reconstruct the wire buffer: position 0 is type header, followed by the payload
                                                let mut wire_buffer = vec![msg_type];
                                                wire_buffer.extend_from_slice(&payload);
                                            
                                                // Now from_bytes will index perfectly: buf[1]=is_delete, buf[2]=key_len, etc.
                                                let incoming_data = message::ReplicateData::from_bytes(&wire_buffer);
                                                
                                                if incoming_data.is_delete {
                                                    local_kv_store.remove(&incoming_data.key);
                                                    println!("[Core {}] Synced DELETE execution frame for key: '{}'", core_id, incoming_data.key);
                                                } else {
                                                    local_kv_store.insert(incoming_data.key.clone(), incoming_data.value.clone());
                                                    println!("[Core {}] Replicated and saved data frame successfully: '{}' -> '{}'", core_id, incoming_data.key, incoming_data.value);
                                                }
                                            
                                                // Respond back with a small acknowledgment frame
                                                let mut ack_buf = [0u8; 9];
                                                ack_buf[0] = message::TYPE_REPLICATE_ACK;
                                                ack_buf[1..9].copy_from_slice(&raft_state.current_term.to_be_bytes());
                                                raft_network.send_packet(raft_state.leader_id.unwrap_or(0), message::TYPE_REPLICATE_ACK, &ack_buf[1..9]);
                                            }



                                            message::TYPE_REPLICATE_ACK => {
                                                // Extract the follower's term from the wire buffer safely
                                                let follower_term = u64::from_be_bytes(wire_buffer[1..9].try_into().unwrap());
                                                println!("[Core {}] Intercepted replication acknowledgment wire frame for Term {}.", core_id, follower_term);
                                            }



                           
                                           _ => {}
                                       }
                                   }
                               }
                        }                           


                    }
        
                }
            }

        });
        
        thread_handles.push(handle);
    }

    // Store thread handles so the main thread can join them.
    for thread in thread_handles{
        thread.join().unwrap();
    }
    Ok(())
}