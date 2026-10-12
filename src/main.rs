use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use nix::sys::epoll::{
    Epoll,
    EpollCreateFlags,
    EpollEvent,
    EpollFlags,
};
use std::collections::VecDeque;
mod election;
use sharding::calculate_shard;
use inter_core::InterCoreMessage;
mod state;      // Points directly to your src/state.rs file

mod message;    // Points directly to your src/message.rs file
mod sharding;
mod inter_core;
mod types;
use state::{RaftState};
mod raft_net;
use types::{Conn, ConnType , PendingWrite };
mod conn_io;
use conn_io::{read_available,flush_output,reply_to_client,bind_client_listener,register_connection,register_raw_fd};
mod command;
use command::{execute_command , expire_pending_writes};
mod peer;
use peer::{handle_peer_message,tick_raft};

fn main() -> Result<(), std::io::Error> {

    // Collect command line arguments: cargo run -- <node_id>
    let args: Vec<String> = std::env::args().collect();
    let my_node_id: u64 = if args.len() > 1 {
        args[1].parse().unwrap_or(0)
    } else {
        0 // Fallback default to Node 0 if no argument passed
    };

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

            let mut raft_state = RaftState::new(my_node_id, core_id);

            // A single call boots up the whole sharded Raft network stack for this thread!
            let mut raft_network = raft_net::CoreRaftNetwork::new(my_node_id, core_id, 4);

            let listener = match bind_client_listener(8080 + my_node_id) {
                Ok(l) => l,
                Err(e) => { eprintln!("[Core {}] Failed to bind client listener: {}", core_id, e); return; }
            };
        
            //Creating new epoll
            let epoll: Epoll = match Epoll::new(EpollCreateFlags::empty()) {
                Ok(epoll) => epoll,
                Err(e) => {
                    eprintln!("[Core {}] Failed to create epoll: {}", core_id, e);
                    return;
                }
            }; 

            for (fd, kind) in [
                (listener.as_raw_fd(), ConnType::Listener),
                (core_channels.my_eventfd, ConnType::Doorbell),
                (raft_network.listener_fd, ConnType::RaftServer),
            ] {
                if let Err(e) = register_raw_fd(&epoll, &mut connections, fd, kind) {
                    eprintln!("[Core {}] Failed to register fd {}: {}", core_id, fd, e);
                    return;
                }
            }
            
            let mut timeout_queue: VecDeque<u64> = VecDeque::new();

            //We define the pending ledger and the id
            let mut pending: HashMap<u64, PendingWrite> = HashMap::new();
            let mut next_id: u64 = 0;
            let mut to_reply: Vec<RawFd> = Vec::new();
            
        
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

                //timout   queue logic to return time out error
                expire_pending_writes(&mut connections, &mut pending, &mut timeout_queue);

                if epoll_count == 0 {
                    raft_network.retry_missing_connections(my_node_id, core_id, 4);

                    tick_raft(&mut raft_state,  &mut raft_network, core_id, my_node_id);
                }

                for i in 0..epoll_count {
        
                    let fd = events[i].data();
                    let fd2 = fd as i32;
                    let Some(con) = connections.get_mut(&fd2) else {continue;};
        
                    match con.conn_type{
                        ConnType::Listener=>{
        
                            let Ok((stream, _)) = listener.accept() else { continue; };
                            if let Err(e) = register_connection(&epoll, &mut connections, stream, ConnType::Client) {
                                eprintln!("[Core {}] Failed to register client: {}", core_id, e);
                            }
                        }
                        ConnType::Client=>{
        
                            if events[i].events().contains(EpollFlags::EPOLLIN){

                                let mut disconnected = !read_available(con);
        
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
                                                    core_channels.forward_to_core(assigned_core, fd2, removed_conn);
                                                }
                                                continue; // Connection successfully pushed out. Stop processing locally!
                                            }
                                            
                                            let value = parts.next().unwrap_or("");
                                            let response_string = execute_command(
                                                cmd_type, key, value, fd2,            // in the Doorbell arm use adopted_fd instead of fd2
                                                &raft_state, &local_kv_store, &mut raft_network,
                                                &mut pending, &mut timeout_queue, &mut next_id, my_node_id,
                                            );
                                            con.input_buffer.clear(); // We fully processed the input command!
                                            con.output_buffer.extend_from_slice(response_string.as_bytes()); // Stage response bytes
                                        }
                                    }
                                }

                                if !disconnected && !con.output_buffer.is_empty() {
                                    disconnected = !flush_output(con);
                                }
                                
                                if disconnected {
                                    connections.remove(&fd2); 
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
                                    if let Err(e) = epoll.add(stream, adopted_event) {
                                        eprintln!("[Core {}] Failed to adopt fd {}: {}", core_id, adopted_fd, e);
                                        continue;
                                    }
                                }

                                if !adopted_conn.input_buffer.is_empty() && adopted_conn.input_buffer.contains(&b'\n') {
                                    if let Ok(payload_str) = std::str::from_utf8(&adopted_conn.input_buffer) {
                                        let clean_command = payload_str.trim();
                                        let mut parts = clean_command.split_whitespace();
                                        let cmd_type = parts.next().unwrap_or("");
                                        let key = parts.next().unwrap_or("");
                                        let value = parts.next().unwrap_or("");

                                        let response_string = execute_command(
                                            cmd_type, key, value, adopted_fd,            
                                            &raft_state, &local_kv_store, &mut raft_network,
                                            &mut pending, &mut timeout_queue, &mut next_id, my_node_id,
                                        );

                                        // Clear the input space and stage the database response bytes
                                        adopted_conn.input_buffer.clear();
                                        adopted_conn.output_buffer.extend_from_slice(response_string.as_bytes());
                                    }
                                }

                                flush_output(&mut adopted_conn);
                                connections.insert(adopted_fd, adopted_conn);
                            }
                        }

                        ConnType::RaftServer => {
                            // FIX: Access the underlying Raft listener object inside your raft_network struct!
                            let Ok((peer_stream, _)) = raft_network._listener.accept() else { continue; };
                            if let Err(e) = register_connection(&epoll, &mut connections, peer_stream, ConnType::RaftPeer) {
                                eprintln!("[Core {}] Failed to register peer: {}", core_id, e);
                            }
                            
                            println!("[Core {}] Accepted incoming peer consensus line connection.", core_id);
                        }

                        ConnType::RaftPeer => {
                               if let Some(ref mut peer_stream) = con.stream {
                                   // Read raw wire byte arrays out of our non-blocking peer cable channel
                                   while let Some((msg_type, payload)) = raft_network.receive_packet(peer_stream) {
                                    
                                    handle_peer_message(msg_type, &payload, &mut raft_state, &mut raft_network, &mut local_kv_store, &mut pending, &mut to_reply, core_id, my_node_id);
                                        
                                   }
                               }
                        }                           

                    }
        
                }

                for fd in to_reply.drain(..) {
                    reply_to_client(&mut connections, fd, "+OK\r\n");
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