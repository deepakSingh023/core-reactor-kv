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
mod node;
mod raft;

mod sharding;
mod inter_core;
use crossbeam_channel::{unbounded, Sender, Receiver};
use sharding::calculate_shard;
use inter_core::InterCoreMessage;

#[derive(Debug)]
enum ConnType {
    Listener,
    Client, 
    Doorbell
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
pub mod raft_proto {
    tonic::include_proto!("raft");
}
fn main() -> Result<(), std::io::Error> {

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

            //Instead of blindly opening a listener it goes into linux kernel and get a raw unconfigured network file descriptor
            let socket = socket2::Socket::new(socket2::Domain::IPV4,socket2::Type::STREAM, Some(socket2::Protocol::TCP)).unwrap();
            
            // Allow multiple reactor threads to bind the same address.
            socket.set_reuse_port(true).unwrap();

       
            let address : std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
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
            
        
            let mut events = [EpollEvent::empty(); 64];
        
            loop {
                
                //Wait for the event while the thread sleeps then the ready connections can be accessed from the events array
                let epoll_count = match epoll.wait(
                    &mut events,
                    nix::sys::epoll::EpollTimeout::NONE
                ) {
                    Ok(count) => count,
                    Err(e) => {
                        eprintln!("[Core {}] epoll_wait error: {}", core_id, e);
                        continue;
                    }
                };

        
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
                                                
                                                // --- 2. LOCAL DATABASE FUNCTIONALITY (IT BELONGS TO US!) ---
                                                // The routing check passed! Now we parse and execute the database operation.
                                                let response_string = match cmd_type {
                                                    "SET" => {
                                                        let value = parts.next().unwrap_or("");
                                                        local_kv_store.insert(key.to_string(), value.to_string());
                                                        "+OK\r\n".to_string()
                                                    }
                                                    "GET" => {
                                                        match local_kv_store.get(key) {
                                                            Some(val) => format!("+{}\r\n", val),
                                                            None => "-ERR KEY_NOT_FOUND\r\n".to_string(),
                                                        }
                                                    }
                                                    "DELETE" => {
                                                        if local_kv_store.remove(key).is_some() {
                                                            "+OK\r\n".to_string()
                                                        } else {
                                                            "-ERR KEY_NOT_FOUND\r\n".to_string()
                                                        }
                                                    }
                                                    _ => "-ERR UNKNOWN_COMMAND\r\n".to_string(),
                                                };
                                    
                                                // --- 3. TRANSITION TO OUTBOUND: Clear input and stage the database response ---
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
                                let adopted_fd = parcel.client_fd; // Fix field name from parcel.fd to parcel.client_fd
                                let mut adopted_conn = parcel.conn;
                                
                                adopted_conn.conn_type = ConnType::Client;
                        
                                if let Some(ref stream) = adopted_conn.stream {
                                    let adopted_event = EpollEvent::new(EpollFlags::EPOLLIN | EpollFlags::EPOLLET, adopted_fd as u64);
                                    epoll.add(stream, adopted_event).expect("Failed to register adopted client stream");
                                }
                        
                                connections.insert(adopted_fd as i32, adopted_conn);
                                println!("[Core {}] Successfully adopted connection fd: {}", core_id, adopted_fd);
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