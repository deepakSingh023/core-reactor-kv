// src/reactor.rs

use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout};
use crate::inter_core::{CoreChannels, InterCoreMessage};
use crate::sharding::calculate_shard;

/// Represents the nature of the network socket active in our epoll loop.
enum ConnType {
    Listener,    // The shared client-facing port 8080 socket
    Client,      // An active connection with a user client app
    Doorbell,    // Our personal cross-core eventfd handle
}

struct Conn {
    fd: RawFd,
    conn_type: ConnType,
}

/// The core loop runner execution context.
/// 
/// INTERVIEW DEFENSE NOTE:
/// This is a pure Shared-Nothing execution context. Every core runs this function 
/// on its own CPU core thread, managing its own private memory and private epoll matrix.
pub fn run_reactor_loop(
    core_id: usize,
    listener: std::net::TcpListener,
    channels: CoreChannels,
) {
    // 1. Store local connections mapping in this thread's private stack frame memory.
    // No other CPU core can read or write to this map. Zero lock contention.
    let mut local_connections: HashMap<RawFd, Conn> = HashMap::new();

    // 2. Initialize a local, separate Epoll instance for this core thread.
    let epoll = Epoll::new(EpollCreateFlags::empty()).expect("Failed to create local epoll");

    // 3. Register the Client Port 8080 Listener into our epoll map
    let listener_fd = listener.as_raw_fd();
    let listener_event = EpollEvent::new(EpollFlags::EPOLLIN, listener_fd as u64);
    epoll.add(&listener, listener_event).expect("Failed to register listener fd");
    local_connections.insert(listener_fd, Conn { fd: listener_fd, conn_type: ConnType::Listener });

    // 4. Register our personal EventFd Doorbell into our epoll map
    let doorbell_fd = channels.my_eventfd;
    let doorbell_event = EpollEvent::new(EpollFlags::EPOLLIN, doorbell_fd as u64);
    
    // Low-level unsafe wrapper to register a raw OS file descriptor into epoll
    unsafe {
        let raw_fd_struct = std::os::fd::BorrowedFd::borrow_raw(doorbell_fd);
        epoll.add(&raw_fd_struct, doorbell_event).expect("Failed to register doorbell fd");
    }
    local_connections.insert(doorbell_fd, Conn { fd: doorbell_fd, conn_type: ConnType::Doorbell });

    // Scratchpad buffer array to collect triggered operating system events
    let mut triggered_events = [EpollEvent::empty(); 64];

    println!("[Core {}] Reactor Loop successfully armed and listening.", core_id);

    loop {
        // Core thread falls asleep inside the kernel here. Wakes up when I/O happens.
        // It consumes ZERO CPU cycles while sleeping.
        let event_count = match epoll.wait(&mut triggered_events, EpollTimeout::NONE) {
            Ok(count) => count,
            Err(_) => continue,
        };

        for i in 0..event_count {
            let active_fd = triggered_events[i].data() as RawFd;
            
            // Look up what kind of connection triggered this specific event
            let Some(conn) = local_connections.get_mut(&active_fd) else { continue; };

            match conn.conn_type {
                
                // =================================================================
                // CASE 1: AN OUTSIDE CLIENT ATTEMPTS TO CONNECT ON PORT 8080
                // =================================================================
                ConnType::Listener => {
                    // Accept the socket connection frame from the kernel line
                    let Ok((stream, _)) = listener.accept() else { continue; };
                    stream.set_nonblocking(true).expect("Failed non-blocking mode configuration");
                    
                    let client_fd = stream.as_raw_fd();

                    // --- THE ROUTING CHECKS ---
                    // Hardcoded dummy key for testing connection routing lifecycle.
                    // Later, we parse the true key payload string from the incoming socket stream buffer.
                    let sample_key = "user_account_balance"; 
                    let assigned_shard_core = calculate_shard(sample_key);

                    if assigned_shard_core == core_id {
                        // This shard belongs to ME. Keep ownership locally.
                        let client_event = EpollEvent::new(EpollFlags::EPOLLIN, client_fd as u64);
                        epoll.add(&stream, client_event).expect("Failed to register client fd");
                        
                        local_connections.insert(client_fd, Conn { fd: client_fd, conn_type: ConnType::Client });
                        println!("[Core {}] Intercepted new client connection. I own it natively.", core_id);
                    } else {
                        // This shard belongs to someone else! Drop the descriptor index into their mailbox.
                        println!("[Core {}] Intercepted client meant for Core {}. Forwarding connection...", core_id, assigned_shard_core);
                        
                        let parcel = InterCoreMessage { client_fd };
                        
                        if let Ok(()) = channels.senders[assigned_shard_core].try_send(parcel) {
                            // Ring the target core's doorbell handle by writing an 8-byte numeric increment token
                            let wake_token: [u8; 8] = 1u64.to_ne_bytes();
                            let target_doorbell = channels.peer_eventfds[assigned_shard_core];
                            unsafe {
                                nix::libc::write(target_doorbell, wake_token.as_ptr() as *const std::ffi::c_void, 8);
                            }
                        } else {
                            // Outbound mailbox pipe is congested. Close connection directly to shield system.
                            unsafe { nix::libc::close(client_fd); }
                        }
                    }
                }

                // =================================================================
                // CASE 2: ANOTHER CORE RANG OUR DOORBELL (EVENTFD FIRED)
                // =================================================================
                ConnType::Doorbell => {
                    // We MUST consume the 8-byte value out of the eventfd counter register.
                    // If we do not read it, the kernel will think data is still pending and fire epoll loop infinitely.
                    let mut drain_buf = [0u8; 8];
                    unsafe {
                        nix::libc::read(active_fd, drain_buf.mut_ptr() as *mut std::ffi::c_void, 8);
                    }

                    // Open our personal incoming mailbox channel and adopt all incoming file descriptors
                    while let Ok(parcel) = channels.receiver.try_recv() {
                        let adopted_fd = parcel.client_fd;
                        
                        // Register this newly adopted socket directly into OUR local epoll map loop
                        unsafe {
                            let adopted_socket = std::os::fd::BorrowedFd::borrow_raw(adopted_fd);
                            let client_event = EpollEvent::new(EpollFlags::EPOLLIN, adopted_fd as u64);
                            epoll.add(&adopted_socket, client_event).expect("Failed to register adopted client fd");
                        }

                        local_connections.insert(adopted_fd, Conn { fd: adopted_fd, conn_type: ConnType::Client });
                        println!("[Core {}] Successfully adopted forwarded client connection descriptor index: {}.", core_id, adopted_fd);
                    }
                }

                // =================================================================
                // CASE 3: ACTIVE CLIENT SENT DATA BYTES OR DISCONNECTED
                // =================================================================
                ConnType::Client => {
                    let mut buf = [0u8; 1024];
                    // Perform plain synchronous system call read
                    let bytes_read = unsafe {
                        nix::libc::read(active_fd, buf.as_mut_ptr() as *mut std::ffi::c_void, 1024)
                    };

                    if bytes_read <= 0 {
                        // Connection dropped by user or read failure. Purge connection references.
                        epoll.delete(&unsafe { std::os::fd::BorrowedFd::borrow_raw(active_fd) }).unwrap();
                        local_connections.remove(&active_fd);
                        unsafe { nix::libc::close(active_fd); }
                        println!("[Core {}] Disconnected client descriptor index: {}", core_id, active_fd);
                    } else {
                        // Data packet received!
                        // INTERVIEW DEFENSE PREP:
                        // This is where our single-threaded local Raft rules check logic fires safely.
                        println!("[Core {}] Received request payload ({} bytes) from active client socket.", core_id, bytes_read);
                        
                        // Return simple success token back down client link path
                        let response_bytes = b"+OK\r\n";
                        unsafe {
                            nix::libc::write(active_fd, response_bytes.as_ptr() as *const std::ffi::c_void, response_bytes.len());
                        }
                    }
                }
            }
        }
    }
}
