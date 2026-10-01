use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use libc::TLS_1_2_VERSION_MAJOR;
use nix::libc::EPOLLIN;
use nix::sys::epoll::{
    Epoll,
    EpollCreateFlags,
    EpollEvent,
    EpollFlags,
};
use socket2::SockAddr;
mod node;
mod raft;

use node::node::Node;
enum ConnType {
    Listener,
    Client,   
}
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

    // Store thread handles so the main thread can join them.    
    let mut thread_handles = vec![];

    for core_id in 0..total_cores{

        // Spawn one reactor thread for this core.
        let handle = std::thread::spawn(move || {
 

            // Get available CPU cores to pin reactor threads.
            let core_ids = core_affinity::get_core_ids().unwrap();


            if let Some(target_core) = core_ids.get(core_id) {
                core_affinity::set_for_current(*target_core);
            }

            // Store connections by file descriptor for quick lookup.
            let mut connections = HashMap::new();

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
        
                                    if !disconnected && ! con.input_buffer.is_empty(){
                                        con.output_buffer.append(&mut con.input_buffer);
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