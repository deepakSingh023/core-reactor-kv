use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use nix::libc::EPOLLIN;
use nix::sys::epoll::{
    Epoll,
    EpollCreateFlags,
    EpollEvent,
    EpollFlags,
};


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


fn main() -> Result<(), std::io::Error> {

    println!("hello world");

    let mut connections = HashMap::new();

    let listener = TcpListener::bind("127.0.0.1:8080")?;

    match listener.set_nonblocking(true) {
        Ok(()) => {
            println!("listener is non-blocking");
        }

        Err(error) => {
            println!("failed to make listener non-blocking: {}", error);
        }
    }

    let epoll = Epoll::new(EpollCreateFlags::empty())?;

    let event = EpollEvent::new(
        EpollFlags::EPOLLIN,
        listener.as_raw_fd() as u64,
    );

    epoll.add(&listener, event)?;

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

        let epoll_count = epoll.wait(&mut events,  nix::sys::epoll::EpollTimeout::NONE)?;

        for i in 0..epoll_count {

            let fd = events[i].data();
            let fd2 = fd as i32;
            let Some(con) = connections.get_mut(&fd2) else {continue;};

            match con.conn_type{
                ConnType::Listener=>{

                    // Keep it flat, but skip if there's nothing to read right now
                    let Ok((stream, addr)) = listener.accept() else { continue; };


                    stream.set_nonblocking(true)?;

                    let client_fd = stream.as_raw_fd() as u64;

                    let event2 = EpollEvent::new(EpollFlags::EPOLLIN | EpollFlags::EPOLLET,client_fd);

                    epoll.add(&stream,event2)?;
                    
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
}