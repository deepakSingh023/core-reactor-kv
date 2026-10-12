use std::io::{ErrorKind, Read, Write};
use libc::posix_spawnattr_t;

use crate::types::Conn;
use std::collections::HashMap;
use std::os::fd::RawFd;
use std::net::{TcpListener,TcpStream};
pub fn read_available(con: &mut Conn) -> bool {
    let Some(stream) = con.stream.as_mut() else { return false; };
    let mut scratch_buf = [0u8; 4096];
    loop {
        match stream.read(&mut scratch_buf) {
            Ok(0) => {
                println!("no data here to read");
                return false;                       // client closed
            }
            Ok(bytes_read) => {
                con.input_buffer.extend_from_slice(&scratch_buf[0..bytes_read]);
                if bytes_read < scratch_buf.len() {
                    return true;                    // kernel buffer drained
                }
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => return true,
            Err(e) => {
                println!("Read error on fd {}: {}", con.fd, e);
                return false;
            }
        }
    }
}

pub fn flush_output(con: &mut Conn) -> bool {
    let Some(stream) = con.stream.as_mut() else { return true; };
    while !con.output_buffer.is_empty() {
        match stream.write(&con.output_buffer) {
            Ok(0) => return false,
            Ok(bytes_written) => {
                con.output_buffer.drain(0..bytes_written);   // remove what was sent
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => return true, // kernel full: keep the rest queued
            Err(e) => {
                println!("Write error on fd {}: {}", con.fd, e);
                return false;
            }
        }
    }
    true
}

pub fn reply_to_client(connections: &mut HashMap<RawFd, Conn>, fd: RawFd, msg: &str) {
    if let Some(con) = connections.get_mut(&fd) {
        con.output_buffer.extend_from_slice(msg.as_bytes());
        if !flush_output(con) {
            connections.remove(&fd);      // broken connection: drop it
        }
    }
}

pub fn bind_client_listener(port: u64) -> std::io::Result<TcpListener> { 
            //Instead of blindly opening a listener it goes into linux kernel and get a raw unconfigured network file descriptor
            let socket = socket2::Socket::new(socket2::Domain::IPV4,socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
            
            // Allow multiple reactor threads to bind the same address.
            socket.set_reuse_port(true)?;

            let current_client_port = 8080 + port; 
                let address: std::net::SocketAddr = format!("127.0.0.1:{}", current_client_port)
                   .parse()
                   .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            
            socket.bind(&socket2::SockAddr::from(address))?;

            socket.listen(128)?;

            //It makes the listener non blocking
            socket.set_nonblocking(true)?;

            // Convert the configured socket into a TcpListener.            
            let listener: std::net::TcpListener = socket.into();

            Ok(listener)

 }