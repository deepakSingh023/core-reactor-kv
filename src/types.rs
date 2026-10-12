use std::os::fd::{AsRawFd, RawFd};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};


#[derive(Debug)]
pub struct Conn {
    pub fd: RawFd,
    pub stream: Option<TcpStream>, 
    pub input_buffer: Vec<u8>,
    pub output_buffer: Vec<u8>,
    pub conn_type: ConnType, 
}

#[derive(Debug,Clone, Copy, PartialEq, Eq)]
pub enum ConnType {
    Listener,
    Client, 
    Doorbell,
    RaftServer, // Listens for new incoming peer network dials
    RaftPeer,   // An active data channel link with a peer node core
}

pub struct PendingWrite {
    pub key: String,
    pub value: String,
    pub is_delete: bool,
    pub client_fd: RawFd,
    pub acked: [bool; 4],      // index = node_id
    pub started_at: Instant,
}
impl Conn {
    pub fn new(fd: RawFd, stream: Option<TcpStream>, conn_type: ConnType) -> Self {
        // Construct and return the Conn instance
        Self {
            fd,                     
            stream,                 
            conn_type,              
            input_buffer: Vec::new(),  
            output_buffer: Vec::new(), 
        }
    }
}