use std::io::{ErrorKind, Read, Write};
use crate::types::Conn;

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