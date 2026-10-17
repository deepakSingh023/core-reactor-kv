// usage: loadgen <set|get> <connections> <seconds> <host:port,host:port,...>
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

type Conn = (TcpStream, BufReader<TcpStream>);

fn worker(id: usize, mode: &str, addrs: &[String], deadline: Instant) -> (Vec<u128>, u64) {
    let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ (id as u64 + 1);
    let mut leader: HashMap<String, String> = HashMap::new(); // key -> address that accepted the write
    let mut open: HashMap<String, Conn> = HashMap::new();
    let (mut latencies, mut errors) = (Vec::new(), 0u64);

    while Instant::now() < deadline {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; // cheap random numbers
        let key = format!("key{}", seed % 1000);
        let mut addr = leader.get(&key).cloned()
            .unwrap_or_else(|| addrs[(seed as usize / 1000) % addrs.len()].clone());
        let line = if mode == "set" { format!("SET {} v\n", key) } else { format!("GET {}\n", key) };

        let start = Instant::now();
        let mut reply = String::new();
        for _ in 0..3 {                                   // follow up to 3 redirects
            if !open.contains_key(&addr) {
                match TcpStream::connect(&addr) {
                    Ok(s) => {
                        s.set_read_timeout(Some(Duration::from_secs(3))).ok();
                        let r = BufReader::new(s.try_clone().unwrap());
                        open.insert(addr.clone(), (s, r));
                    }
                    Err(_) => break,
                }
            }
            let (s, r) = open.get_mut(&addr).unwrap();
            reply.clear();
            if s.write_all(line.as_bytes()).is_err() || r.read_line(&mut reply).is_err() || reply.is_empty() {
                open.remove(&addr);
                reply.clear();
                break;
            }
            if reply.starts_with("-MOVED") {
                addr = reply.split_whitespace().nth(2).unwrap_or("").to_string();
                leader.insert(key.clone(), addr.clone());
                continue;
            }
            break;
        }
        if reply.is_empty() || (reply.starts_with("-ERR") && !reply.contains("KEY_NOT_FOUND")) {
            errors += 1;
        }
        latencies.push(start.elapsed().as_micros());
    }
    (latencies, errors)
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 5 {
        eprintln!("usage: loadgen <set|get> <connections> <seconds> <host:port,host:port,...>");
        return;
    }
    let mode = a[1].clone();
    let connections: usize = a[2].parse().expect("connections");
    let secs: u64 = a[3].parse().expect("seconds");
    let addrs: Vec<String> = a[4].split(',').map(String::from).collect();
    let deadline = Instant::now() + Duration::from_secs(secs);

    let handles: Vec<_> = (0..connections).map(|id| {
        let (mode, addrs) = (mode.clone(), addrs.clone());
        std::thread::spawn(move || worker(id, &mode, &addrs, deadline))
    }).collect();

    let (mut all, mut errors) = (Vec::new(), 0u64);
    for h in handles { let (l, e) = h.join().unwrap(); all.extend(l); errors += e; }
    all.sort();
    let n = all.len();
    if n == 0 { println!("no requests completed"); return; }
    println!("ops={} ops/s={:.0} p50={:.2}ms p99={:.2}ms errors={}",
        n, n as f64 / secs as f64, all[n / 2] as f64 / 1000.0, all[n * 99 / 100] as f64 / 1000.0, errors);
}