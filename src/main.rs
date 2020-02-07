#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::{thread, time};

use crate::message::{Assignment, Get};

pub mod message;
pub mod types;

const PORT: u16 = 4333;

fn handle_client(stream: TcpStream, _counter: Arc<RwLock<HashMap<String, String>>>) {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut buffer = Vec::new();
    'read: while match reader.read_until(b'\n', &mut buffer) {
        Ok(size) => {
            if size == 0 {
                break 'read;
            }
            trace!("stream read {} bytes", size);

            let _get = match Get::deserialize(&buffer[..size]) {
                Ok(message) => message,
                Err(e) => {
                    error!("deserialization failed: {}", e);
                    continue 'read;
                }
            };

            let assignment = Assignment {
                addresses: vec![
                    String::from("54.183.196.119"),
                    String::from("13.52.220.64"),
                    String::from("18.144.90.156"),
                ],
            };

            let serialized = assignment.serialize();
            writer.write_all(serialized.as_bytes()).unwrap();
            writer.flush().unwrap();
            buffer.clear();
            true
        }
        Err(error) => {
            stream.shutdown(Shutdown::Both).unwrap();
            error!("stream read failed: {}", error);
            false
        }
    } {}
}

fn assigner_loop(counter: Arc<RwLock<HashMap<String, String>>>) {
    loop {
        trace!("generating assignments");

        {
            // Sample shared data structure modification.
            let mut map = counter.write().unwrap();
            map.insert("hello".to_string(), "world".to_string());
        }

        thread::sleep(time::Duration::from_millis(3000));
    }
}

fn main() {
    simple_logger::init().unwrap();

    // TODO: Broadcast Update's to all task servers

    // Shared assignment table.
    let assignments = HashMap::new();
    let counter = Arc::new(RwLock::new(assignments));

    // Spawn and detach thread for assignment generation.
    let assigner_counter = Arc::clone(&counter);
    thread::spawn(move || {
        assigner_loop(assigner_counter);
    });

    // Listen and handle incoming client connections.
    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();
    for stream in listener.incoming() {
        let client_counter = Arc::clone(&counter);
        match stream {
            Ok(stream) => {
                info!("client successfully connected");
                thread::spawn(move || {
                    handle_client(stream, client_counter);
                });
            }
            Err(e) => {
                error!("client connect failed: {}", e);
            }
        }
    }
    drop(listener);
}
