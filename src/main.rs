#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use crate::message::{Assignment, Get, Update};
use crate::types::Slice;

pub mod message;
pub mod types;
pub mod utils;

mod assigner;

// Port the client uses to talk to the task server.
const CLIENT_PORT: u16 = 3333;
// Port to listen for client connections on.
const LISTEN_PORT: u16 = 4333;

const TASK_ONE_ADDRESS: &str = "54.241.208.105";
const TASK_TWO_ADDRESS: &str = "18.144.148.168";
const TASK_THREE_ADDRESS: &str = "52.9.0.84";

fn handle_client(stream: TcpStream, counter: Arc<RwLock<HashMap<&str, Vec<Slice>>>>) {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut buffer = Vec::new();
    'read: while match reader.read_until(b'\n', &mut buffer) {
        Ok(size) => {
            if size == 0 {
                break 'read;
            }
            trace!("stream read {} bytes", size);

            let get = match Get::deserialize(&buffer[..size]) {
                Ok(message) => message,
                Err(e) => {
                    error!("deserialization failed: {}", e);
                    continue 'read;
                }
            };

            let slice_key = get.slice_key;

            // Determine the assigned task servers for the slice.
            let mut client_assignments: Vec<String> = Vec::new();
            let assignments = counter.read().unwrap();
            for (&server, slices) in assignments.iter() {
                for slice in slices {
                    if slice_key >= slice.start && slice_key <= slice.end {
                        let task = format!("{}:{}", server, CLIENT_PORT);
                        client_assignments.push(task);
                    }
                }
            }
            trace!(
                "assignment for slice {}: {:?}",
                slice_key,
                client_assignments
            );

            let assignment = Assignment {
                addresses: client_assignments,
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

fn set_inital_assignments(counter: Arc<RwLock<HashMap<&str, Vec<Slice>>>>) {
    let mut assignments = counter.write().unwrap();
    let max = std::u64::MAX;
    assignments.insert(TASK_ONE_ADDRESS, vec![Slice::new(0, max / 3)]);
    assignments.insert(
        TASK_TWO_ADDRESS,
        vec![Slice::new((max / 3) + 1, (max / 3) * 2)],
    );
    assignments.insert(TASK_THREE_ADDRESS, vec![Slice::new((max / 3) * 2 + 1, max)]);
}

fn main() {
    simple_logger::init().unwrap();

    // Shared assignment table.
    let assignments = HashMap::new();
    let counter = Arc::new(RwLock::new(assignments));

    // Set initial assignments using a uniform distribution of the key space.
    let set_counter = Arc::clone(&counter);
    set_inital_assignments(set_counter);

    // Send inital assignments to task servers.
    {
        let send_counter = Arc::clone(&counter);
        let inital_assignments = send_counter.read().unwrap();
        utils::send_update(
            &TASK_ONE_ADDRESS,
            Update::new(
                inital_assignments.get(TASK_ONE_ADDRESS).unwrap(),
                &Vec::new(),
            ),
        )
        .unwrap();

        utils::send_update(
            &TASK_TWO_ADDRESS,
            Update::new(
                inital_assignments.get(TASK_TWO_ADDRESS).unwrap(),
                &Vec::new(),
            ),
        )
        .unwrap();

        utils::send_update(
            &TASK_THREE_ADDRESS,
            Update::new(
                inital_assignments.get(TASK_THREE_ADDRESS).unwrap(),
                &Vec::new(),
            ),
        )
        .unwrap();
    }

    let assigner_counter = Arc::clone(&counter);
    assigner::run(assigner_counter);

    // Listen and handle incoming client connections.
    let listener = TcpListener::bind(format!("0.0.0.0:{}", LISTEN_PORT)).unwrap();
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
