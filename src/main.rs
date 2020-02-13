#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::{thread, time};

use crate::message::{Assignment, Get, Update};
use crate::types::Slice;

pub mod message;
pub mod types;

const SLEEP_MILLIS: u64 = 5000;

const CLIENT_PORT: u16 = 3333;
const TASK_PORT: u16 = 4233;
const LISTEN_PORT: u16 = 4333;

const TASK_ONE_ADDRESS: &str = "54.241.208.105";
const TASK_TWO_ADDRESS: &str = "18.144.148.168";
const TASK_THREE_ADDRESS: &str = "52.9.0.84";

fn handle_client(stream: TcpStream, _counter: Arc<RwLock<HashMap<&str, Vec<Slice>>>>) {
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
                    format!("{}:{}", TASK_ONE_ADDRESS, CLIENT_PORT),
                    format!("{}:{}", TASK_TWO_ADDRESS, CLIENT_PORT),
                    format!("{}:{}", TASK_THREE_ADDRESS, CLIENT_PORT),
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

fn send_update(task_addr: &str, msg: Update) -> Result<(), io::Error> {
    match TcpStream::connect(task_addr) {
        Ok(mut stream) => {
            let serialized = msg.serialize();
            stream.write_all(serialized.as_bytes()).unwrap();
            Ok(())
        }
        Err(e) => {
            error!("failed to connect to task server: {}", e);
            Err(e)
        }
    }
}

fn set_inital_assignments(counter: Arc<RwLock<HashMap<&str, Vec<Slice>>>>) {
    let mut assignments = counter.write().unwrap();
    let max = std::u64::MAX;
    assignments.insert("54.241.208.105:4233", vec![Slice::new(0, max / 3)]);
    assignments.insert(
        "18.144.148.168:4233",
        vec![Slice::new((max / 3) + 1, (max / 3) * 2)],
    );
    assignments.insert("52.9.0.84:4233", vec![Slice::new((max / 3) * 2 + 1, max)]);
}

fn assigner_loop(_counter: Arc<RwLock<HashMap<&str, Vec<Slice>>>>) {
    loop {
        trace!("generating assignments");

        // TODO: Add assignment generation logic here.

        thread::sleep(time::Duration::from_millis(SLEEP_MILLIS));
    }
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
    let send_counter = Arc::clone(&counter);
    let inital_assignments = send_counter.read().unwrap();
    let task1: String = format!("{}:{}", TASK_ONE_ADDRESS, TASK_PORT);
    send_update(
        &task1,
        Update::new(
            inital_assignments.get(&task1.as_str()).unwrap(),
            &Vec::new(),
        ),
    )
    .unwrap();

    let task2: String = format!("{}:{}", TASK_TWO_ADDRESS, TASK_PORT);
    send_update(
        &task2,
        Update::new(
            inital_assignments.get(&task2.as_str()).unwrap(),
            &Vec::new(),
        ),
    )
    .unwrap();

    let task3: String = format!("{}:{}", TASK_THREE_ADDRESS, TASK_PORT);
    send_update(
        &task3,
        Update::new(
            inital_assignments.get(&task3.as_str()).unwrap(),
            &Vec::new(),
        ),
    )
    .unwrap();

    // Spawn and detach thread for assignment generation.
    let assigner_counter = Arc::clone(&counter);
    thread::spawn(move || {
        assigner_loop(assigner_counter);
    });

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
