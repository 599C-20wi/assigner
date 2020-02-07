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

const PORT: u16 = 4333;

const TASK_ONE_ADDR: &str = "54.183.196.119:4333";
const TASK_TWO_ADDR: &str = "13.52.220.64:4333";
const TASK_THREE_ADDR: &str = "18.144.90.156:4333";

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
    assignments.insert(TASK_ONE_ADDR, vec![Slice::new(0, max / 3)]);
    assignments.insert(
        TASK_TWO_ADDR,
        vec![Slice::new((max / 3) + 1, (max / 3) * 2)],
    );
    assignments.insert(TASK_THREE_ADDR, vec![Slice::new((max / 3) * 2 + 1, max)]);
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
    send_update(
        &TASK_ONE_ADDR,
        Update::new(inital_assignments.get(TASK_ONE_ADDR).unwrap(), &Vec::new()),
    )
    .unwrap();

    send_update(
        &TASK_TWO_ADDR,
        Update::new(inital_assignments.get(TASK_TWO_ADDR).unwrap(), &Vec::new()),
    )
    .unwrap();

    send_update(
        &TASK_THREE_ADDR,
        Update::new(
            inital_assignments.get(TASK_THREE_ADDR).unwrap(),
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
