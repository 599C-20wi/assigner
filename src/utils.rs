use std::io::{Error, Write};
use std::net::TcpStream;

use crate::message::Update;

// Port the assigner uses to talk to the task server.
const TASK_PORT: u16 = 4233;

pub fn send_update(task: &str, msg: Update) -> Result<(), Error> {
    debug!("sending assignment update to {}, update={:?}", task, msg);
    let task = format!("{}:{}", task, TASK_PORT);
    match TcpStream::connect(task) {
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
