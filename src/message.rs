use serde::{Serialize, Deserialize};
use serde_json::Error;

use crate::types::Slice;

// Assigner -> task
#[derive(Serialize, Deserialize, Debug)]
pub struct Update {
    pub assigned: Vec<Slice>,
    pub unassigned: Vec<Slice>,
}

impl Update {
    pub fn serialize(&self) -> String {
        let serialized = serde_json::to_string(&self).unwrap();
        add_newline(serialized)
    }

    pub fn deserialize(serialized: &[u8]) -> Result<Update, Error> {
        serde_json::from_slice(&serialized)
    }
}

// Client -> assigner
#[derive(Serialize, Deserialize, Debug)]
pub struct Get {
    pub key: String,
}

impl Get {
    pub fn serialize(&self) -> String {
        let serialized = serde_json::to_string(&self).unwrap();
        add_newline(serialized)
    }

    pub fn deserialize(serialized: &[u8]) -> Result<Get, Error> {
        serde_json::from_slice(&serialized)
    }
}

// Assigner -> client
#[derive(Serialize, Deserialize, Debug)]
pub struct Assignment {
    pub addresses: Vec<String>,
}

impl Assignment {
    pub fn serialize(&self) -> String {
        let serialized = serde_json::to_string(&self).unwrap();
        add_newline(serialized)
    }

    pub fn deserialize(serialized: &[u8]) -> Result<Assignment, Error> {
        serde_json::from_slice(&serialized)
    }
}

fn add_newline(mut serialized: String) -> String {
    // Add newline to end of serialized string.
    let mut buffer = [0; 2];
    let result = '\n'.encode_utf8(&mut buffer);
    serialized.push_str(result);
    serialized
}
