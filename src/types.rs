use serde::{Deserialize, Serialize};
use serde_json::Error;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Slice {
    pub start: u64, // inclusive
    pub end: u64,   // inclusive
}

impl Slice {
    pub fn new(s: u64, e: u64) -> Slice {
        Slice { start: s, end: e }
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn deserialize(serialized: &[u8]) -> Result<Slice, Error> {
        serde_json::from_slice(&serialized)
    }
}
