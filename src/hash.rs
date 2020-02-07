use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Hashes an application key into a slice key.
pub fn to_slice_key<T: Hash>(app_key: &T) -> u64 {
    let mut s = DefaultHasher::new();
    app_key.hash(&mut s);
    s.finish()
}
