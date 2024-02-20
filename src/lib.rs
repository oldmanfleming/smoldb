#![deny(missing_docs)]
//! A simple key/value store

mod storage;

pub use storage::{Bitcask, Storage, StorageError, StorageResult};
