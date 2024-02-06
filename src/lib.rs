#![deny(missing_docs)]
//! A simple key/value store

mod store;

pub use store::{KvStore, KvsError, Result};
