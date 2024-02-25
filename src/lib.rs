#![deny(missing_docs)]
//! A simple key/value store

mod client;
mod common;
mod server;
mod storage;

pub use client::{Client, ClientError, ClientResult};
pub use server::{Server, ServerError, ServerResult};
pub use storage::{Bitcask, Sled, Storage, StorageError, StorageResult};
