#![deny(missing_docs)]
//! A simple key/value store

mod client;
mod common;
mod storage;
mod server;

pub use storage::{Bitcask, Storage, StorageError, StorageResult};
pub use client::{Client, ClientError, ClientResult};
pub use server::{Server, ServerError, ServerResult};
