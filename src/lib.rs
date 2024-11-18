#![deny(missing_docs)]
//! A simple key/value store

mod client;
mod common;
mod server;
mod storage;
mod thread_pool;

pub use client::{Client, ClientError, ClientResult};
pub use server::{Server, ServerError, ServerResult};
pub use storage::{Bitcask, Sled, Storage, StorageError, StorageResult};
pub use thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};
