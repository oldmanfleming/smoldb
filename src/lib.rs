#![deny(missing_docs)]
//! A simple key/value store

mod client;
mod net;
mod server;

pub use client::{Client, ClientError, ClientResult};
pub use server::{run, ServerError, ServerResult, StorageType};
