#![deny(missing_docs)]
//! A simple key/value store

mod error;
mod storage;

pub use error::{Result, SmolError};
pub use storage::Storage;
