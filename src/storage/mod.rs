mod bitcask;

use std::string::FromUtf8Error;
use thiserror::Error;

pub use bitcask::Bitcask;

/// The `Engine` trait for the various storage engines.
pub trait Storage {
    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> StorageResult<Option<String>>;

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> StorageResult<()>;

    /// Remove a given key.
    ///
    /// Returns `StorageError::KeyNotFound` if the key does not exist.
    fn remove(&mut self, key: String) -> StorageResult<()>;

    /// List all keys.
    fn list_keys(&self) -> Vec<String>;
}

/// The `StorageError` type for `Storage`.
#[derive(Error, Debug)]
pub enum StorageError {
    /// IO error.
    #[error("An IO error occurred: {0}")]
    Io(#[from] std::io::Error),

    /// SystemTime error.
    #[error("A system time error occurred: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    /// Key not found error.
    #[error("Key not found")]
    KeyNotFound,

    /// UTF-8 decoding error.
    #[error("A UTF-8 decoding error occured: {0}")]
    Utf8(#[from] FromUtf8Error),

    /// Data corruption error.
    #[error("A data corruption error was detected. Stored checksum: {0}, Calculated checksum:{1}")]
    DataCorruption(u16, u16),

    /// Unexpected error.
    #[error("An unexpected error occurred: {0}")]
    Unexpected(String),
}

/// The `Result` type for `Storage`.
pub type StorageResult<T> = std::result::Result<T, StorageError>;
