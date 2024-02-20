use std::string::FromUtf8Error;

use thiserror::Error;

/// The `StorageError` type for `Storage`.
#[derive(Error, Debug)]
pub enum SmolError {
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
pub type Result<T> = std::result::Result<T, SmolError>;
