use thiserror::Error;

/// The `KvsError` type for `KvStore`.
#[derive(Error, Debug)]
pub enum KvsError {
    /// IO error.
    #[error("An IO error occurred: {0}")]
    Io(#[from] std::io::Error),

    /// SystemTime error.
    #[error("A system time error occurred: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    /// Key not found error.
    #[error("Key not found")]
    KeyNotFound,

    /// Data corruption error.
    #[error("Data corruption error: {0}")]
    DataCorruption(String),
}

/// The `Result` type for `KvStore`.
pub type Result<T> = std::result::Result<T, KvsError>;
