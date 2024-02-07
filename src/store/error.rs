use thiserror::Error;

/// The `KvsError` type for `KvStore`.
#[derive(Error, Debug)]
pub enum KvsError {
    /// IO error.
    #[error("An IO error occurred: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization or deserialization error.
    #[error("A serialization or deserialization error occurred: {0}")]
    Serde(#[from] serde_json::Error),

    /// Key not found error.
    #[error("Key not found")]
    KeyNotFound,
}

/// The `Result` type for `KvStore`.
pub type Result<T> = std::result::Result<T, KvsError>;
