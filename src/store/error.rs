use thiserror::Error;

/// The `KvsError` type for `KvStore`.
#[derive(Error, Debug)]
pub enum KvsError {
}

/// The `Result` type for `KvStore`.
pub type Result<T> = std::result::Result<T, KvsError>;
