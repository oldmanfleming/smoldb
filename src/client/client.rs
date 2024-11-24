use crate::net::{
    GetResponse, ListResponse, NetError, NetReadExt, NetWriteExt, RemoveResponse, Request,
    SetResponse,
};
use std::{
    net::SocketAddr,
    sync::{MutexGuard, PoisonError},
};
use thiserror::Error;

use super::pool::Pool;

/// The `ClientError` type for `Client`.
#[derive(Error, Debug)]
pub enum ClientError {
    /// An IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// A Codec error.
    #[error("Codec error: {0}")]
    Codec(#[from] NetError),

    /// A server error.
    #[error("Server error: {0}")]
    Server(String),

    /// An acquire error.
    #[error("Acquire error: {0}")]
    Acquire(#[from] tokio::sync::AcquireError),

    /// Mutex Poisoned error.
    #[error("A mutex was poisoned: {0}")]
    MutexPoisoned(String),
}

impl<T> From<PoisonError<MutexGuard<'_, T>>> for ClientError {
    fn from(err: PoisonError<MutexGuard<'_, T>>) -> Self {
        ClientError::MutexPoisoned(err.to_string())
    }
}

/// The `ClientResult` type for `Client`.
pub type ClientResult<T> = std::result::Result<T, ClientError>;

/// The client for the smoldb server.
#[derive(Clone)]
pub struct Client {
    pool: Pool,
}

// TODO: We should not unwrap reads but instead handle none
// This requires modifying the pool to allow for manual de-allocation of connections
impl Client {
    /// Connects to the smoldb server at the given address.
    pub fn connect(addr: SocketAddr, pool_size: usize) -> Self {
        let pool = Pool::new(addr, pool_size);
        Self { pool }
    }

    /// Gets the string value of a given string key.
    pub async fn get(&self, key: String) -> ClientResult<Option<String>> {
        let request = Request::Get { key };
        let mut conn = self.pool.get().await?;
        conn.writer.write(request).await?;
        let response: GetResponse = conn.reader.read().await?.unwrap();
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }

    /// Sets the value of a string key to a string.
    pub async fn set(&self, key: String, value: String) -> ClientResult<()> {
        let request = Request::Set { key, value };
        let mut conn = self.pool.get().await?;
        conn.writer.write(request).await?;
        let response: SetResponse = conn.reader.read().await?.unwrap();
        match response {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }

    /// Remove a given key.
    pub async fn remove(&self, key: String) -> ClientResult<()> {
        let request = Request::Remove { key };
        let mut conn = self.pool.get().await?;
        conn.writer.write(request).await?;
        let response: RemoveResponse = conn.reader.read().await?.unwrap();
        match response {
            RemoveResponse::Ok(()) => Ok(()),
            RemoveResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }

    /// List all keys.
    pub async fn list(&self) -> ClientResult<Vec<String>> {
        let request = Request::List;
        let mut conn = self.pool.get().await?;
        conn.writer.write(request).await?;
        let response: ListResponse = conn.reader.read().await?.unwrap();
        match response {
            ListResponse::Ok(keys) => Ok(keys),
            ListResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }
}
