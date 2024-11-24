use futures::sink::SinkExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// The `NetError` type.
#[derive(Error, Debug)]
pub enum NetError {
    /// An IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// A bincode error.
    #[error("Serde error: {0}")]
    Bincode(#[from] bincode::Error),

    /// A server error.
    #[error("Unexected error: {0}")]
    Unexected(String),
}

/// The `NetResult` type.
pub type NetResult<T> = std::result::Result<T, NetError>;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
    List,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SetResponse {
    Ok(()),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RemoveResponse {
    Ok(()),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ListResponse {
    Ok(Vec<String>),
    Err(String),
}

/// Helper trait for reading our defined request/response types from a tcp stream.
pub trait NetReadExt {
    async fn read<E: DeserializeOwned>(&mut self) -> NetResult<Option<E>>;
}

/// Helper trait for writing our defined request/response types to a tcp stream.
pub trait NetWriteExt {
    async fn write<E: Serialize>(&mut self, request: E) -> NetResult<()>;
}

impl NetReadExt for OwnedReadHalf {
    async fn read<E: DeserializeOwned>(&mut self) -> NetResult<Option<E>> {
        let mut reader = FramedRead::new(self, LengthDelimitedCodec::new());
        if let Some(ser) = reader.next().await {
            Ok(Some(bincode::deserialize(&ser?)?))
        } else {
            Ok(None)
        }
    }
}

impl NetWriteExt for OwnedWriteHalf {
    async fn write<E: Serialize>(&mut self, request: E) -> NetResult<()> {
        let mut writer = FramedWrite::new(self, LengthDelimitedCodec::new());
        let ser = bincode::serialize(&request)?;
        writer.send(ser.into()).await?;
        writer.flush().await?;
        Ok(())
    }
}
