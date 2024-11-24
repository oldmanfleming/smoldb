use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::oneshot,
};
use tracing::{debug, error};

use crate::net::{
    GetResponse, ListResponse, NetError, NetReadExt, NetWriteExt, RemoveResponse, Request,
    SetResponse,
};

use super::storage::{Bitcask, Sled, Storage, StorageError};

/// The `ServerError` type for `Server`.
#[derive(Error, Debug)]
pub enum ServerError {
    /// An IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// A storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    /// A codec error.
    #[error("Codec error: {0}")]
    CodecError(#[from] NetError),
}

/// The `ServerResult` type for `Server`.
pub type ServerResult<T> = std::result::Result<T, ServerError>;

/// The storage type.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum StorageType {
    /// Bitcask storage.
    Bitcask,
    /// Sled storage.
    Sled,
}

/// Runs the smoldb server at the given address with the given stop signal.
pub async fn run(
    addr: SocketAddr,
    dir: PathBuf,
    storage_type: StorageType,
    rx: oneshot::Receiver<()>,
) -> ServerResult<()> {
    let listener = TcpListener::bind(addr).await?;
    match storage_type {
        StorageType::Bitcask => listen(listener, Bitcask::open(&dir)?, rx).await,
        StorageType::Sled => listen(listener, Sled::open(&dir)?, rx).await,
    }
}

async fn listen<S: Storage>(
    listener: TcpListener,
    storage: S,
    rx: oneshot::Receiver<()>,
) -> ServerResult<()> {
    select! {
        _ = async {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("error accepting connection: {}", e);
                        continue;
                    }
                };
                let storage = storage.clone();
                tokio::spawn(async move {
                    let addr = stream.peer_addr().unwrap();
                    match serve(storage, stream).await {
                        Ok(_) => debug!("{}: connection closed", addr),
                        Err(e) => error!("{}: error serving connection: {}", addr, e),
                    }

                });
            }
        } => {},
        _ = rx => {},
    };
    Ok(())
}

async fn serve<S: Storage>(storage: S, stream: TcpStream) -> ServerResult<()> {
    let peer_addr = stream.peer_addr()?;
    let (mut reader, mut writer) = stream.into_split();
    debug!("{}: connection established", peer_addr);
    loop {
        let request = if let Some(r) = reader.read::<Request>().await? {
            r
        } else {
            return Ok(());
        };
        match request {
            Request::Get { key } => {
                debug!("{}: get {}", peer_addr, &key);
                let response = match storage.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("test: {}", e.to_string())),
                };
                writer.write(response).await?;
            }
            Request::Set { key, value } => {
                debug!("{}: set {} {}", peer_addr, &key, &value);
                let response = match storage.set(key, value) {
                    Ok(()) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(e.to_string()),
                };
                writer.write(response).await?;
            }
            Request::Remove { key } => {
                debug!("{}: remove {}", peer_addr, &key);
                let response = match storage.remove(key) {
                    Ok(()) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(e.to_string()),
                };
                writer.write(response).await?;
            }
            Request::List => {
                debug!("{}: list", peer_addr);
                let keys = storage.list_keys();
                let response = ListResponse::Ok(keys);
                writer.write(response).await?;
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use tempfile::TempDir;
//     use tokio::sync::oneshot;
//     use tokio::time;

//     use super::*;

//     #[tokio::test]
//     async fn test_run() {
//         let addr = "127.0.0.1:4014".parse().unwrap();
//         let dir = TempDir::new().unwrap();
//         let storage_type = StorageType::Bitcask;
//         let (tx, rx) = oneshot::channel();
//         let handle = tokio::spawn(async move {
//             run(addr, dir.into_path(), storage_type, rx).await;
//         });
//         time::sleep(time::Duration::from_secs(1)).await;
//         tx.send(()).unwrap();
//         handle.await.unwrap();
//     }
// }
