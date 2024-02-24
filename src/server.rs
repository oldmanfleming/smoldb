use std::{
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use thiserror::Error;
use tracing::{debug, error};

use crate::{
    common::{GetResponse, ListResponse, RemoveResponse, Request, SetResponse},
    Storage, StorageError,
};

/// The `ServerError` type for `Server`.
#[derive(Error, Debug)]
pub enum ServerError {
    /// An IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// A storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    /// A bincode error.
    #[error("Serde error: {0}")]
    Bincode(#[from] bincode::Error),
}

/// The `ServerResult` type for `Server`.
pub type ServerResult<T> = std::result::Result<T, ServerError>;

/// The `Server` type for the smoldb server.
pub struct Server<S: Storage> {
    storage: S,
}

impl<S: Storage> Server<S> {
    /// Creates a new `Server` with the given storage engine.
    pub fn new(storage: S) -> Self {
        Server { storage }
    }

    /// Runs the server at the given address.
    pub fn run(&mut self, addr: SocketAddr) -> ServerResult<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error serving connection: {}", e);
                    }
                }
                Err(err) => error!("Connection failed: {}", err),
            }
        }

        Ok(())
    }

    fn serve(&mut self, stream: TcpStream) -> ServerResult<()> {
        let peer_addr = stream.peer_addr()?;
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let request: Request = bincode::deserialize_from(reader)?;
        match request {
            Request::Get { key } => {
                let response = match self.storage.get(key.clone()) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(e.to_string()),
                };
                bincode::serialize_into(&mut writer, &response)?;
                writer.flush()?;
                debug!("{}: get {}", peer_addr, key)
            }
            Request::Set { key, value } => {
                let response = match self.storage.set(key.clone(), value) {
                    Ok(()) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(e.to_string()),
                };
                bincode::serialize_into(&mut writer, &response)?;
                writer.flush()?;
                debug!("{}: set {}", peer_addr, key)
            }
            Request::Remove { key } => {
                let response = match self.storage.remove(key.clone()) {
                    Ok(()) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(e.to_string()),
                };
                bincode::serialize_into(&mut writer, &response)?;
                writer.flush()?;
                debug!("{}: remove {}", peer_addr, key)
            }
            Request::List => {
                let keys = self.storage.list_keys();
                let response = ListResponse::Ok(keys);
                bincode::serialize_into(&mut writer, &response)?;
                writer.flush()?;
                debug!("{}: list", peer_addr)
            }
        }

        Ok(())
    }
}
