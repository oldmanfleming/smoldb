use std::{
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use thiserror::Error;
use tracing::{debug, error};

use crate::{
    common::{GetResponse, ListResponse, RemoveResponse, Request, SetResponse},
    Storage, StorageError, ThreadPool,
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

    /// A thread pool error.
    #[error("Thread pool error: {0}")]
    RayonThreadPoolError(#[from] rayon::ThreadPoolBuildError),

    /// A Ctrl-C error.
    #[error("Ctrl-C error: {0}")]
    CtrlC(#[from] ctrlc::Error),
}

/// The `ServerResult` type for `Server`.
pub type ServerResult<T> = std::result::Result<T, ServerError>;

/// The `Server` type for the smoldb server.
pub struct Server<S: Storage, T: ThreadPool> {
    storage: S,
    thread_pool: T,
}

impl<S: Storage, T: ThreadPool> Server<S, T> {
    /// Creates a new `Server` with the given storage engine.
    pub fn new(storage: S, thread_pool: T) -> Self {
        Server {
            storage,
            thread_pool,
        }
    }

    /// Runs the server at the given address.
    pub fn run(&mut self, addr: SocketAddr) -> ServerResult<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let storage = self.storage.clone();
                    self.thread_pool.spawn(move || {
                        if let Err(e) = serve(storage, stream) {
                            error!("Error serving connection: {}", e);
                        }
                    });
                }
                Err(err) => error!("Connection failed: {}", err),
            }
        }

        Ok(())
    }
}

fn serve<S: Storage>(storage: S, stream: TcpStream) -> ServerResult<()> {
    let peer_addr = stream.peer_addr()?;
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let request: Request = bincode::deserialize_from(reader)?;

    match request {
        Request::Get { key } => {
            debug!("{}: get {}", peer_addr, &key);
            let response = match storage.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(e) => GetResponse::Err(e.to_string()),
            };
            bincode::serialize_into(&mut writer, &response)?;
            writer.flush()?;
        }
        Request::Set { key, value } => {
            debug!("{}: set {} {}", peer_addr, &key, &value);
            let response = match storage.set(key, value) {
                Ok(()) => SetResponse::Ok(()),
                Err(e) => SetResponse::Err(e.to_string()),
            };
            bincode::serialize_into(&mut writer, &response)?;
            writer.flush()?;
        }
        Request::Remove { key } => {
            debug!("{}: remove {}", peer_addr, &key);
            let response = match storage.remove(key) {
                Ok(()) => RemoveResponse::Ok(()),
                Err(e) => RemoveResponse::Err(e.to_string()),
            };
            bincode::serialize_into(&mut writer, &response)?;
            writer.flush()?;
        }
        Request::List => {
            debug!("{}: list", peer_addr);
            let keys = storage.list_keys();
            let response = ListResponse::Ok(keys);
            bincode::serialize_into(&mut writer, &response)?;
            writer.flush()?;
        }
    }

    Ok(())
}
