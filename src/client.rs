use std::{
    io::{BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

use thiserror::Error;

use crate::common::{GetResponse, ListResponse, RemoveResponse, Request, SetResponse};

/// The `Client` type for the smoldb client.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

/// The `ClientError` type for `Client`.
#[derive(Error, Debug)]
pub enum ClientError {
    /// An IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// A bincode error.
    #[error("Serde error: {0}")]
    Bincode(#[from] bincode::Error),

    /// A server error.
    #[error("Server error: {0}")]
    Server(String),
}

/// The `ClientResult` type for `Client`.
pub type ClientResult<T> = std::result::Result<T, ClientError>;

impl Client {
    /// Connects to the smoldb server at the given address.
    pub fn connect(addr: &SocketAddr) -> ClientResult<Self> {
        let reader = TcpStream::connect(addr)?;
        let writer = reader.try_clone()?;

        Ok(Client {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        })
    }

    /// Gets the string value of a given string key.
    pub fn get(&mut self, key: String) -> ClientResult<Option<String>> {
        let request = Request::Get { key };
        bincode::serialize_into(&mut self.writer, &request)?;
        self.writer.flush()?;
        let response: GetResponse = bincode::deserialize_from(&mut self.reader)?;
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }

    /// Sets the value of a string key to a string.
    pub fn set(&mut self, key: String, value: String) -> ClientResult<()> {
        let request = Request::Set { key, value };
        bincode::serialize_into(&mut self.writer, &request)?;
        self.writer.flush()?;
        let response: SetResponse = bincode::deserialize_from(&mut self.reader)?;
        match response {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> ClientResult<()> {
        let request = Request::Remove { key };
        bincode::serialize_into(&mut self.writer, &request)?;
        self.writer.flush()?;
        let response: RemoveResponse = bincode::deserialize_from(&mut self.reader)?;
        match response {
            RemoveResponse::Ok(()) => Ok(()),
            RemoveResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }

    /// List all keys.
    pub fn list(&mut self) -> ClientResult<Vec<String>> {
        let request = Request::List;
        bincode::serialize_into(&mut self.writer, &request)?;
        self.writer.flush()?;
        let response: ListResponse = bincode::deserialize_from(&mut self.reader)?;
        match response {
            ListResponse::Ok(keys) => Ok(keys),
            ListResponse::Err(e) => Err(ClientError::Server(e)),
        }
    }
}
