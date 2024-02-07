use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, Write},
    path::PathBuf,
};

use crate::{KvsError, Result};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use smoldb::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
    path_buf: PathBuf,
}

impl KvStore {
    /// Opens a `KvStore` at a given path.
    ///
    /// If the path does not exist, it will be created.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path_buf: PathBuf = path.into();
        path_buf.push("db");

        Ok(KvStore {
            store: HashMap::new(),
            path_buf,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let file = self.get_file()?;

        self.load_store(&file)?;

        Ok(self.store.get(&key).cloned())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let file = self.get_file()?;

        self.write_command(file, Command::Set { key, value })?;

        Ok(())
    }

    /// Remove a given key.
    ///
    /// Returns `KvsError::KeyNotFound` if the key does not exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let file = self.get_file()?;

        self.load_store(&file)?;

        if self.store.get(&key).is_none() {
            return Err(KvsError::KeyNotFound);
        }

        self.write_command(file, Command::Remove { key })?;

        Ok(())
    }

    fn get_file(&mut self) -> Result<std::fs::File> {
        Ok(fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&self.path_buf)?)
    }

    fn load_store(&mut self, file: &std::fs::File) -> Result<()> {
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let command: Command = serde_json::from_str(&line?)?;

            match command {
                Command::Set { key, value } => {
                    self.store.insert(key, value);
                }
                Command::Remove { key } => {
                    self.store.remove(&key);
                }
            }
        }

        Ok(())
    }

    fn write_command(&mut self, file: std::fs::File, command: Command) -> Result<()> {
        let serialized = serde_json::to_vec(&command)?;

        let mut writer = std::io::BufWriter::new(file);

        writer.write_all(&serialized)?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
