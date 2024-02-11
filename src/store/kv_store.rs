use crate::{KvsError, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

const TOMBSTONE: &str = "";

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored on disk using the Bitcask append-only log format.
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
pub struct KvStore {
    key_dir: HashMap<String, Entry>,
    path_buf: PathBuf,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

#[derive(Debug, Clone)]
struct Entry {
    file_id: String,
    value_len: u32,
    value_pos: u64,
    timestamp: u64,
}

impl KvStore {
    /// Opens a `KvStore` at a given path.
    ///
    /// If the path does not exist, it will be created.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path_buf: PathBuf = path.into();
        path_buf.push("db");

        let writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path_buf)?,
        );
        let reader = BufReader::new(File::open(&path_buf)?);

        let mut store = KvStore {
            key_dir: HashMap::new(),
            path_buf,
            writer,
            reader,
        };

        while let Some((key, entry)) = store.read_next_entry()? {
            store.key_dir.insert(key, entry);
        }

        Ok(store)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.key_dir.get(&key).cloned() {
            if entry.value_len == 0 {
                return Ok(None);
            }

            return Ok(Some(self.read_value(&entry)?));
        }
        Ok(None)
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = self.write_entry(&key, &value)?;
        self.key_dir.insert(key, entry);
        Ok(())
    }

    /// Remove a given key.
    ///
    /// Returns `KvsError::KeyNotFound` if the key does not exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_dir.get(&key).is_none() {
            return Err(KvsError::KeyNotFound);
        }
        let entry = self.write_entry(&key, &TOMBSTONE.to_string())?;
        self.key_dir.insert(key, entry);
        Ok(())
    }

    // Write a key/value pair to the given writer in the bitcask format.
    // An entry indicating the location of the value for the given key is returned.
    // Fixed-width header            Variable-length body
    //+=====+=====+=====+====== - - +============== - - +
    //| u16 | u64 | u32 | u32       | [u8] | [u8] |
    //+=====+=====+=====+====== - - +============== - - +
    // checksum (2 bytes)
    // timestamp (8 bytes)
    // key_len (4 bytes)
    // val_len (4 bytes)
    // key (key_len bytes)
    // value (val_len bytes)
    fn write_entry(&mut self, key: &String, value: &String) -> Result<Entry> {
        let key_len = key.len();
        let value_len = value.len();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut entry = Vec::<u8>::with_capacity(8 + 4 + 4 + key_len + value_len);

        entry.write_u64::<BigEndian>(timestamp)?;
        entry.write_u32::<BigEndian>(key_len as u32)?;
        entry.write_u32::<BigEndian>(value_len as u32)?;
        entry.write_all(key.as_bytes())?;
        entry.write_all(value.as_bytes())?;

        let checksum = X25.checksum(&entry);

        self.writer.write_u16::<BigEndian>(checksum)?;
        self.writer.write_all(&entry)?;
        self.writer.flush()?;

        let value_pos = self.writer.seek(std::io::SeekFrom::Current(0))? - value_len as u64;

        Ok(Entry {
            file_id: String::new(),
            value_len: value_len as u32,
            value_pos,
            timestamp,
        })
    }

    // Read the next key/value entry from the given reader in the bitcask format.
    // Fixed-width header            Variable-length body
    //+=====+=====+=====+====== - - +============== - - +
    //| u16 | u64 | u32 | u32       | [u8] | [u8] |
    //+=====+=====+=====+====== - - +============== - - +
    // checksum (2 bytes)
    // timestamp (8 bytes)
    // key_len (4 bytes)
    // val_len (4 bytes)
    // key (key_len bytes)
    // value (val_len bytes)
    fn read_next_entry(&mut self) -> Result<Option<(String, Entry)>> {
        // Check if we are at the end of the reader
        // Move back to the current position after checking
        let current_pos = self.reader.seek(std::io::SeekFrom::Current(0))?;
        if current_pos == self.reader.seek(std::io::SeekFrom::End(0))? {
            return Ok(None);
        }
        self.reader.seek(std::io::SeekFrom::Start(current_pos))?;

        let checksum = self.reader.read_u16::<BigEndian>()?;
        let timestamp = self.reader.read_u64::<BigEndian>()?;
        let key_len = self.reader.read_u32::<BigEndian>()?;
        let value_len = self.reader.read_u32::<BigEndian>()?;

        let mut key_bytes = vec![0; key_len as usize];
        self.reader.read_exact(&mut key_bytes)?;

        let value_pos = self.reader.seek(std::io::SeekFrom::Current(0))?;

        let mut value_bytes = vec![0; value_len as usize];
        self.reader.read_exact(&mut value_bytes)?;

        let mut entry_bytes =
            Vec::<u8>::with_capacity(8 + 4 + 4 + key_len as usize + value_len as usize);
        entry_bytes.write_u64::<BigEndian>(timestamp)?;
        entry_bytes.write_u32::<BigEndian>(key_len)?;
        entry_bytes.write_u32::<BigEndian>(value_len)?;
        entry_bytes.write_all(&key_bytes)?;
        entry_bytes.write_all(&value_bytes)?;

        let read_checksum = X25.checksum(&entry_bytes);

        if checksum != read_checksum {
            return Err(KvsError::DataCorruption(checksum, read_checksum));
        }

        let entry = Entry {
            file_id: String::new(),
            value_len,
            value_pos,
            timestamp,
        };

        let key = String::from_utf8(key_bytes)?;

        Ok(Some((key, entry)))
    }

    fn read_value(&mut self, entry: &Entry) -> Result<String> {
        self.reader
            .seek(std::io::SeekFrom::Start(entry.value_pos))?;

        let mut value_bytes = vec![0; entry.value_len as usize];
        self.reader.read_exact(&mut value_bytes)?;

        Ok(String::from_utf8(value_bytes)?)
    }
}
