use crate::{KvsError, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    collections::HashMap,
    fs,
    io::{Read, Seek, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

const TOMBSTONE: &[u8; 0] = b"";

#[derive(Debug, Clone)]
struct Entry {
    file_id: String,
    value_len: u32,
    value_pos: u64,
    timestamp: u64,
}

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
    key_dir: HashMap<Vec<u8>, Entry>,
    path_buf: PathBuf,
}

impl KvStore {
    /// Opens a `KvStore` at a given path.
    ///
    /// If the path does not exist, it will be created.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path_buf: PathBuf = path.into();
        path_buf.push("db");

        let mut store = KvStore {
            key_dir: HashMap::new(),
            path_buf,
        };

        store.load_store()?;

        Ok(store)
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.key_dir.get(key.as_bytes()).cloned() {
            if entry.value_len == 0 {
                return Ok(None);
            }

            let file = self.get_file()?;
            let mut reader = std::io::BufReader::new(file);
            let value_bytes = self.read_value(&mut reader, &entry)?;
            let value = String::from_utf8(value_bytes).map_err(|err| {
                KvsError::DataCorruption(format!("Invalid UTF-8 sequence: {}", err))
            })?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let file = self.get_file()?;
        let mut writer = std::io::BufWriter::new(file);
        let entry = self.write_entry(&mut writer, key.as_bytes(), value.as_bytes())?;
        self.key_dir.insert(key.into(), entry);

        Ok(())
    }

    /// Remove a given key.
    ///
    /// Returns `KvsError::KeyNotFound` if the key does not exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let key_bytes = key.as_bytes();
        if self.key_dir.get(key_bytes).is_none() {
            return Err(KvsError::KeyNotFound);
        }
        let file = self.get_file()?;
        let mut writer = std::io::BufWriter::new(file);
        let entry = self.write_entry(&mut writer, key_bytes, TOMBSTONE)?;
        self.key_dir.insert(key_bytes.to_vec(), entry);
        Ok(())
    }

    fn get_file(&mut self) -> Result<std::fs::File> {
        Ok(fs::OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&self.path_buf)?)
    }

    fn load_store(&mut self) -> Result<()> {
        let file = self.get_file()?;
        let mut reader = std::io::BufReader::new(file);
        while let Some((key, entry)) = self.read_entry(&mut reader)? {
            self.key_dir.insert(key, entry);
        }
        Ok(())
    }

    // Write a key/value pair to the given writer in the bitcask format.
    // Fixed-width header Variable-length body
    //+=====+=====+=====+====== - - +============== - - +
    //| u16 | u64 | u32 | u32       | [u8] | [u8] |
    //+=====+=====+=====+====== - - +============== - - +
    // checksum (2 bytes)
    // timestamp (8 bytes)
    // key_len (4 bytes)
    // val_len (4 bytes)
    // key (key_len bytes)
    // value (val_len bytes)
    fn write_entry<W: Write + Seek>(
        &mut self,
        writer: &mut W,
        key: &[u8],
        value: &[u8],
    ) -> Result<Entry> {
        let key_len = key.len();
        let value_len = value.len();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut entry = Vec::<u8>::with_capacity(8 + 4 + 4 + key_len + value_len);

        entry.write_u64::<BigEndian>(timestamp)?;
        entry.write_u32::<BigEndian>(key_len as u32)?;
        entry.write_u32::<BigEndian>(value_len as u32)?;
        entry.write_all(key)?;
        entry.write_all(value)?;

        let checksum = X25.checksum(&entry);

        writer.write_u16::<BigEndian>(checksum)?;

        writer.write_all(&entry)?;

        writer.flush()?;

        let value_pos = writer.seek(std::io::SeekFrom::Current(0))? - value_len as u64;

        Ok(Entry {
            file_id: String::new(),
            value_len: value_len as u32,
            value_pos,
            timestamp,
        })
    }

    fn read_entry<R: Read + Seek>(&self, reader: &mut R) -> Result<Option<(Vec<u8>, Entry)>> {
        // Check if we are at the end of the reader
        let current_pos = reader.seek(std::io::SeekFrom::Current(0))?;
        if current_pos == reader.seek(std::io::SeekFrom::End(0))? {
            return Ok(None);
        }
        reader.seek(std::io::SeekFrom::Start(current_pos))?;

        let checksum = reader.read_u16::<BigEndian>()?;
        let timestamp = reader.read_u64::<BigEndian>()?;
        let key_len = reader.read_u32::<BigEndian>()?;
        let value_len = reader.read_u32::<BigEndian>()?;

        let mut key = vec![0; key_len as usize];
        reader.read_exact(&mut key)?;

        let value_pos = reader.seek(std::io::SeekFrom::Current(0))?;

        let mut value = vec![0; value_len as usize];
        reader.read_exact(&mut value)?;

        let mut entry_bytes =
            Vec::<u8>::with_capacity(8 + 4 + 4 + key_len as usize + value_len as usize);
        entry_bytes.write_u64::<BigEndian>(timestamp)?;
        entry_bytes.write_u32::<BigEndian>(key_len)?;
        entry_bytes.write_u32::<BigEndian>(value_len)?;
        entry_bytes.write_all(&key)?;
        entry_bytes.write_all(&value)?;

        let read_checksum = X25.checksum(&entry_bytes);

        if checksum != read_checksum {
            return Err(KvsError::DataCorruption(format!(
                "Checksum mismatch: expected {}, got {}",
                checksum, read_checksum
            )));
        }

        let entry = Entry {
            file_id: String::new(),
            value_len,
            value_pos,
            timestamp,
        };

        Ok(Some((key, entry)))
    }

    fn read_value<R: Read + Seek>(&self, reader: &mut R, entry: &Entry) -> Result<Vec<u8>> {
        reader.seek(std::io::SeekFrom::Start(entry.value_pos))?;

        let mut value = vec![0; entry.value_len as usize];
        reader.read_exact(&mut value)?;

        Ok(value)
    }
}
