use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crossbeam_skiplist::SkipMap;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use super::{Storage, StorageError, StorageResult};

const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

const TOMBSTONE: &str = "";

const LOG_FILE_EXT: &str = "log";

const HINT_FILE_EXT: &str = "hint";

const LOWEST_LOG_FILE_ID: u64 = 0;

const LOG_SIZE_THRESHOLD: u64 = 1024 * 1024;

/// `Bitcask` stores string key/value pairs durably on disk using the Bitcask append-only log format.
///
/// The implementation follows the [Bitcask Paper](https://riak.com/assets/bitcask-intro.pdf).
///
/// `Bitcask` is a thread-safe implementation of the `Storage` trait and can be cloned and shared between threads.
///
/// Example:
///
/// ```rust
/// # use smoldb::{Bitcask, Storage, StorageError, StorageResult};
/// let mut storage = Bitcask::open(std::env::current_dir().unwrap()).unwrap();
/// storage.set("key".to_owned(), "value".to_owned()).unwrap();
/// let val = storage.get("key".to_owned()).unwrap();
/// assert_eq!(val, Some("value".to_owned()));
/// ```
///
/// Async Example:
///
/// ```rust
/// # use smoldb::{Bitcask, Storage, StorageError, StorageResult};
/// let storage = Bitcask::open(std::env::current_dir().unwrap()).unwrap();
/// let storage_clone = storage.clone();
/// std::thread::spawn(move || {
///    storage_clone.set("key".to_owned(), "value".to_owned()).unwrap();
/// });
/// std::thread::sleep(std::time::Duration::from_secs(1));
/// let val = storage.get("key".to_owned()).unwrap();
/// assert_eq!(val, Some("value".to_owned()));
#[derive(Clone)]
pub struct Bitcask {
    key_dir: Arc<SkipMap<String, Entry>>,
    path: Arc<PathBuf>,
    writer: Arc<Mutex<Writer>>,
    reader: Reader,
}

impl Bitcask {
    /// Opens `Storage` at a given path.
    ///
    /// If the path does not exist, it will be created.
    pub fn open(path: impl Into<PathBuf>) -> StorageResult<Bitcask> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;

        // Find the highest hint file and then find all the log files that are higher than that hint file.
        let mut hint_file = Option::<u64>::None;
        let mut log_files = Vec::<u64>::new();
        for entry in fs::read_dir(&path)? {
            let file_path = entry?.path();
            let ext = file_path.extension().and_then(|ext| ext.to_str());
            if (ext != Some(LOG_FILE_EXT)) && (ext != Some(HINT_FILE_EXT)) {
                continue;
            }
            let stem = file_path
                .file_stem()
                .and_then(|file_id| file_id.to_str())
                .and_then(|file_id| file_id.parse::<u64>().ok())
                .ok_or(StorageError::Unexpected(format!(
                    "Could not parse file {}",
                    file_path.display()
                )))?;
            match ext {
                Some(LOG_FILE_EXT) => {
                    log_files.push(stem);
                }
                Some(HINT_FILE_EXT) => {
                    if hint_file.map_or(true, |hint_file| stem > hint_file) {
                        hint_file = Some(stem);
                    }
                }
                _ => {}
            }
        }
        let mut log_files: Vec<u64> = log_files
            .into_iter()
            // Tricky detail here: The merge file associated with the hint file file will be excluded from the log files.
            // This is what we want but it's a bit subtle...
            // The merge file shares the same file extension as the log files.
            // But it will be exluded here because it shares the same id as it's hint file and we are evaluating on > hint_file.
            .filter(|file_id| hint_file.map_or(true, |hint_file| file_id > &hint_file))
            .collect();
        log_files.sort_unstable();

        let key_dir = SkipMap::new();
        let mut readers = HashMap::<u64, BufReader<File>>::new();

        // Open a reader for the hint file if it exists
        // Read through hint file and load the key_dir with it's entries
        // Add a reader for the associated merge file to the readers map
        if let Some(hint_file) = hint_file {
            let mut hint_reader = BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(hint_path(&path, &hint_file))?,
            );

            while let Some((key, entry)) = read_next_hint(&mut hint_reader, hint_file.clone())? {
                key_dir.insert(key, entry);
            }

            let merge_reader = BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(log_path(&path, &hint_file))?,
            );
            readers.insert(hint_file, merge_reader);
        }

        // Open a reader for each log file and load the key_dir with it's entries
        // Add a reader for the log file to the readers map
        for file_id in log_files.iter() {
            let mut reader = BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(log_path(&path, &file_id))?,
            );

            while let Some((key, entry)) = read_next_entry(&mut reader, *file_id)? {
                key_dir.insert(key, entry);
            }

            readers.insert(*file_id, reader);
        }

        // Get the last file id or 1 if there are no files
        // The last file is the current file that we write too
        let active_file_id = match (log_files.last(), hint_file) {
            (Some(&last_file_id), _) => last_file_id,
            (None, Some(hint_file_id)) => hint_file_id + 1,
            (None, None) => LOWEST_LOG_FILE_ID,
        };
        let writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path(&path, &active_file_id))?,
        );

        let path = Arc::new(path);

        Ok(Bitcask {
            key_dir: Arc::new(key_dir),
            path: path.clone(),
            writer: Arc::new(Mutex::new(Writer {
                path: path.clone(),
                writer,
                active_file_id,
            })),
            reader: Reader {
                path,
                readers: RefCell::new(readers),
            },
        })
    }
}

impl Storage for Bitcask {
    /// Compacts the storage.
    fn compact(&self) -> StorageResult<()> {
        // Notes:
        // - Merging (compaction) can be done asynchronously instead of on open but it requires that the merge/hint files are not overwritten but
        //   are instead incremented with a new file id. The readers do not necessarily have to be shared between threads in this scenario as the Key_dir
        //   wil be updated as a part of the merge process with new entries that are not in the reader map. And when those are encountered a new reader can be opened.
        // - Merging just becomes dumping the current key_dir into hint/merge files with the next incremented file_id since we acquire a lock on the writer as a part
        //   of this process we can be sure that no new entries are being written to the active file during this time.
        // - Since we are incrementing files, reads can still be served during the merge process as the old files are still available.
        // - Once the key_dir is updated we can clean up the older log files and hint/merge files leaving only the latest merge/hint file with a new log file left.
        //
        // Merge process:
        // 0. Acquire lock on writer
        // 1. Create new merge/hint files
        // 2. Iterate over key_dir and read the value for each entry
        //  - Write the key and value to the merge file
        //  - Write the key and value info to the hint file
        //  - Update the key_dir with the new entry
        // 3. Flush the merge/hint files
        // 4. Set wrtier to new active log file
        // 5. Release lock on writer
        // 6. Remove all old log files
        // 7. Remove old merge and hint files

        let mut writer = self.writer.lock()?;

        let compaction_file_id = writer.active_file_id + 1;
        let mut merge_writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path(&self.path, &compaction_file_id))?,
        );
        let mut hint_writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(hint_path(&self.path, &compaction_file_id))?,
        );

        // Dump the current key_dir into the merge/hint files
        // Update the key_dir with the new hint entry that points to the new merge file
        for item in self.key_dir.iter() {
            let key = item.key();
            let entry = item.value();
            if entry.value_len == 0 {
                continue;
            }

            let value = self.reader.read_value(entry)?;

            let merge_entry = write_value(&mut merge_writer, compaction_file_id, &key, &value)?;

            write_hint(&mut hint_writer, &key, &merge_entry)?;

            self.key_dir.insert(key.clone(), merge_entry);
        }

        merge_writer.flush()?;
        hint_writer.flush()?;

        writer.set_writer(compaction_file_id + 1)?;

        // Release the lock on the writer as the key_dir is now updated
        drop(writer);

        // Anything with file id lower than compaction_file_id can now be safely removed as nothing in the key_dir should point to these files
        //
        let mut readers = self.reader.readers.borrow_mut();
        fs::read_dir(self.path.as_ref())?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_path = entry.path();
                let stem = file_path
                    .file_stem()
                    .and_then(|file_id| file_id.to_str())
                    .and_then(|file_id| file_id.parse::<u64>().ok());
                match stem {
                    Some(file_id) if file_id < compaction_file_id => {
                        readers.remove(&file_id);
                        Some(fs::remove_file(file_path))
                    }
                    _ => None,
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> StorageResult<Option<String>> {
        if let Some(entry) = self.key_dir.get(&key) {
            let entry = entry.value();
            if entry.value_len == 0 {
                return Ok(None);
            }

            return Ok(Some(self.reader.read_value(entry)?));
        }
        Ok(None)
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> StorageResult<()> {
        let mut writer = self.writer.lock().unwrap();
        let entry = writer.write_value(&key, &value)?;
        // If the size of the active file is greater than the threshold we will create a new active file
        //
        // Adding the pos of the last value written to the end of the file with it's length will
        // give us the total size in bytes of the active file.
        if entry.value_pos + (entry.value_len as u64) > LOG_SIZE_THRESHOLD {
            let active_file_id = writer.active_file_id + 1;
            writer.set_writer(active_file_id)?;
        }

        self.key_dir.insert(key, entry);

        Ok(())
    }

    /// Remove a given key.
    ///
    /// Returns `StorageError::KeyNotFound` if the key does not exist.
    fn remove(&self, key: String) -> StorageResult<()> {
        if self.key_dir.get(&key).is_none() {
            return Err(StorageError::KeyNotFound);
        }
        let entry = self
            .writer
            .lock()
            .unwrap()
            .write_value(&key, &TOMBSTONE.to_string())?;
        self.key_dir.insert(key, entry);
        Ok(())
    }

    /// List all keys.
    fn list_keys(&self) -> Vec<String> {
        // Keys that have been removed will still have an entry in the key_dir
        // but the value_len will be 0.
        self.key_dir
            .iter()
            .filter(|entry| entry.value().value_len != 0)
            .map(|entry| entry.key().clone())
            .collect()
    }
}

#[derive(Debug, Clone)]
struct Entry {
    file_id: u64,
    value_len: u32,
    value_pos: u64,
    _timestamp: u64,
}

#[derive(Debug)]
struct Writer {
    path: Arc<PathBuf>,
    writer: BufWriter<File>,
    active_file_id: u64,
}

impl Writer {
    fn write_value(&mut self, key: &String, value: &String) -> StorageResult<Entry> {
        write_value(self.writer.get_mut(), self.active_file_id, key, value)
    }

    fn set_writer(&mut self, active_file_id: u64) -> StorageResult<()> {
        self.writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path(&self.path, &active_file_id))?,
        );
        self.active_file_id = active_file_id;
        Ok(())
    }
}

#[derive(Debug)]
struct Reader {
    path: Arc<PathBuf>,
    readers: RefCell<HashMap<u64, BufReader<File>>>,
}

impl Reader {
    fn read_value(&self, entry: &Entry) -> StorageResult<String> {
        let mut readers = self.readers.borrow_mut();
        if let Some(reader) = readers.get_mut(&entry.file_id) {
            return read_value(reader, entry);
        }
        let mut reader = BufReader::new(
            fs::OpenOptions::new()
                .read(true)
                .open(log_path(&self.path, &entry.file_id))?,
        );
        let value = read_value(&mut reader, entry)?;
        readers.insert(entry.file_id, reader);
        Ok(value)
    }
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Reader {
            path: self.path.clone(),
            readers: RefCell::new(HashMap::new()),
        }
    }
}

fn log_path(path: &Path, gen: &u64) -> PathBuf {
    path.join(format!("{}.log", gen))
}

fn hint_path(path: &Path, gen: &u64) -> PathBuf {
    path.join(format!("{}.hint", gen))
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
fn write_value<W: Write + Seek>(
    writer: &mut W,
    file_id: u64,
    key: &String,
    value: &String,
) -> StorageResult<Entry> {
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

    writer.write_u16::<BigEndian>(checksum)?;
    writer.write_all(&entry)?;
    writer.flush()?;

    let value_pos = writer.seek(std::io::SeekFrom::Current(0))? - value_len as u64;

    Ok(Entry {
        file_id,
        value_len: value_len as u32,
        value_pos,
        _timestamp: timestamp,
    })
}

// Read the next key/value entry from the given reader in the bitcask data format.
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
fn read_next_entry<R: Read + Seek>(
    reader: &mut R,
    file_id: u64,
) -> StorageResult<Option<(String, Entry)>> {
    // Check if we are at the end of the reader
    // Move back to the current position after checking
    let current_pos = reader.seek(std::io::SeekFrom::Current(0))?;
    if current_pos == reader.seek(std::io::SeekFrom::End(0))? {
        return Ok(None);
    }
    reader.seek(std::io::SeekFrom::Start(current_pos))?;

    let checksum = reader.read_u16::<BigEndian>()?;
    let timestamp = reader.read_u64::<BigEndian>()?;
    let key_len = reader.read_u32::<BigEndian>()?;
    let value_len = reader.read_u32::<BigEndian>()?;

    let mut key_bytes = vec![0; key_len as usize];
    reader.read_exact(&mut key_bytes)?;

    let value_pos = reader.seek(std::io::SeekFrom::Current(0))?;

    let mut value_bytes = vec![0; value_len as usize];
    reader.read_exact(&mut value_bytes)?;

    let mut entry_bytes =
        Vec::<u8>::with_capacity(8 + 4 + 4 + key_len as usize + value_len as usize);
    entry_bytes.write_u64::<BigEndian>(timestamp)?;
    entry_bytes.write_u32::<BigEndian>(key_len)?;
    entry_bytes.write_u32::<BigEndian>(value_len)?;
    entry_bytes.write_all(&key_bytes)?;
    entry_bytes.write_all(&value_bytes)?;

    let read_checksum = X25.checksum(&entry_bytes);

    if checksum != read_checksum {
        return Err(StorageError::DataCorruption(checksum, read_checksum));
    }

    let entry = Entry {
        file_id,
        value_len,
        value_pos,
        _timestamp: timestamp,
    };

    let key = String::from_utf8(key_bytes)?;

    Ok(Some((key, entry)))
}

// Read the value for the given entry from the given reader.
fn read_value<R: Read + Seek>(reader: &mut R, entry: &Entry) -> StorageResult<String> {
    reader.seek(std::io::SeekFrom::Start(entry.value_pos))?;

    let mut value_bytes = vec![0; entry.value_len as usize];
    reader.read_exact(&mut value_bytes)?;

    Ok(String::from_utf8(value_bytes)?)
}

// Write a given key/value entry to the writer in the bitcask hint format.
// Fixed-width header            Variable-length body
//+=====+=====+=====+====== - - +======== - - +
//| u64 | u32 | u32 | u64       | [u8] |
//+=====+=====+=====+====== - - +======== - - +
// timestamp (8 bytes)
// key_len (4 bytes)
// val_len (4 bytes)
// val_pos (8 bytes)
// key (key_len bytes)
fn write_hint<W: Write + Seek>(writer: &mut W, key: &String, entry: &Entry) -> StorageResult<()> {
    writer.write_u64::<BigEndian>(entry._timestamp)?;
    writer.write_u32::<BigEndian>(key.len() as u32)?;
    writer.write_u32::<BigEndian>(entry.value_len)?;
    writer.write_u64::<BigEndian>(entry.value_pos)?;
    writer.write_all(key.as_bytes())?;
    Ok(())
}

// Read the next key/value entry from the given reader in the bitcask hint format.
// Fixed-width header            Variable-length body
//+=====+=====+=====+====== - - +======== - - +
//| u64 | u32 | u32 | u64       | [u8] |
//+=====+=====+=====+====== - - +======== - - +
// timestamp (8 bytes)
// key_len (4 bytes)
// val_len (4 bytes)
// val_pos (8 bytes)
// key (key_len bytes)
fn read_next_hint<R: Read + Seek>(
    reader: &mut R,
    file_id: u64,
) -> StorageResult<Option<(String, Entry)>> {
    // Check if we are at the end of the reader
    // Move back to the current position after checking
    let current_pos = reader.seek(std::io::SeekFrom::Current(0))?;
    if current_pos == reader.seek(std::io::SeekFrom::End(0))? {
        return Ok(None);
    }
    reader.seek(std::io::SeekFrom::Start(current_pos))?;

    let timestamp = reader.read_u64::<BigEndian>()?;
    let key_len = reader.read_u32::<BigEndian>()?;
    let value_len = reader.read_u32::<BigEndian>()?;
    let value_pos = reader.read_u64::<BigEndian>()?;

    let mut key_bytes = vec![0; key_len as usize];
    reader.read_exact(&mut key_bytes)?;
    let key = String::from_utf8(key_bytes)?;

    let entry = Entry {
        file_id,
        value_len,
        value_pos,
        _timestamp: timestamp,
    };

    Ok(Some((key, entry)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    // Should get previously stored value.
    #[test]
    fn get_stored_value() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bitcask = Bitcask::open(temp_dir.path())?;

        bitcask.set("key1".to_owned(), "value1".to_owned())?;
        bitcask.set("key2".to_owned(), "value2".to_owned())?;

        assert_eq!(bitcask.get("key1".to_owned())?, Some("value1".to_owned()));
        assert_eq!(bitcask.get("key2".to_owned())?, Some("value2".to_owned()));

        // Open from disk again and check persistent data.
        drop(bitcask);
        let store = Bitcask::open(temp_dir.path())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
        assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

        Ok(())
    }

    // Should overwrite existent value.
    #[test]
    fn overwrite_value() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bitcask = Bitcask::open(temp_dir.path())?;

        bitcask.set("key1".to_owned(), "value1".to_owned())?;
        assert_eq!(bitcask.get("key1".to_owned())?, Some("value1".to_owned()));
        bitcask.set("key1".to_owned(), "value2".to_owned())?;
        assert_eq!(bitcask.get("key1".to_owned())?, Some("value2".to_owned()));

        // Open from disk again and check persistent data.
        drop(bitcask);
        let store = Bitcask::open(temp_dir.path())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
        store.set("key1".to_owned(), "value3".to_owned())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

        Ok(())
    }

    // Should get `None` when getting a non-existent key.
    #[test]
    fn get_non_existent_value() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bitcask = Bitcask::open(temp_dir.path())?;

        bitcask.set("key1".to_owned(), "value1".to_owned())?;
        assert_eq!(bitcask.get("key2".to_owned())?, None);

        // Open from disk again and check persistent data.
        drop(bitcask);
        let store = Bitcask::open(temp_dir.path())?;
        assert_eq!(store.get("key2".to_owned())?, None);

        Ok(())
    }

    #[test]
    fn remove_non_existent_key() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bitcask = Bitcask::open(temp_dir.path())?;
        assert!(bitcask.remove("key1".to_owned()).is_err());

        Ok(())
    }

    #[test]
    fn remove_key() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bitcask = Bitcask::open(temp_dir.path())?;
        bitcask.set("key1".to_owned(), "value1".to_owned())?;
        assert!(bitcask.remove("key1".to_owned()).is_ok());
        assert_eq!(bitcask.get("key1".to_owned())?, None);

        Ok(())
    }

    // Insert data and call `merge` to compact log files
    // Test dir size grows and shrinks before and after merging
    // Test data correctness after merging
    #[test]
    fn compaction() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bitcask = Bitcask::open(temp_dir.path()).unwrap();

        let dir_size = || {
            let entries = WalkDir::new(temp_dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let initial_size = dir_size();
        for iter in 0..=1000 {
            for key_id in 0..=1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                bitcask.set(key, value).unwrap();
            }
        }

        let new_size = dir_size();
        assert!(
            new_size > initial_size,
            "expected dir size to grow before merge"
        );

        bitcask.compact()?;

        let final_size = dir_size();
        assert!(
            final_size < new_size,
            "expected dir size to shrink after merge"
        );

        // test that store can read from the merged log
        drop(bitcask);

        let store = Bitcask::open(temp_dir.path())?;
        for key_id in 0..=1000 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key)?, Some(format!("{}", 1000)));
        }

        Ok(())
    }

    #[test]
    fn concurrent_set() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = Bitcask::open(temp_dir.path())?;
        let barrier = Arc::new(Barrier::new(1001));
        for i in 0..1000 {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                store
                    .set(format!("key{}", i), format!("value{}", i))
                    .unwrap();
                barrier.wait();
            });
        }
        barrier.wait();

        for i in 0..1000 {
            assert_eq!(store.get(format!("key{}", i))?, Some(format!("value{}", i)));
        }

        // Open from disk again and check persistent data
        drop(store);
        let store = Bitcask::open(temp_dir.path())?;
        for i in 0..1000 {
            assert_eq!(store.get(format!("key{}", i))?, Some(format!("value{}", i)));
        }

        Ok(())
    }

    #[test]
    fn concurrent_get() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = Bitcask::open(temp_dir.path())?;
        for i in 0..100 {
            store
                .set(format!("key{}", i), format!("value{}", i))
                .unwrap();
        }

        let mut handles = Vec::new();
        for thread_id in 0..100 {
            let store = store.clone();
            let handle = std::thread::spawn(move || {
                for i in 0..100 {
                    let key_id = (i + thread_id) % 100;
                    assert_eq!(
                        store.get(format!("key{}", key_id)).unwrap(),
                        Some(format!("value{}", key_id))
                    );
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Open from disk again and check persistent data
        drop(store);
        let store = Bitcask::open(temp_dir.path())?;
        let mut handles = Vec::new();
        for thread_id in 0..100 {
            let store = store.clone();
            let handle = std::thread::spawn(move || {
                for i in 0..100 {
                    let key_id = (i + thread_id) % 100;
                    assert_eq!(
                        store.get(format!("key{}", key_id)).unwrap(),
                        Some(format!("value{}", key_id))
                    );
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }

        Ok(())
    }
}
