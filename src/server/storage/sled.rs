use std::{path::PathBuf, sync::Arc};

use sled::{Db, Tree};

use super::{Storage, StorageError, StorageResult};

/// Wrapper of `sled::Db`
#[derive(Clone)]
pub struct Sled(Arc<Db>);

impl Sled {
    /// Creates a `Sled` storage engine using `sled::Db`.
    pub fn open(path: impl Into<PathBuf>) -> StorageResult<Self> {
        let db = ::sled::open(path.into())?;
        Ok(Sled(Arc::new(db)))
    }
}

impl Storage for Sled {
    fn compact(&self) -> StorageResult<()> {
        Ok(())
    }

    fn set(&self, key: String, value: String) -> StorageResult<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> StorageResult<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&self, key: String) -> StorageResult<()> {
        let tree: &Tree = &self.0;
        tree.remove(key)?.ok_or(StorageError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }

    fn list_keys(&self) -> Vec<String> {
        let tree: &Tree = &self.0;
        tree.iter()
            .keys()
            .filter_map(Result::ok)
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .filter_map(|i_vec| String::from_utf8(i_vec).ok())
            .collect()
    }
}
