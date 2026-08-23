//! The [`Store`] trait and its built-in backends.

use std::error::Error as StdError;

/// A single write in a [`Store::commit`] batch.
pub enum Op<'a> {
    /// Insert or overwrite `key` with `value`.
    Put(&'a [u8], &'a [u8]),
    /// Remove `key`.
    Delete(&'a [u8]),
}

/// A key/value pair returned by a store seek.
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// An ordered key/value byte store: the durable substrate under the queue.
///
/// Keys sort lexicographically. The queue needs forward and backward seeks and
/// one atomic, optionally durable, batch write; it never asks the store to know
/// anything about queues.
pub trait Store: Send + Sync {
    /// The backend's error type.
    type Error: StdError + Send + Sync + 'static;

    /// Value for an exact key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Smallest entry whose key is `>= from`.
    fn seek(&self, from: &[u8]) -> Result<Option<KeyValue>, Self::Error>;

    /// Greatest entry whose key is `<= upto`.
    fn seek_back(&self, upto: &[u8]) -> Result<Option<KeyValue>, Self::Error>;

    /// Apply `ops`. When `durable`, do not return until they survive a crash.
    fn commit(&self, ops: &[Op<'_>], durable: bool) -> Result<(), Self::Error>;
}

// ---- mem ----

/// In-memory [`Store`] backed by a `BTreeMap`. Not persistent; `durable` is a
/// no-op. Useful as the default, for tests, and as a benchmark baseline.
#[derive(Debug, Default)]
pub struct MemStore {
    map: std::sync::Mutex<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Store for MemStore {
    type Error = std::convert::Infallible;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    fn seek(&self, from: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .range(from.to_vec()..)
            .next()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn seek_back(&self, upto: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .range(..=upto.to_vec())
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn commit(&self, ops: &[Op<'_>], _durable: bool) -> Result<(), Self::Error> {
        let mut map = self.map.lock().unwrap();
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    map.insert(k.to_vec(), v.to_vec());
                }
                Op::Delete(k) => {
                    map.remove(*k);
                }
            }
        }
        Ok(())
    }
}

// ---- sled ----

/// [`Store`] backed by a [sled](https://docs.rs/sled) database. Requires the
/// `sled` feature.
///
/// Only one process may open a given database directory at a time; sled takes an
/// exclusive lock. A backend error on open, including a corrupt store, is surfaced
/// by [`Builder::open`](crate::Builder::open) as
/// [`OpenError::Store`](crate::OpenError::Store); the queue does not auto-repair.
#[cfg(feature = "sled")]
pub struct SledStore {
    db: sled::Db,
}

#[cfg(feature = "sled")]
impl SledStore {
    /// Open (creating if needed) a sled database at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> sled::Result<Self> {
        Ok(Self {
            db: sled::open(path)?,
        })
    }

    /// Wrap an already-open sled database.
    pub fn from_db(db: sled::Db) -> Self {
        Self { db }
    }
}

#[cfg(feature = "sled")]
impl Store for SledStore {
    type Error = sled::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.db.get(key)?.map(|v| v.to_vec()))
    }

    fn seek(&self, from: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        match self.db.range(from.to_vec()..).next() {
            Some(r) => {
                let (k, v) = r?;
                Ok(Some((k.to_vec(), v.to_vec())))
            }
            None => Ok(None),
        }
    }

    fn seek_back(&self, upto: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        match self.db.range(..=upto.to_vec()).next_back() {
            Some(r) => {
                let (k, v) = r?;
                Ok(Some((k.to_vec(), v.to_vec())))
            }
            None => Ok(None),
        }
    }

    fn commit(&self, ops: &[Op<'_>], durable: bool) -> Result<(), Self::Error> {
        let mut batch = sled::Batch::default();
        for op in ops {
            match op {
                Op::Put(k, v) => batch.insert(*k, *v),
                Op::Delete(k) => batch.remove(*k),
            }
        }
        self.db.apply_batch(batch)?;
        if durable {
            self.db.flush()?;
        }
        Ok(())
    }
}

// ---- redb ----

#[cfg(feature = "redb")]
const REDB_TABLE: redb::TableDefinition<'static, &[u8], &[u8]> =
    redb::TableDefinition::new("entries");

/// [`Store`] backed by a [redb](https://docs.rs/redb) database. Requires the
/// `redb` feature.
///
/// Only one process may open a given database file at a time; redb takes an
/// exclusive lock. A backend error on open, including a corrupt store, is surfaced
/// by [`Builder::open`](crate::Builder::open) as
/// [`OpenError::Store`](crate::OpenError::Store); the queue does not auto-repair.
#[cfg(feature = "redb")]
pub struct RedbStore {
    db: redb::Database,
}

#[cfg(feature = "redb")]
impl RedbStore {
    /// Open (creating if needed) a redb database at `path`.
    // The Store trait surfaces redb::Error unboxed, so keep open consistent.
    #[allow(clippy::result_large_err)]
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, redb::Error> {
        let db = redb::Database::create(path)?;
        let wtx = db.begin_write()?;
        wtx.open_table(REDB_TABLE)?;
        wtx.commit()?;
        Ok(Self { db })
    }

    /// Wrap an already-open redb database.
    pub fn from_db(db: redb::Database) -> Self {
        Self { db }
    }
}

#[cfg(feature = "redb")]
impl Store for RedbStore {
    type Error = redb::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(REDB_TABLE)?;
        Ok(table.get(key)?.map(|g| g.value().to_vec()))
    }

    fn seek(&self, from: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(REDB_TABLE)?;
        match table.range::<&[u8]>(from..)?.next() {
            Some(r) => {
                let (k, v) = r?;
                Ok(Some((k.value().to_vec(), v.value().to_vec())))
            }
            None => Ok(None),
        }
    }

    fn seek_back(&self, upto: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(REDB_TABLE)?;
        match table.range::<&[u8]>(..=upto)?.next_back() {
            Some(r) => {
                let (k, v) = r?;
                Ok(Some((k.value().to_vec(), v.value().to_vec())))
            }
            None => Ok(None),
        }
    }

    fn commit(&self, ops: &[Op<'_>], durable: bool) -> Result<(), Self::Error> {
        let mut wtx = self.db.begin_write()?;
        if !durable {
            wtx.set_durability(redb::Durability::None);
        }
        {
            let mut table = wtx.open_table(REDB_TABLE)?;
            for op in ops {
                match op {
                    Op::Put(k, v) => {
                        table.insert(*k, *v)?;
                    }
                    Op::Delete(k) => {
                        table.remove(*k)?;
                    }
                }
            }
        }
        wtx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_store_contract() {
        contract(MemStore::new());
    }

    #[cfg(feature = "sled")]
    #[test]
    fn sled_store_contract() {
        let dir = tempfile::tempdir().unwrap();
        contract(SledStore::open(dir.path().join("db")).unwrap());
    }

    #[cfg(feature = "redb")]
    #[test]
    fn redb_store_contract() {
        let dir = tempfile::tempdir().unwrap();
        contract(RedbStore::open(dir.path().join("db.redb")).unwrap());
    }

    // A store reads keys back exactly, keeps them in byte-lexicographic order, and
    // applies deletes; seeks find the nearest key in each direction.
    fn contract<S: Store>(store: S) {
        assert!(store.get(b"missing").unwrap().is_none());
        assert!(store.seek(b"a").unwrap().is_none());

        store
            .commit(
                &[
                    Op::Put(b"b", b"2"),
                    Op::Put(b"a", b"1"),
                    Op::Put(b"c", b"3"),
                ],
                true,
            )
            .unwrap();

        assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(store.get(b"z").unwrap(), None);

        let (k, v) = store.seek(b"a").unwrap().unwrap();
        assert_eq!((k.as_slice(), v.as_slice()), (&b"a"[..], &b"1"[..]));
        assert_eq!(store.seek(b"aa").unwrap().unwrap().0.as_slice(), b"b");
        assert_eq!(store.seek_back(b"bz").unwrap().unwrap().0.as_slice(), b"b");
        assert_eq!(
            store.seek_back(b"\xff").unwrap().unwrap().0.as_slice(),
            b"c"
        );

        store.commit(&[Op::Delete(b"b")], true).unwrap();
        assert_eq!(store.get(b"b").unwrap(), None);
        assert_eq!(store.seek(b"b").unwrap().unwrap().0.as_slice(), b"c");
    }
}
