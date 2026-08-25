//! The queue: [`Builder`], [`Producer`], [`Consumer`], and the [`Reserved`] guard.

use std::collections::BTreeSet;
use std::ops::Deref;

use crate::error::{OpenError, PushError, TryPushError};
use crate::store::{Op, Store};
use crate::sync::{Arc, Condvar, Mutex};

// Keys: meta at 0x00; entries at 0x01 ++ seq as a big-endian u64, so the store's
// byte-lexicographic order matches seq order. A little-endian or text encoding
// would not (256 would sort before 255).
const META_KEY: [u8; 1] = [0x00];
const ENTRY_PREFIX: u8 = 0x01;
const ENTRY_LOW: [u8; 1] = [ENTRY_PREFIX];
const ENTRY_HIGH: [u8; 9] = [ENTRY_PREFIX, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
const FORMAT_VERSION: u8 = 1;

/// How writes are made durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// fsync every push and ack. Strongest, slowest.
    #[default]
    Sync,
    /// Batch concurrent pushes behind a single fsync; acks still fsync each. Same
    /// durability as `Sync` with far less fsync overhead under load.
    Group,
    /// Do not fsync. Fastest, but no durability guarantee: recent items can be lost
    /// on a crash (the backend persists on its own schedule, if at all).
    None,
}

impl Durability {
    fn durable(self) -> bool {
        !matches!(self, Durability::None)
    }

    fn group(self) -> bool {
        matches!(self, Durability::Group)
    }
}

struct Inner {
    tail: u64,
    // Lowest un-acked seq: where `reserve` starts scanning, so it skips the acked
    // prefix (and, on an LSM backend, the tombstones acks leave). Advanced only on
    // ack, never in reserve - a producer claims a seq under the lock but commits it
    // after, so a lower seq can still appear; advancing in reserve could skip it.
    head: u64,
    // Seqs acked out of order, above `head`. On acking `head`, it jumps the whole
    // contiguous acked run at once and stays just below the live entries, so the scan
    // does not wade through tombstones even when producers commit out of order.
    acked_above: BTreeSet<u64>,
    len: usize,
    reserved: BTreeSet<u64>,
    closed: bool,
}

struct Shared<S> {
    store: S,
    capacity: usize,
    durable: bool,
    group: bool,
    inner: Mutex<Inner>,
    room: Condvar,
    group_state: Mutex<GroupState>,
    group_ready: Condvar,
}

#[derive(Default)]
struct GroupState {
    pending: Vec<(u64, Vec<u8>)>,
    flushing: bool,
    done: std::collections::BTreeMap<u64, bool>,
}

impl<S: Store> Shared<S> {
    // Batch this entry with other concurrent pushes and make the batch durable with
    // one fsync. The first caller in flushes the whole pending batch; the rest wait
    // for their seq to be recorded.
    fn group_commit(&self, seq: u64, value: &[u8]) -> Result<(), S::Error> {
        let mut group = self.group_state.lock().unwrap();
        group.pending.push((seq, value.to_vec()));

        if group.flushing {
            while !group.done.contains_key(&seq) {
                group = self.group_ready.wait(group).unwrap();
            }
        } else {
            group.flushing = true;
            loop {
                let batch = std::mem::take(&mut group.pending);
                if batch.is_empty() {
                    group.flushing = false;
                    break;
                }
                drop(group);

                let keys: Vec<[u8; 9]> = batch.iter().map(|(s, _)| entry_key(*s)).collect();
                let ops: Vec<Op<'_>> = batch
                    .iter()
                    .zip(&keys)
                    .map(|((_, value), key)| Op::Put(key, value))
                    .collect();
                let ok = self.store.commit(&ops, true).is_ok();

                group = self.group_state.lock().unwrap();
                for (flushed, _) in &batch {
                    group.done.insert(*flushed, ok);
                }
                self.group_ready.notify_all();
                if group.pending.is_empty() {
                    group.flushing = false;
                    break;
                }
            }
        }

        let outcome = group.done.remove(&seq);
        drop(group);
        match outcome {
            Some(true) => Ok(()),
            // The batch fsync failed; retry just this entry so the caller gets its
            // own typed error (and the entry lands if the retry succeeds).
            _ => self.store.commit(&[Op::Put(&entry_key(seq), value)], true),
        }
    }
}

/// The producer and consumer ends returned by [`Builder::open`].
pub type Ends<S> = (Producer<S>, Consumer<S>);

/// Builds a queue over a [`Store`].
pub struct Builder<S> {
    store: S,
    capacity: usize,
    durability: Durability,
}

impl<S: Store> Builder<S> {
    /// Start a builder over `store` (capacity 1024, [`Durability::Sync`]).
    pub fn new(store: S) -> Self {
        Self {
            store,
            capacity: 1024,
            durability: Durability::Sync,
        }
    }

    /// Set the maximum number of unacked items before `push` blocks. Must be > 0.
    pub fn capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        self.capacity = capacity;
        self
    }

    /// Set the durability policy.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Open the queue, recovering any items already in the store.
    pub fn open(self) -> Result<Ends<S>, OpenError<S::Error>> {
        let durable = self.durability.durable();
        let group = self.durability.group();

        match self.store.get(&META_KEY).map_err(OpenError::Store)? {
            Some(meta) => {
                let version = meta.first().copied().unwrap_or_default();
                if version != FORMAT_VERSION {
                    return Err(OpenError::UnsupportedVersion(version));
                }
            }
            None => self
                .store
                .commit(&[Op::Put(&META_KEY, &[FORMAT_VERSION])], durable)
                .map_err(OpenError::Store)?,
        }

        let tail = match self
            .store
            .seek_back(&ENTRY_HIGH)
            .map_err(OpenError::Store)?
        {
            Some((key, _)) if is_entry(&key) => seq_of(&key) + 1,
            _ => 0,
        };

        let mut len = 0usize;
        let mut head = tail;
        let mut cursor = ENTRY_LOW.to_vec();
        while let Some((key, _)) = self.store.seek(&cursor).map_err(OpenError::Store)? {
            if !is_entry(&key) {
                break;
            }
            if len == 0 {
                head = seq_of(&key);
            }
            len += 1;
            cursor = entry_key(seq_of(&key) + 1).to_vec();
        }

        let shared = Arc::new(Shared {
            store: self.store,
            capacity: self.capacity,
            durable,
            group,
            inner: Mutex::new(Inner {
                tail,
                head,
                acked_above: BTreeSet::new(),
                len,
                reserved: BTreeSet::new(),
                closed: false,
            }),
            room: Condvar::new(),
            group_state: Mutex::new(GroupState::default()),
            group_ready: Condvar::new(),
        });
        Ok((
            Producer {
                shared: Arc::clone(&shared),
            },
            Consumer { shared },
        ))
    }
}

/// The producer half. Clone it for multiple producers.
pub struct Producer<S> {
    shared: Arc<Shared<S>>,
}

impl<S> Clone for Producer<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<S: Store> Producer<S> {
    /// Append `value`, waiting while the queue is at capacity.
    pub fn push(&self, value: &[u8]) -> Result<(), PushError<S::Error>> {
        let seq = {
            let mut inner = self.shared.inner.lock().unwrap();
            loop {
                if inner.closed {
                    return Err(PushError::Closed);
                }
                if inner.len < self.shared.capacity {
                    break;
                }
                inner = self.shared.room.wait(inner).unwrap();
            }
            let seq = inner.tail;
            inner.tail += 1;
            inner.len += 1;
            seq
        };
        self.write(seq, value).map_err(PushError::Store)
    }

    /// Append `value`, or return [`TryPushError::Full`] instead of waiting.
    pub fn try_push(&self, value: &[u8]) -> Result<(), TryPushError<S::Error>> {
        let seq = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.closed {
                return Err(TryPushError::Closed);
            }
            if inner.len >= self.shared.capacity {
                return Err(TryPushError::Full);
            }
            let seq = inner.tail;
            inner.tail += 1;
            inner.len += 1;
            seq
        };
        self.write(seq, value).map_err(TryPushError::Store)
    }

    /// Close the queue. Further pushes fail; the consumer can still drain.
    pub fn close(&self) {
        {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.closed = true;
        }
        self.shared.room.notify_all();
    }

    /// Number of unacked items currently in the queue.
    pub fn len(&self) -> usize {
        self.shared.inner.lock().unwrap().len
    }

    /// Whether the queue holds no unacked items.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Commit the entry outside the capacity lock; on failure, give back the slot.
    fn write(&self, seq: u64, value: &[u8]) -> Result<(), S::Error> {
        let result = if self.shared.group {
            self.shared.group_commit(seq, value)
        } else {
            self.shared
                .store
                .commit(&[Op::Put(&entry_key(seq), value)], self.shared.durable)
        };
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                {
                    let mut inner = self.shared.inner.lock().unwrap();
                    inner.len -= 1;
                }
                self.shared.room.notify_one();
                Err(e)
            }
        }
    }
}

/// The consumer half. Single consumer, so it does not implement `Clone`.
pub struct Consumer<S> {
    shared: Arc<Shared<S>>,
}

impl<S: Store> Consumer<S> {
    /// Reserve the oldest unreserved item, or `None` if there is nothing to
    /// deliver. Ack or nack the returned [`Reserved`] to finish with it.
    pub fn reserve(&self) -> Result<Option<Reserved<S>>, S::Error> {
        let mut cursor = entry_key(self.shared.inner.lock().unwrap().head).to_vec();
        loop {
            match self.shared.store.seek(&cursor)? {
                Some((key, value)) if is_entry(&key) => {
                    let seq = seq_of(&key);
                    let mut inner = self.shared.inner.lock().unwrap();
                    if inner.reserved.contains(&seq) {
                        drop(inner);
                        cursor = entry_key(seq + 1).to_vec();
                        continue;
                    }
                    inner.reserved.insert(seq);
                    drop(inner);
                    return Ok(Some(Reserved {
                        shared: Arc::clone(&self.shared),
                        seq,
                        value,
                        done: false,
                    }));
                }
                _ => return Ok(None),
            }
        }
    }
}

/// A reserved (in-flight) item. Derefs to its bytes; [`ack`](Reserved::ack)
/// removes it, [`nack`](Reserved::nack) or drop returns it for redelivery.
pub struct Reserved<S: Store> {
    shared: Arc<Shared<S>>,
    seq: u64,
    value: Vec<u8>,
    done: bool,
}

impl<S: Store> Reserved<S> {
    /// The item's sequence number: a stable id that survives redelivery.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Remove the item from the queue, committed per the durability policy.
    pub fn ack(mut self) -> Result<(), S::Error> {
        let key = entry_key(self.seq);
        self.shared
            .store
            .commit(&[Op::Delete(&key)], self.shared.durable)?;
        {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.reserved.remove(&self.seq);
            inner.len -= 1;
            // Advance head only when the oldest un-acked entry is the one acked (a
            // lower seq still being committed by a slow producer must never be
            // skipped), then jump the contiguous run of out-of-order acks above it.
            if self.seq == inner.head {
                let mut next = inner.head + 1;
                while inner.acked_above.remove(&next) {
                    next += 1;
                }
                inner.head = next;
            } else {
                inner.acked_above.insert(self.seq);
            }
        }
        self.shared.room.notify_one();
        self.done = true;
        Ok(())
    }

    /// Return the item for redelivery without removing it.
    pub fn nack(mut self) {
        self.release();
        self.done = true;
    }

    fn release(&self) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.reserved.remove(&self.seq);
    }
}

impl<S: Store> Deref for Reserved<S> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<S: Store> Drop for Reserved<S> {
    fn drop(&mut self) {
        if !self.done {
            self.release();
        }
    }
}

fn entry_key(seq: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = ENTRY_PREFIX;
    key[1..].copy_from_slice(&seq.to_be_bytes());
    key
}

fn seq_of(key: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[1..9]);
    u64::from_be_bytes(bytes)
}

fn is_entry(key: &[u8]) -> bool {
    key.len() == 9 && key[0] == ENTRY_PREFIX
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    #[test]
    fn key_roundtrip() {
        for seq in [0u64, 1, 255, 256, u32::MAX as u64, u64::MAX] {
            let key = entry_key(seq);
            assert!(is_entry(&key));
            assert_eq!(seq_of(&key), seq);
        }
    }

    #[test]
    fn keys_sort_by_seq_after_meta() {
        assert!(META_KEY[..] < ENTRY_LOW[..]);
        assert!(ENTRY_LOW[..] < entry_key(0)[..]);
        assert!(entry_key(1) < entry_key(2));
        assert!(entry_key(255) < entry_key(256));
        assert!(entry_key(u64::MAX)[..] <= ENTRY_HIGH[..]);
    }

    // Big-endian keys must sort in numeric seq order across byte boundaries, where a
    // little-endian or text encoding would not.
    #[test]
    fn store_orders_keys_by_numeric_seq() {
        let store = MemStore::new();
        for &seq in &[300u64, 1, 256, 255, 2, 65_536, 65_535] {
            store
                .commit(&[Op::Put(&entry_key(seq), b"x")], false)
                .unwrap();
        }
        assert_eq!(
            collect_seqs(&store),
            vec![1, 2, 255, 256, 300, 65_535, 65_536]
        );
    }

    #[test]
    fn open_recovers_tail_len_and_skips_gaps() {
        let store = MemStore::new();
        store
            .commit(
                &[
                    Op::Put(&entry_key(5), b"five"),
                    Op::Put(&entry_key(7), b"seven"),
                ],
                false,
            )
            .unwrap();

        let (tx, rx) = Builder::new(store).capacity(8).open().unwrap();
        assert_eq!(tx.len(), 2);

        tx.push(b"eight").unwrap(); // tail recovered as 8
        let a = rx.reserve().unwrap().unwrap();
        assert_eq!((a.seq(), &*a), (5, &b"five"[..]));
        a.ack().unwrap();
        let b = rx.reserve().unwrap().unwrap();
        assert_eq!(b.seq(), 7); // gap at 6 is skipped
        b.ack().unwrap();
        assert_eq!(rx.reserve().unwrap().unwrap().seq(), 8);
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let store = MemStore::new();
        store.commit(&[Op::Put(&META_KEY, &[2])], false).unwrap();
        match Builder::new(store).open() {
            Err(OpenError::UnsupportedVersion(v)) => assert_eq!(v, 2),
            _ => panic!("expected UnsupportedVersion"),
        }
    }

    #[test]
    fn try_push_is_full_at_capacity() {
        let (tx, rx) = mem(1);
        tx.push(b"a").unwrap();
        assert!(matches!(tx.try_push(b"b"), Err(TryPushError::Full)));
        rx.reserve().unwrap().unwrap().ack().unwrap();
        tx.try_push(b"b").unwrap();
    }

    #[test]
    fn close_rejects_further_push() {
        let (tx, _rx) = mem(4);
        tx.close();
        assert!(matches!(tx.push(b"a"), Err(PushError::Closed)));
        assert!(matches!(tx.try_push(b"a"), Err(TryPushError::Closed)));
    }

    #[test]
    fn nack_returns_item_for_redelivery() {
        let (tx, rx) = mem(4);
        tx.push(b"a").unwrap();
        rx.reserve().unwrap().unwrap().nack();
        assert_eq!(rx.reserve().unwrap().unwrap().seq(), 0);
    }

    #[test]
    fn drop_returns_item_for_redelivery() {
        let (tx, rx) = mem(4);
        tx.push(b"a").unwrap();
        drop(rx.reserve().unwrap().unwrap());
        assert_eq!(rx.reserve().unwrap().unwrap().seq(), 0);
    }

    #[test]
    fn reserve_is_none_when_empty_or_all_reserved() {
        let (tx, rx) = mem(4);
        assert!(rx.reserve().unwrap().is_none());
        tx.push(b"a").unwrap();
        let _held = rx.reserve().unwrap().unwrap();
        assert!(rx.reserve().unwrap().is_none());
    }

    #[test]
    fn group_durability_delivers_in_order() {
        let (tx, rx) = Builder::new(MemStore::new())
            .capacity(8)
            .durability(Durability::Group)
            .open()
            .unwrap();
        for i in 0..4u8 {
            tx.push(&[i]).unwrap();
        }
        for i in 0..4u8 {
            let item = rx.reserve().unwrap().unwrap();
            assert_eq!(&*item, &[i][..]);
            item.ack().unwrap();
        }
        assert!(rx.reserve().unwrap().is_none());
    }

    fn mem(capacity: usize) -> (Producer<MemStore>, Consumer<MemStore>) {
        Builder::new(MemStore::new())
            .capacity(capacity)
            .open()
            .unwrap()
    }

    fn collect_seqs(store: &MemStore) -> Vec<u64> {
        let mut seqs = Vec::new();
        let mut cursor = ENTRY_LOW.to_vec();
        while let Some((key, _)) = store.seek(&cursor).unwrap() {
            if !is_entry(&key) {
                break;
            }
            seqs.push(seq_of(&key));
            cursor = entry_key(seq_of(&key) + 1).to_vec();
        }
        seqs
    }
}
