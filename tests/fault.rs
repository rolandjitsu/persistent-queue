//! Crash-recovery and commit-failure behavior, exercised with a shared,
//! fault-injecting in-memory store.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use persistent_queue::{Builder, Op, PushError, Store};

#[test]
fn crash_and_reopen_loses_no_item() {
    let store = SharedMem::default();
    let pushed: u64 = 20;

    {
        let (tx, rx) = Builder::new(store.clone()).capacity(64).open().unwrap();
        for i in 0..pushed {
            tx.push(format!("item-{i}").as_bytes()).unwrap();
        }

        // Ack the even seqs; hold the odds reserved, then drop them (a crash mid-
        // processing) so they must be redelivered after reopen.
        let mut held = Vec::new();
        while let Some(item) = rx.reserve().unwrap() {
            if item.seq() % 2 == 0 {
                item.ack().unwrap();
            } else {
                held.push(item);
            }
        }
        // `held` (with tx and rx) drops here: a crash with the odd seqs unacked.
    }

    let (_tx, rx) = Builder::new(store.clone()).capacity(64).open().unwrap();
    let mut recovered = Vec::new();
    while let Some(item) = rx.reserve().unwrap() {
        recovered.push(item.seq());
        item.ack().unwrap();
    }
    recovered.sort_unstable();

    let expected: Vec<u64> = (0..pushed).filter(|s| s % 2 == 1).collect();
    assert_eq!(
        recovered, expected,
        "unacked items survive, acked ones do not"
    );
}

#[test]
fn commit_failure_is_reported_and_state_stays_consistent() {
    let store = SharedMem::default();
    let (tx, rx) = Builder::new(store.clone()).capacity(8).open().unwrap();

    tx.push(b"a").unwrap();

    // Make the next commit fail, so this push cannot land.
    store.fail_commit_number(store.commits.load(Ordering::SeqCst));
    assert!(matches!(tx.push(b"b"), Err(PushError::Store(_))));
    assert_eq!(tx.len(), 1, "a failed push must not consume capacity");

    // Stop failing; the queue keeps working and "b" never landed.
    store.stop_failing();
    tx.push(b"c").unwrap();
    assert_eq!(tx.len(), 2);

    let first = rx.reserve().unwrap().unwrap();
    assert_eq!(&*first, &b"a"[..]);
    first.ack().unwrap();
    let second = rx.reserve().unwrap().unwrap();
    assert_eq!(&*second, &b"c"[..]);
    second.ack().unwrap();
    assert!(rx.reserve().unwrap().is_none());
}

// A shared in-memory store: data lives behind an Arc, so a queue can be dropped
// ("crash") and a new one opened over the same data ("reopen"). It can also fail
// commits after a set count, to exercise the error paths.
#[derive(Clone, Default)]
struct SharedMem {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    commits: Arc<AtomicUsize>,
    fail_at: Arc<Mutex<Option<usize>>>,
}

impl SharedMem {
    fn fail_commit_number(&self, n: usize) {
        *self.fail_at.lock().unwrap() = Some(n);
    }

    fn stop_failing(&self) {
        *self.fail_at.lock().unwrap() = None;
    }
}

impl Store for SharedMem {
    type Error = FaultError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FaultError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    fn seek(&self, from: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, FaultError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .range(from.to_vec()..)
            .next()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn seek_back(&self, upto: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, FaultError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .range(..=upto.to_vec())
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn commit(&self, ops: &[Op<'_>], _durable: bool) -> Result<(), FaultError> {
        let n = self.commits.fetch_add(1, Ordering::SeqCst);
        if matches!(*self.fail_at.lock().unwrap(), Some(fail) if n >= fail) {
            return Err(FaultError);
        }
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

#[derive(Debug)]
struct FaultError;

impl fmt::Display for FaultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "injected store failure")
    }
}

impl Error for FaultError {}
