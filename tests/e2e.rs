//! End-to-end: the full stack - a codec or the async facade over a real on-disk
//! backend - survives a crash (drop + reopen the same path) and recovers. Each test
//! is gated on the features it exercises, so they run under `--all-features`.

#[cfg(all(feature = "sled", feature = "serde"))]
#[test]
fn typed_bincode_survives_a_crash_on_sled() {
    use persistent_queue::{Bincode, Builder, SledStore};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Job {
        id: u64,
        name: String,
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue");

    // Session 1: push 10 jobs, ack the even ids, hold (then drop) the odd ones.
    {
        let store = SledStore::open(&path).unwrap();
        let (tx, rx) = Builder::new(store).open_typed(Bincode).unwrap();
        for id in 0..10u64 {
            tx.push(&Job {
                id,
                name: format!("job-{id}"),
            })
            .unwrap();
        }
        let mut held = Vec::new();
        while let Some(item) = rx.reserve().unwrap() {
            if item.id % 2 == 0 {
                item.ack().unwrap();
            } else {
                held.push(item);
            }
        }
        // `held` (odd ids, unacked) drops here - a crash mid-processing.
    }

    // Session 2: reopen the same store; the odd jobs return, and still decode.
    let store = SledStore::open(&path).unwrap();
    let (_tx, rx) = Builder::new(store).open_typed::<Job, _>(Bincode).unwrap();
    let mut recovered = Vec::new();
    while let Some(item) = rx.reserve().unwrap() {
        assert_eq!(item.name, format!("job-{}", item.id));
        recovered.push(item.id);
        item.ack().unwrap();
    }
    recovered.sort_unstable();
    assert_eq!(recovered, vec![1u64, 3, 5, 7, 9]);
}

#[cfg(all(feature = "redb", feature = "rkyv"))]
#[test]
fn typed_rkyv_survives_a_crash_on_redb() {
    use persistent_queue::{Builder, RedbStore, Rkyv};

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Job {
        id: u64,
        name: String,
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.redb");

    {
        let store = RedbStore::open(&path).unwrap();
        let (tx, rx) = Builder::new(store).open_typed(Rkyv).unwrap();
        for id in 0..10u64 {
            tx.push(&Job {
                id,
                name: format!("job-{id}"),
            })
            .unwrap();
        }
        let mut held = Vec::new();
        while let Some(item) = rx.reserve().unwrap() {
            if item.id % 2 == 0 {
                item.ack().unwrap();
            } else {
                held.push(item);
            }
        }
    }

    let store = RedbStore::open(&path).unwrap();
    let (_tx, rx) = Builder::new(store).open_typed::<Job, _>(Rkyv).unwrap();
    let mut recovered = Vec::new();
    while let Some(item) = rx.reserve().unwrap() {
        assert_eq!(item.name, format!("job-{}", item.id));
        recovered.push(item.id);
        item.ack().unwrap();
    }
    recovered.sort_unstable();
    assert_eq!(recovered, vec![1u64, 3, 5, 7, 9]);
}

#[cfg(all(feature = "tokio", feature = "sled"))]
#[tokio::test]
async fn async_survives_a_crash_on_sled() {
    use persistent_queue::{Builder, SledStore};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue");

    // Session 1: push 6, ack the first 3, drop the rest (a crash).
    {
        let store = SledStore::open(&path).unwrap();
        let (tx, rx) = Builder::new(store).open_async().await.unwrap();
        for i in 0..6u64 {
            tx.push(i.to_le_bytes().to_vec()).await.unwrap();
        }
        for _ in 0..3 {
            rx.reserve().await.unwrap().unwrap().ack().await.unwrap();
        }
    }

    // Session 2: reopen; the 3 unacked items come back.
    let store = SledStore::open(&path).unwrap();
    let (tx, rx) = Builder::new(store).open_async().await.unwrap();
    tx.close(); // so reserve terminates once the recovered items drain
    let mut recovered = 0;
    while let Some(item) = rx.reserve().await.unwrap() {
        recovered += 1;
        item.ack().await.unwrap();
    }
    assert_eq!(recovered, 3, "the 3 unacked items survived the crash");
}
