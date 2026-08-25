use persistent_queue::{Builder, Consumer, Durability, MemStore, Producer};

#[test]
fn push_reserve_ack_roundtrip() {
    let (tx, rx) = mem_queue(8);
    tx.push(b"a").unwrap();
    tx.push(b"b").unwrap();
    assert_eq!(tx.len(), 2);

    let a = rx.reserve().unwrap().unwrap();
    assert_eq!(&*a, &b"a"[..]);
    a.ack().unwrap();
    assert_eq!(tx.len(), 1);

    let b = rx.reserve().unwrap().unwrap();
    assert_eq!(&*b, &b"b"[..]);
    b.ack().unwrap();

    assert!(rx.reserve().unwrap().is_none());
    assert!(tx.is_empty());
}

#[test]
fn backpressure_unblocks_on_ack() {
    use std::thread;
    use std::time::Duration;

    let (tx, rx) = mem_queue(1);
    tx.push(b"a").unwrap();

    let producer = tx.clone();
    let handle = thread::spawn(move || producer.push(b"b").unwrap());

    thread::sleep(Duration::from_millis(50));
    assert!(
        !handle.is_finished(),
        "push should wait while the queue is full"
    );

    rx.reserve().unwrap().unwrap().ack().unwrap();
    handle.join().unwrap();
    assert_eq!(tx.len(), 1);
}

#[cfg(feature = "sled")]
#[test]
fn sled_reopen_redelivers_unacked() {
    use persistent_queue::SledStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("q");

    {
        let (tx, _rx) = Builder::new(SledStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        tx.push(b"a").unwrap();
        tx.push(b"b").unwrap();
    }
    {
        let (_tx, rx) = Builder::new(SledStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        let a = rx.reserve().unwrap().unwrap();
        assert_eq!(&*a, &b"a"[..]);
        a.ack().unwrap();
    }
    {
        let (_tx, rx) = Builder::new(SledStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        let b = rx.reserve().unwrap().unwrap();
        assert_eq!(&*b, &b"b"[..]); // a was acked and durably removed
        b.ack().unwrap();
        assert!(rx.reserve().unwrap().is_none());
    }
}

#[cfg(feature = "redb")]
#[test]
fn redb_reopen_redelivers_unacked() {
    use persistent_queue::RedbStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("q.redb");

    {
        let (tx, _rx) = Builder::new(RedbStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        tx.push(b"a").unwrap();
        tx.push(b"b").unwrap();
    }
    {
        let (_tx, rx) = Builder::new(RedbStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        let a = rx.reserve().unwrap().unwrap();
        assert_eq!(&*a, &b"a"[..]);
        a.ack().unwrap();
    }
    {
        let (_tx, rx) = Builder::new(RedbStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        let b = rx.reserve().unwrap().unwrap();
        assert_eq!(&*b, &b"b"[..]);
        b.ack().unwrap();
        assert!(rx.reserve().unwrap().is_none());
    }
}

#[test]
fn group_commit_delivers_every_concurrent_push() {
    use std::collections::HashSet;
    use std::thread;

    let (tx, rx) = Builder::new(MemStore::new())
        .capacity(1024)
        .durability(Durability::Group)
        .open()
        .unwrap();

    let producers = 4;
    let per = 8;
    let mut handles = Vec::new();
    for p in 0..producers {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..per {
                tx.push(format!("{p}-{i}").as_bytes()).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let mut seen = HashSet::new();
    while let Some(item) = rx.reserve().unwrap() {
        assert!(seen.insert(item.to_vec()), "no duplicates");
        item.ack().unwrap();
    }
    assert_eq!(seen.len(), (producers * per) as usize);
}

#[cfg(feature = "rocksdb")]
#[test]
fn rocksdb_reopen_redelivers_unacked() {
    use persistent_queue::RocksStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("q");

    {
        let (tx, _rx) = Builder::new(RocksStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        tx.push(b"a").unwrap();
        tx.push(b"b").unwrap();
    }
    {
        let (_tx, rx) = Builder::new(RocksStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        let a = rx.reserve().unwrap().unwrap();
        assert_eq!(&*a, &b"a"[..]);
        a.ack().unwrap();
    }
    {
        let (_tx, rx) = Builder::new(RocksStore::open(&path).unwrap())
            .capacity(8)
            .open()
            .unwrap();
        let b = rx.reserve().unwrap().unwrap();
        assert_eq!(&*b, &b"b"[..]);
        b.ack().unwrap();
        assert!(rx.reserve().unwrap().is_none());
    }
}

fn mem_queue(capacity: usize) -> (Producer<MemStore>, Consumer<MemStore>) {
    Builder::new(MemStore::new())
        .capacity(capacity)
        .open()
        .unwrap()
}
