use persistent_queue::{Builder, Consumer, MemStore, Producer};

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

fn mem_queue(capacity: usize) -> (Producer<MemStore>, Consumer<MemStore>) {
    Builder::new(MemStore::new())
        .capacity(capacity)
        .open()
        .unwrap()
}
