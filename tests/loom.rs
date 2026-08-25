#![cfg(loom)]

use persistent_queue::{Builder, Durability, MemStore};

// A producer blocked on a full queue must be woken when the consumer frees a slot.
// loom explores every interleaving of the ack/notify against the producer's wait,
// which is where a lost wakeup would hide.
#[test]
fn capacity_handoff_has_no_lost_wakeup() {
    loom::model(|| {
        let (tx, rx) = Builder::new(MemStore::new()).capacity(1).open().unwrap();
        tx.push(b"a").unwrap(); // fills the single slot

        let producer = tx.clone();
        let blocked = loom::thread::spawn(move || {
            producer.push(b"b").unwrap(); // must block until "a" is acked
        });

        // Freeing the slot must wake the blocked producer.
        let a = rx.reserve().unwrap().expect("first item is present");
        assert_eq!(&*a, &b"a"[..]);
        a.ack().unwrap();

        blocked.join().unwrap();

        let b = rx.reserve().unwrap().expect("second item is present");
        assert_eq!(&*b, &b"b"[..]);
        b.ack().unwrap();
    });
}

// Two producers in Group mode: one flushes the batch, the other waits for its seq.
// loom explores leader election and the follower's wait so neither is lost.
#[test]
fn group_commit_flushes_every_producer() {
    loom::model(|| {
        let (tx, rx) = Builder::new(MemStore::new())
            .capacity(8)
            .durability(Durability::Group)
            .open()
            .unwrap();

        let p1 = {
            let tx = tx.clone();
            loom::thread::spawn(move || tx.push(b"a").unwrap())
        };
        let p2 = {
            let tx = tx.clone();
            loom::thread::spawn(move || tx.push(b"b").unwrap())
        };
        p1.join().unwrap();
        p2.join().unwrap();

        let mut seen = 0;
        while let Some(item) = rx.reserve().unwrap() {
            item.ack().unwrap();
            seen += 1;
        }
        assert_eq!(seen, 2);
    });
}

// Two producers under Sync commit in either order while the consumer runs alongside.
// Both items must be delivered: a head cursor that advanced in `reserve` would skip
// the seq that commits second (it lands below the advanced head). loom explores the
// interleavings; the spin guard turns a lost item into a failure instead of a hang.
#[test]
fn concurrent_producers_deliver_both_sync() {
    loom::model(|| {
        let (tx, rx) = Builder::new(MemStore::new()).capacity(4).open().unwrap();

        let p1 = {
            let tx = tx.clone();
            loom::thread::spawn(move || tx.push(b"a").unwrap())
        };
        let p2 = {
            let tx = tx.clone();
            loom::thread::spawn(move || tx.push(b"b").unwrap())
        };

        let mut seen = 0;
        let mut spins = 0;
        while seen < 2 {
            if let Some(item) = rx.reserve().unwrap() {
                item.ack().unwrap();
                seen += 1;
            } else {
                spins += 1;
                assert!(spins < 1000, "an item was lost: the consumer starved");
                loom::thread::yield_now();
            }
        }

        p1.join().unwrap();
        p2.join().unwrap();
    });
}
