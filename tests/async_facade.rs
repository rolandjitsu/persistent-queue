#![cfg(feature = "tokio")]

use persistent_queue::{Builder, MemStore};

#[tokio::test]
async fn async_push_reserve_ack_roundtrip() {
    let (tx, rx) = Builder::new(MemStore::new()).open_async().await.unwrap();
    tx.push(b"job".to_vec()).await.unwrap();

    let item = rx.reserve().await.unwrap().unwrap();
    assert_eq!(&*item, b"job");
    assert_eq!(item.seq(), 0);
    item.ack().await.unwrap();
}

#[tokio::test]
async fn async_nack_redelivers() {
    let (tx, rx) = Builder::new(MemStore::new()).open_async().await.unwrap();
    tx.push(b"x".to_vec()).await.unwrap();

    rx.reserve().await.unwrap().unwrap().nack();
    let again = rx.reserve().await.unwrap().unwrap();
    assert_eq!(&*again, b"x");
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_push_blocks_until_capacity_frees() {
    let (tx, rx) = Builder::new(MemStore::new())
        .capacity(1)
        .open_async()
        .await
        .unwrap();
    tx.push(b"a".to_vec()).await.unwrap();

    // The second push blocks (queue is full); run it in a task.
    let tx2 = tx.clone();
    let pending = tokio::spawn(async move { tx2.push(b"b".to_vec()).await });

    // Free a slot; the blocked push then completes.
    let a = rx.reserve().await.unwrap().unwrap();
    assert_eq!(&*a, b"a");
    a.ack().await.unwrap();

    pending.await.unwrap().unwrap();
    let b = rx.reserve().await.unwrap().unwrap();
    assert_eq!(&*b, b"b");
    b.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_reserve_waits_for_an_item() {
    let (tx, rx) = Builder::new(MemStore::new()).open_async().await.unwrap();

    // The consumer blocks on an empty queue instead of returning None.
    let consumer = tokio::spawn(async move {
        let item = rx.reserve().await.unwrap().unwrap();
        let seq = item.seq();
        item.ack().await.unwrap();
        seq
    });

    tokio::task::yield_now().await;
    tx.push(b"late".to_vec()).await.unwrap();

    assert_eq!(consumer.await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_reserve_unblocks_on_close() {
    let (tx, rx) = Builder::new(MemStore::new()).open_async().await.unwrap();

    // Blocked on an empty queue; close must wake it to a terminal None.
    let consumer = tokio::spawn(async move { rx.reserve().await.unwrap().is_none() });

    tokio::task::yield_now().await;
    tx.close();

    assert!(
        consumer.await.unwrap(),
        "reserve returns None once closed and drained"
    );
}

#[tokio::test]
async fn async_reserve_returns_none_after_close_and_drain() {
    let (tx, rx) = Builder::new(MemStore::new()).open_async().await.unwrap();
    tx.push(b"x".to_vec()).await.unwrap();
    tx.close();

    let item = rx.reserve().await.unwrap().unwrap();
    item.ack().await.unwrap();
    assert!(rx.reserve().await.unwrap().is_none());
}
