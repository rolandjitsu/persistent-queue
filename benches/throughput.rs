//! Throughput of push + reserve + ack across backends, durability policies, and
//! producer counts. Reproduce with `cargo bench --all-features`.

use std::hint::black_box;
use std::thread;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use persistent_queue::{Builder, Consumer, Durability, MemStore, Producer, Store};

#[cfg(feature = "redb")]
use persistent_queue::RedbStore;
#[cfg(feature = "rocksdb")]
use persistent_queue::RocksStore;
#[cfg(feature = "sled")]
use persistent_queue::SledStore;

const MSG: usize = 256;

// `producers` threads push `total` messages of `msg` bytes; this thread drains all.
fn run<S: Store + 'static>(
    tx: &Producer<S>,
    rx: &Consumer<S>,
    producers: usize,
    total: usize,
    msg: usize,
) {
    let per = total / producers;
    let payload = vec![0u8; msg];
    let mut handles = Vec::with_capacity(producers);
    for _ in 0..producers {
        let tx = tx.clone();
        let payload = payload.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..per {
                tx.push(&payload).unwrap();
            }
        }));
    }
    let mut got = 0;
    while got < per * producers {
        if let Some(item) = rx.reserve().unwrap() {
            black_box(item.len());
            item.ack().unwrap();
            got += 1;
        } else {
            thread::yield_now();
        }
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

macro_rules! bench {
    ($group:expr, $name:expr, $store:expr, $dur:expr, $producers:expr, $total:expr) => {{
        let (tx, rx) = Builder::new($store)
            .capacity(1024)
            .durability($dur)
            .open()
            .unwrap();
        $group.bench_function($name, |b| {
            b.iter(|| run(&tx, &rx, $producers, $total, MSG));
        });
    }};
}

// In-memory (no fsync): the queue's own coordination cost, and whether group-commit
// adds overhead when there is no fsync to amortize.
fn mem(c: &mut Criterion) {
    const TOTAL: usize = 10_000;
    let mut group = c.benchmark_group("mem");
    group.throughput(Throughput::Elements(TOTAL as u64));
    for &producers in &[1usize, 4, 16, 64] {
        bench!(
            group,
            format!("sync/p{producers}"),
            MemStore::new(),
            Durability::Sync,
            producers,
            TOTAL
        );
        bench!(
            group,
            format!("group/p{producers}"),
            MemStore::new(),
            Durability::Group,
            producers,
            TOTAL
        );
    }
    group.finish();
}

// On-disk: the cost of durability, how group-commit amortizes fsync under producers,
// and how sled and redb compare.
#[cfg(all(feature = "sled", feature = "redb", feature = "rocksdb"))]
fn disk(c: &mut Criterion) {
    const TOTAL: usize = 200;
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("disk");
    group.throughput(Throughput::Elements(TOTAL as u64));
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_secs(3));
    for &producers in &[1usize, 4, 16, 64] {
        for (label, dur) in [
            ("sync", Durability::Sync),
            ("group", Durability::Group),
            ("none", Durability::None),
        ] {
            let sled =
                SledStore::open(dir.path().join(format!("sled-{label}-{producers}"))).unwrap();
            bench!(
                group,
                format!("sled/{label}/p{producers}"),
                sled,
                dur,
                producers,
                TOTAL
            );
            let redb =
                RedbStore::open(dir.path().join(format!("redb-{label}-{producers}.redb"))).unwrap();
            bench!(
                group,
                format!("redb/{label}/p{producers}"),
                redb,
                dur,
                producers,
                TOTAL
            );
            let rocks =
                RocksStore::open(dir.path().join(format!("rocks-{label}-{producers}"))).unwrap();
            bench!(
                group,
                format!("rocks/{label}/p{producers}"),
                rocks,
                dur,
                producers,
                TOTAL
            );
        }
    }
    group.finish();
}

#[cfg(all(feature = "sled", feature = "redb", feature = "rocksdb"))]
criterion_group!(benches, mem, disk);
#[cfg(not(all(feature = "sled", feature = "redb", feature = "rocksdb")))]
criterion_group!(benches, mem);
criterion_main!(benches);
