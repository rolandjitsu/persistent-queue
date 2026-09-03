//! Overhead of the async facade vs the sync core: one push + reserve + ack per
//! iteration. On `MemStore` the gap is the `spawn_blocking` hop the facade adds per
//! op; on a disk backend it is dwarfed by the fsync. Reproduce with
//! `cargo bench --features tokio,sled --bench async_overhead`.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use persistent_queue::{Builder, MemStore};
use tokio::runtime::Runtime;

const MSG: &[u8] = &[0u8; 256];

// In-memory: the store op is ~free, so the async/sync gap is the facade's overhead
// (the spawn_blocking hop and the Notify bookkeeping) laid bare.
fn mem(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("push_ack/mem");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    let (tx, rx) = Builder::new(MemStore::new()).open().unwrap();
    group.bench_function("sync", |b| {
        b.iter(|| {
            tx.push(MSG).unwrap();
            let item = rx.reserve().unwrap().unwrap();
            black_box(item.len());
            item.ack().unwrap();
        });
    });
    drop((tx, rx));

    let (tx, rx) = rt
        .block_on(async { Builder::new(MemStore::new()).open_async().await })
        .unwrap();
    group.bench_function("async", |b| {
        b.to_async(&rt).iter(|| async {
            tx.push(MSG.to_vec()).await.unwrap();
            let item = rx.reserve().await.unwrap().unwrap();
            black_box(item.len());
            item.ack().await.unwrap();
        });
    });
    drop((tx, rx));

    group.finish();
}

// On disk: the fsync dominates, so the facade's hop should vanish into the noise.
#[cfg(feature = "sled")]
fn sled(c: &mut Criterion) {
    use persistent_queue::SledStore;

    let rt = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("push_ack/sled");
    group.throughput(Throughput::Elements(1));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    let (tx, rx) = Builder::new(SledStore::open(dir.path().join("sync")).unwrap())
        .open()
        .unwrap();
    group.bench_function("sync", |b| {
        b.iter(|| {
            tx.push(MSG).unwrap();
            let item = rx.reserve().unwrap().unwrap();
            black_box(item.len());
            item.ack().unwrap();
        });
    });
    drop((tx, rx));

    let (tx, rx) = rt
        .block_on(async {
            Builder::new(SledStore::open(dir.path().join("async")).unwrap())
                .open_async()
                .await
        })
        .unwrap();
    group.bench_function("async", |b| {
        b.to_async(&rt).iter(|| async {
            tx.push(MSG.to_vec()).await.unwrap();
            let item = rx.reserve().await.unwrap().unwrap();
            black_box(item.len());
            item.ack().await.unwrap();
        });
    });
    drop((tx, rx));

    group.finish();
}

#[cfg(feature = "sled")]
criterion_group!(benches, mem, sled);
#[cfg(not(feature = "sled"))]
criterion_group!(benches, mem);
criterion_main!(benches);
