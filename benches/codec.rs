//! Codec cost: serde/bincode vs rkyv, for one queue-message-shaped record. Encode,
//! full owned decode, and reading a single field - the last is rkyv's zero-copy win
//! (the `open_archived` read path: align + validate + read in place, no decode).
//! Run with `cargo bench --features serde,rkyv --bench codec`.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use persistent_queue::{Bincode, Codec, Rkyv};
use rkyv::rancor::Error as RkyvError;

#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, serde::Serialize, serde::Deserialize,
)]
struct Event {
    id: u64,
    kind: u32,
    timestamp: u64,
    label: String,
    payload: Vec<u8>,
}

fn sample() -> Event {
    Event {
        id: 42,
        kind: 7,
        timestamp: 1_700_000_000,
        label: "order.created".to_owned(),
        payload: vec![0u8; 1024],
    }
}

fn codec(c: &mut Criterion) {
    let event = sample();
    let bin = Bincode.encode(&event).unwrap();
    let rk = Rkyv.encode(&event).unwrap();

    let mut enc = c.benchmark_group("encode");
    enc.bench_function("bincode", |b| {
        b.iter(|| black_box(Bincode.encode(black_box(&event)).unwrap()));
    });
    enc.bench_function("rkyv", |b| {
        b.iter(|| black_box(Rkyv.encode(black_box(&event)).unwrap()));
    });
    enc.finish();

    let mut dec = c.benchmark_group("decode_owned");
    dec.bench_function("bincode", |b| {
        b.iter(|| {
            let event: Event = Bincode.decode(black_box(&bin)).unwrap();
            black_box(event)
        });
    });
    dec.bench_function("rkyv", |b| {
        b.iter(|| {
            let event: Event = Rkyv.decode(black_box(&rk)).unwrap();
            black_box(event)
        });
    });
    dec.finish();

    let mut read = c.benchmark_group("read_one_field");
    read.bench_function("bincode", |b| {
        b.iter(|| {
            let event: Event = Bincode.decode(black_box(&bin)).unwrap();
            black_box(event.id)
        });
    });
    read.bench_function("rkyv", |b| {
        // Mirrors ArchivedConsumer::reserve + get: align, validate, read in place.
        b.iter(|| {
            let mut aligned = rkyv::util::AlignedVec::<16>::new();
            aligned.extend_from_slice(black_box(&rk));
            let view = rkyv::access::<ArchivedEvent, RkyvError>(&aligned).unwrap();
            black_box(view.id.to_native())
        });
    });
    read.finish();
}

criterion_group!(benches, codec);
criterion_main!(benches);
