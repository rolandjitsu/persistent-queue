# persistent-queue

[![CI](https://img.shields.io/github/actions/workflow/status/rolandjitsu/persistent-queue/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/rolandjitsu/persistent-queue/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/rolandjitsu/persistent-queue/main?style=flat-square)](https://codecov.io/gh/rolandjitsu/persistent-queue)
[![crates.io](https://img.shields.io/crates/v/persistent-queue?style=flat-square)](https://crates.io/crates/persistent-queue)
[![docs.rs](https://img.shields.io/docsrs/persistent-queue?style=flat-square)](https://docs.rs/persistent-queue)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](./LICENSE)

A durable, at-least-once MPSC queue backed by in-memory and durable backends.

Many producers push byte payloads; a single consumer reserves each one and holds it
in flight until it acks (removes) or nacks (returns) it. A dropped reservation, a
panic, or a crash all put the item back, so nothing is lost. Storage is a pluggable
`Store` - in-memory by default, `sled` or `redb` behind features, or your own - and
the core is synchronous and runtime-agnostic. See `DESIGN.md` for the on-disk layout,
cursors, and crash recovery.

## Usage

```rust
use persistent_queue::{Builder, MemStore};

let (tx, rx) = Builder::new(MemStore::new()).capacity(1024).open().unwrap();

tx.push(b"job").unwrap(); // waits if the queue is at capacity

if let Some(item) = rx.reserve().unwrap() {
    assert_eq!(&*item, b"job"); // derefs to the bytes
    item.ack().unwrap();        // remove it; or item.nack() to retry later
}
```

For durability, use an on-disk backend:

```rust,ignore
use persistent_queue::{Builder, SledStore};

let store = SledStore::open("/var/lib/myapp/queue").unwrap();
let (tx, rx) = Builder::new(store).capacity(1024).open().unwrap();
```

`RedbStore::open` works the same way behind the `redb` feature.

## Backends

| Backend      | Feature   | Persistent | Notes                                     |
| ------------ | --------- | ---------- | ----------------------------------------- |
| `MemStore`   | (default) | No         | Zero dependencies; tests and baseline.    |
| `SledStore`  | `sled`    | Yes        | On-disk, backed by sled.                  |
| `RedbStore`  | `redb`    | Yes        | On-disk, backed by redb.                  |
| `RocksStore` | `rocksdb` | Yes        | On-disk, backed by RocksDB (bundled C++). |

Implement `Store` yourself for any other key/value store.

- **One process per store.** `sled`, `redb`, and `rocksdb` take an exclusive lock on
  their files, so only one process can open a given database at a time.
- **Corruption is surfaced, not repaired.** A backend error on open (including a
  corrupt store) is returned as `OpenError::Store`; the queue does not auto-repair or
  discard data. A store written by a newer on-disk format is rejected with
  `OpenError::UnsupportedVersion`.
- **RocksDB is a bundled C++ library.** The `rocksdb` feature compiles it from source
  and statically links it into your binary (nothing extra to ship at runtime), but it
  needs a C++ toolchain to build, adds compile time and binary size, and links the C++
  runtime - so a fully static (musl) build is difficult. For a small, pure-Rust, fully
  static binary, use `sled` or `redb`.

## Guarantees

- **Durability.** Once `push` returns under a durable policy, the item survives a
  crash.
- **At-least-once.** Every pushed item is delivered at least once. A crash between
  handling an item and its durable ack redelivers it. True exactly-once is not
  possible from the queue alone; `Reserved::seq()` is a stable id you can dedupe on
  for effectively-once (see the roadmap).
- **Order.** FIFO by sequence number; `reserve` hands items out oldest first.

Durability is a policy on the builder: `Sync` (fsync every push and ack), `Group`
(batch concurrent pushes behind one fsync), or `None` (no fsync - fastest, but recent
items can be lost on a crash). `MemStore` never persists, regardless of policy.

What survives a crash (process exit or power loss):

| Backend                   | `None`                    | `Sync` / `Group`         |
| ------------------------- | ------------------------- | ------------------------ |
| `MemStore`                | nothing (in-memory only)  | nothing (in-memory only) |
| `sled`, `redb`, `rocksdb` | not guaranteed (no fsync) | survives (fsync'd)       |

## Comparison

How persistent-queue relates to some common alternatives - it is durable,
at-least-once, multi-producer, and backend-agnostic:

| Crate                | Durable | Producers / consumers | Storage                            |
| -------------------- | ------- | --------------------- | ---------------------------------- |
| `persistent-queue`   | Yes     | MPSC                  | in-memory, sled, redb, or your own |
| `yaque`              | Yes     | SPSC                  | files                              |
| `sled`, `redb`       | Yes     | not a queue           | own file (key/value store)         |
| `flume`, `crossbeam` | No      | MPMC                  | in-memory                          |

`yaque` is a persistent queue but single-producer / single-consumer (its docs: "an
SPSC channel using your OS' filesystem"). `sled` and `redb` are key/value stores,
not queues - persistent-queue is built on top of them. `flume` and `crossbeam` are
fast in-memory channels with no durability. Backends and semantics change over time;
check each crate's current docs.

## Benchmarks

Throughput across backends, durability policies, and producer counts is in
[BENCHMARKS.md](./BENCHMARKS.md). Short version: the crate's own overhead is
in-memory-fast, and durability cost is the backend's `fsync`. Reproduce with
`cargo bench --all-features`, and measure on your own hardware.

## Roadmap

- **Codec features** (`serde`, `rkyv`) and a typed `Queue<T>` layer over the byte core.
- **tokio async facade** (feature-gated), an async API over the sync core.
- **Relaxed ack durability**: at-least-once already tolerates redelivery, so acks need
  not fsync individually - batching or lazily flushing them roughly halves the
  per-message fsync cost.
- **Richer delivery**: multiple / competing consumers with visibility timeouts,
  dead-letter handling after N redeliveries, priorities and delayed delivery.
- **Byte-based capacity**, to bound on-disk size directly (today it bounds by unacked
  count).
- **Effectively-once helper**: `Reserved::seq()` already survives redelivery as a
  stable id; an opt-in helper could persist processed seqs so the consumer dedupes
  automatically.
- **Zero-copy reads**: return `Bytes` or borrowed data from the store to skip a copy
  per reserve (pairs with the rkyv codec).

## Status

Early, single-maintainer software. The surface is intentionally small: a producer,
a consumer, the `Reserved` guard, and the `Store` trait. Contributions welcome.

## License

[Apache-2.0](./LICENSE).
