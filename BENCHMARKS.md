# Benchmarks

Throughput of the full push + reserve + ack cycle across backends, durability
policies, and producer counts.

- Workload: N producer threads push N total messages of 256 bytes; one consumer
  reserves and acks all of them. Capacity 1024.
- Tool: criterion; each figure is the median. Reproduce with
  `cargo bench --all-features`.
- Two machines, reported separately. **Do not compare numbers across machines** -
  different CPUs and disks.

## Read this first

The durable on-disk numbers come from the Linux box. On macOS only the in-memory
numbers are shown: that run's durable path is dominated by the fsync barrier (macOS
flushes to the device differently than Linux), so it never gave a clean disk sweep.
macOS is not slow overall - its in-memory numbers beat Linux here; it just pays more
per fsync. The two machines differ in CPU, disk, and OS, so compare within a machine,
not across.

## Linux (x86-64 workstation)

### In-memory (`MemStore`), 10,000 messages

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 10.2 ms | 0.98 Melem/s |
| 1         | Group      | 12.1 ms | 0.83 Melem/s |
| 16        | Sync       | 15.6 ms | 0.64 Melem/s |
| 16        | Group      | 21.6 ms | 0.46 Melem/s |
| 64        | Sync       | 17.9 ms | 0.56 Melem/s |
| 64        | Group      | 26.3 ms | 0.38 Melem/s |

### On-disk, 200 messages

sled:

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 7.26 ms | 27.5 Kelem/s |
| 1         | Group      | 7.15 ms | 28.0 Kelem/s |
| 1         | None       | 3.47 ms | 57.7 Kelem/s |
| 16        | Sync       | 6.97 ms | 28.7 Kelem/s |
| 16        | Group      | 4.91 ms | 40.8 Kelem/s |
| 16        | None       | 3.72 ms | 53.8 Kelem/s |
| 64        | Sync       | 7.92 ms | 25.3 Kelem/s |
| 64        | Group      | 4.98 ms | 40.1 Kelem/s |
| 64        | None       | 4.56 ms | 43.9 Kelem/s |

redb:

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 5.43 ms | 36.8 Kelem/s |
| 1         | Group      | 5.52 ms | 36.2 Kelem/s |
| 1         | None       | 16.8 ms | 11.9 Kelem/s |
| 16        | Sync       | 5.71 ms | 35.0 Kelem/s |
| 16        | Group      | 3.86 ms | 51.9 Kelem/s |
| 16        | None       | 17.1 ms | 11.7 Kelem/s |
| 64        | Sync       | 7.05 ms | 28.4 Kelem/s |
| 64        | Group      | 4.21 ms | 47.5 Kelem/s |
| 64        | None       | 19.7 ms | 10.2 Kelem/s |

rocksdb:

| Producers | Durability | Time    | Throughput  |
| --------: | ---------- | ------: | ----------: |
| 1         | Sync       | 900 us  | 222 Kelem/s |
| 1         | Group      | 935 us  | 214 Kelem/s |
| 1         | None       | 816 us  | 245 Kelem/s |
| 16        | Sync       | 1.44 ms | 139 Kelem/s |
| 16        | Group      | 1.01 ms | 199 Kelem/s |
| 16        | None       | 1.45 ms | 138 Kelem/s |
| 64        | Sync       | 2.09 ms | 95.9 Kelem/s |
| 64        | Group      | 1.96 ms | 102 Kelem/s |
| 64        | None       | 1.99 ms | 101 Kelem/s |

## macOS (Apple-silicon laptop)

### In-memory (`MemStore`), 10,000 messages

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 6.27 ms | 1.59 Melem/s |
| 1         | Group      | 7.11 ms | 1.41 Melem/s |
| 16        | Sync       | 28.5 ms | 0.35 Melem/s |
| 16        | Group      | 41.7 ms | 0.24 Melem/s |

Disk was not swept on macOS (see "Read this first"). Use the Linux numbers.

## Async facade overhead

The `tokio` facade runs each store op (push, reserve, ack) on `spawn_blocking`, so it
adds one blocking-pool hop per op over the sync core - a big multiplier on an in-memory
op, and a rounding error next to an fsync. One push + reserve + ack, macOS:

| Backend           | sync    | async facade |
| ----------------- | ------: | -----------: |
| `MemStore`        | 148 ns  | 8.8 us       |
| `sled` (fsync/op) | 10.7 ms | 13.8 ms      |

- On `MemStore` the facade adds ~8.6 us (the three `spawn_blocking` hops against a
  ~150 ns memory op, ~60x). Prefer the sync API for raw in-memory throughput.
- On `sled` the ~10 ms fsync dominates; the hop is negligible, and the gap here is
  macOS fsync jitter (the async run spanned 11.6-17 ms). The facade is effectively free
  once a durable write is involved.
- Reproduce with `cargo bench --features tokio,sled --bench async_overhead`.

## Codec cost

Per-message cost of the typed layer's codecs, on a queue-message-shaped record (a u64,
u32, u64, a short string, and a 1 KB payload): encode, full owned decode, and reading
a single field.

Linux:

| operation      | `Bincode` | `Rkyv` | speedup |
| -------------- | --------: | -----: | ------: |
| encode         | 498 ns    | 94 ns  | ~5x     |
| decode (owned) | 750 ns    | 61 ns  | ~12x    |
| read one field | 756 ns    | 24 ns  | ~31x    |

macOS:

| operation      | `Bincode` | `Rkyv` | speedup |
| -------------- | --------: | -----: | ------: |
| encode         | 418 ns    | 100 ns | ~4x     |
| decode (owned) | 587 ns    | 90 ns  | ~6.5x   |
| read one field | 557 ns    | 33 ns  | ~17x    |

- `Rkyv` wins across the board; the standout is reading one field, where the zero-copy
  path (`open_archived`) reads it in place instead of decoding the whole 1 KB record.
- The read is ~17-31x (not the ~100x of a pre-aligned buffer) because the store hands
  back an unaligned `Vec<u8>`: the zero-copy path copies it into an aligned buffer and
  validates it once (~20-26 ns) before reading in place.
- rkyv asks more of the message type (derive `Archive`/`Serialize`/`Deserialize`, and
  some types do not fit); `Bincode` only needs serde. Reach for `Rkyv` when decode or
  field reads are hot, or messages are large.
- Reproduce with `cargo bench --features serde,rkyv --bench codec`.

## Takeaways

- The crate's own overhead is in-memory-fast; durability cost is the backend.
- **RocksDB is the fastest on-disk backend here** (~96-245 Kelem/s, several times
  sled and redb). It relies on `reserve` seeking from the head cursor; a front-scan
  is far slower on its LSM because of the tombstones acks leave.
- **Group-commit wins on disk under concurrency** for the B-tree stores (redb
  `group/p16` 52 vs `sync/p16` 35 Kelem/s; sled 41 vs 29). RocksDB barely changes -
  its WAL already coalesces writes.
- **Group-commit is a loss in memory** - no fsync to amortize, so its coordination is
  pure overhead. Never `Group` on `MemStore`.
- **redb `None` is the outlier** (~11 Kelem/s, slower than its own `Sync`): redb is
  copy-on-write and cannot reclaim pages until a durable commit, so a `None`-only run
  never checkpoints and the store grows. `None` also gives no crash guarantee (see the
  README durability table). With redb, prefer `Sync`/`Group`.
- Pick the backend and policy for your workload, and measure on your own hardware.
