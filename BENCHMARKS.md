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
numbers are shown: that run was under load and its durable path is dominated by the
fsync barrier (macOS flushes differ from Linux), so it never gave a clean disk sweep.
macOS in-memory is in fact faster than Linux here - the hardware is fine, the machines
just differ.

## Linux (x86-64 workstation)

### In-memory (`MemStore`), 10,000 messages

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 10.2 ms | 0.98 Melem/s |
| 1         | Group      | 11.6 ms | 0.86 Melem/s |
| 4         | Sync       | 13.8 ms | 0.72 Melem/s |
| 4         | Group      | 15.2 ms | 0.66 Melem/s |
| 16        | Sync       | 15.6 ms | 0.64 Melem/s |
| 16        | Group      | 20.6 ms | 0.49 Melem/s |
| 64        | Sync       | 16.3 ms | 0.61 Melem/s |
| 64        | Group      | 26.1 ms | 0.38 Melem/s |

### On-disk, 200 messages

sled:

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 5.99 ms | 33.4 Kelem/s |
| 1         | Group      | 6.31 ms | 31.7 Kelem/s |
| 1         | None       | 3.37 ms | 59.3 Kelem/s |
| 16        | Sync       | 6.03 ms | 33.2 Kelem/s |
| 16        | Group      | 3.90 ms | 51.2 Kelem/s |
| 16        | None       | 3.30 ms | 60.7 Kelem/s |
| 64        | Sync       | 6.59 ms | 30.4 Kelem/s |
| 64        | Group      | 4.16 ms | 48.1 Kelem/s |
| 64        | None       | 4.03 ms | 49.7 Kelem/s |

redb:

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 5.00 ms | 40.0 Kelem/s |
| 1         | Group      | 4.95 ms | 40.4 Kelem/s |
| 1         | None       | 16.4 ms | 12.2 Kelem/s |
| 16        | Sync       | 5.43 ms | 36.9 Kelem/s |
| 16        | Group      | 3.28 ms | 61.1 Kelem/s |
| 16        | None       | 16.7 ms | 11.9 Kelem/s |
| 64        | Sync       | 6.61 ms | 30.3 Kelem/s |
| 64        | Group      | 3.86 ms | 51.8 Kelem/s |
| 64        | None       | 19.2 ms | 10.4 Kelem/s |

rocksdb:

| Producers | Durability | Time   | Throughput   |
| --------: | ---------- | -----: | -----------: |
| 1         | Sync       | 190 ms | 1.05 Kelem/s |
| 1         | Group      | 194 ms | 1.03 Kelem/s |
| 1         | None       | 152 ms | 1.32 Kelem/s |
| 16        | Sync       | 154 ms | 1.30 Kelem/s |
| 16        | Group      | 155 ms | 1.29 Kelem/s |
| 16        | None       | 154 ms | 1.30 Kelem/s |
| 64        | Sync       | 146 ms | 1.37 Kelem/s |
| 64        | Group      | 147 ms | 1.36 Kelem/s |
| 64        | None       | 138 ms | 1.45 Kelem/s |

(p4 rows omitted for space; they track p1/p16. Full output is what
`cargo bench --all-features` prints.)

## macOS (Apple-silicon laptop)

### In-memory (`MemStore`), 10,000 messages

| Producers | Durability | Time    | Throughput   |
| --------: | ---------- | ------: | -----------: |
| 1         | Sync       | 6.27 ms | 1.59 Melem/s |
| 1         | Group      | 7.11 ms | 1.41 Melem/s |
| 4         | Sync       | 15.5 ms | 0.65 Melem/s |
| 4         | Group      | 15.0 ms | 0.67 Melem/s |
| 16        | Sync       | 28.5 ms | 0.35 Melem/s |
| 16        | Group      | 41.7 ms | 0.24 Melem/s |
| 64        | Sync       | 23.5 ms | 0.43 Melem/s |
| 64        | Group      | 38.2 ms | 0.26 Melem/s |

Disk was not swept on macOS (see "Read this first"). Use the Linux numbers.

## Takeaways

- The crate's own overhead is in-memory-fast; durability cost is the backend, not
  this code.
- **Group-commit works, on disk, under concurrency.** On Linux it pulls clearly ahead
  of `Sync` from ~16 producers up (redb: 61 vs 37 Kelem/s at 16; sled: 51 vs 33). At
  one producer it ties `Sync` - nothing to batch.
- **Group-commit is a loss in memory** - no fsync to amortize, so its coordination is
  pure overhead (slower than `Sync` at every producer count). Never `Group` on
  `MemStore`.
- **`None` is not always faster.** Fastest for sled; *slower* than `Sync` for redb
  (~16 ms vs ~5 ms), because redb is copy-on-write and cannot reclaim the pages a
  transaction frees until a durable commit - a `None`-only run never checkpoints, so
  the store grows and that outweighs the fsync it skips. `None` also gives no crash
  guarantee (see the durability table in the README); with redb there is no reason to
  use it.
- **RocksDB is ~30x slower here, and its durability policy barely matters** (`Sync`,
  `Group`, and `None` all land near 150 ms), so the cost is not fsync. It is the
  access pattern: `reserve` scans forward from the front on every call, and each `ack`
  leaves an LSM tombstone that RocksDB keeps until compaction, so the scan wades
  through a growing pile of deleted keys (and sets up a fresh iterator each time). A
  B-tree (sled, redb) drops deleted keys immediately and scans cheaply. A head cursor
  that seeks straight to the oldest live entry would let RocksDB skip the tombstones -
  see the roadmap. For now, prefer sled or redb.
- Acks are the remaining floor under `Group` (each still fsyncs); relaxing ack
  durability is the next lever (roadmap).
- Pick the policy and backend for your workload, and measure on your own hardware.
