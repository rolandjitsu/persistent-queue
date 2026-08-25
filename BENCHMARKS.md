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
