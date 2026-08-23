# persistent-queue - design

A durable, at-least-once, multi-producer / single-consumer (MPSC) queue. Items are
written to a pluggable byte store and survive process and machine crashes. The core
is synchronous and depends on no runtime; an optional `tokio` feature adds an async
facade.

This document is the design we agreed before writing code. It is the contract the
implementation and tests are written against.

## Scope (v0.1)

In:

- Durable FIFO queue, bounded, with backpressure when full.
- At-least-once delivery via reserve / ack.
- Backend-agnostic storage behind a small `Store` trait; in-memory backend by
  default, `sled` and `redb` behind features.
- Sync core, optional `tokio` async facade.
- A raw byte API, plus an optional typed layer behind a codec feature.

Out (possible later, explicitly not in v0.1):

- Multiple or competing consumers.
- Priorities, delayed / scheduled delivery, TTL, dead-letter queues.
- Encryption or compression (do it in your codec if you need it).

## Guarantees

- **Durability.** Once `push` returns under the durable policy, the item survives a
  crash. See the durability policies below for the weaker, faster modes.
- **At-least-once.** Every pushed item is delivered at least once. It is removed only
  when the consumer acks it. If the consumer crashes after handling an item but
  before the ack is durable, the item is redelivered on restart. This is inherent:
  the side effect and the ack cannot be made atomic across a crash.
- **Not exactly-once.** The queue cannot promise it. To get effectively-once, the
  consumer must be idempotent, or must record completion in the same transaction as
  its own work (an idempotency key on the consumer side is the usual answer).
- **Order.** FIFO by sequence number. Each `push` takes a short lock to grab the next
  sequence number, so items are ordered by which push grabs it first: one producer's
  items stay in order, and across producers it is first-come-first-served. `reserve`
  hands items out oldest first.

## Model

The queue is an ordered log keyed by a monotonic `u64` sequence number. Two cursors,
`head` (oldest unacked) and `tail` (next sequence to write), walk that log. Neither
cursor is persisted - both are derived from the stored keys on open (see Recovery).
The set of currently reserved (in-flight) items is kept in memory only, which is
exactly what makes recovery redeliver them.

```
key space (ordered):   [0x00] meta           <- format version, written once
                       [0x01][seq: u64 BE]    <- one entry per key, value = item bytes

head = smallest existing entry key   (oldest unacked)
tail = largest existing entry key + 1 (next push)   (0 if empty)
reserved: in-memory set of seqs handed out but not yet acked
```

Keeping entries under a `0x01` prefix keeps them ordered and contiguous in the store
and keeps the one-byte `0x00` meta record out of the entry range. The meta record
holds only a format version (and room for flags); it deliberately does **not** hold
`head` / `tail`, because a persisted cursor can disagree with the keys after a crash
and then has to be reconciled against them anyway - so we skip it and derive.

### Why derive the cursors instead of storing them

On open we need `tail = max(existing entry key) + 1` and `head = min(existing entry
key)`. If we also stored `head`/`tail` in the meta record, a crash between writing an
entry key and updating the meta record would leave them inconsistent, so recovery
would still have to scan the keys to fix them up. Deriving is strictly simpler, has
one source of truth (the keys), and both backends give min/max cheaply (a B-tree
first/last).

## The `Store` trait

The store is pure key/value bytes. It knows nothing about queues, sequence numbers,
or acks. It must provide ordered access in both directions and one atomic, optionally
durable, write.

```rust
pub trait Store: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Value for an exact key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Smallest entry whose key is >= `from`.
    fn seek(&self, from: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, Self::Error>;

    /// Greatest entry whose key is <= `upto`.
    fn seek_back(&self, upto: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, Self::Error>;

    /// Apply all ops atomically. When `durable` is true, do not return until the
    /// write survives a crash (fsync). When false, it may still be in the OS page
    /// cache (fast, lost on power failure).
    fn commit(&self, ops: &[Op<'_>], durable: bool) -> Result<(), Self::Error>;
}

pub enum Op<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
}
```

- `seek` / `seek_back` cover reserve (walk forward from `head`), recovery (`head` =
  `seek(entry_prefix)`, `tail` = `seek_back(max entry key)`), and skipping reserved
  seqs.
- `commit` applies a batch of ops and, when `durable`, does not return until they
  survive a crash. `push` is one `Put` and `ack` is one `Delete` - single-key writes,
  atomic on their own in every backend, so there are no transactions here. The list
  exists only for group-commit (many pushes behind one fsync), and it need not be
  all-or-nothing: if a crash lands mid-batch, each key that made it is a valid entry,
  and a producer only sees `Ok` after the fsync returns, so nothing is lost. Backends
  that offer atomic batches (sled, redb, rocksdb) are welcome to; we do not rely on it.

Backends in v0.1: `mem` (a `Mutex<BTreeMap<Vec<u8>, Vec<u8>>>`, the default;
`durable` is a no-op), `sled` (feature `sled`), and `redb` (feature `redb`). Two
on-disk backends give a real durability comparison in the benchmarks. `rocksdb` stays
on the roadmap - it drags a C++ dependency. Any other store - a raw append-only file,
an object store - is a downstream `Store` impl.

## Public API

The core is bytes in, bytes out. A typed layer sits on top behind a codec feature.

```rust
// Construction. Capacity is the max number of unacked items before push blocks.
let queue = Builder::new(store)
    .capacity(1024)                 // backpressure bound (unacked items)
    .durability(Durability::Group)  // Sync | Group | None
    .open()?;                       // derives head/tail, recovers state

// Producer (sync core). Blocks while the queue is full.
queue.push(&bytes)?;                // PushError::Closed
queue.try_push(&bytes)?;            // TryPushError::{Full, Closed}

// Consumer (single consumer). None when empty.
if let Some(item) = queue.reserve()? {   // Reserved<'_>
    handle(&item);                       // Deref -> &[u8]
    item.ack()?;                         // remove, advance head, commit
    // item.nack()?  -> return for redelivery
    // dropping without ack == nack (safe default; nothing is lost on a panic)
}
```

- `Reserved` derefs to `&[u8]`, and exposes `seq()` and `ack()` / `nack()`. Its
  `Drop` is a nack: if the consumer panics or drops it, the item stays for
  redelivery. This is the opposite of `weighted-mpsc`'s `Lease` (whose drop releases)
  - deliberately a different type name so the difference is loud.
- `close()` marks the queue closed: pending and future `push` calls return `Closed`,
  the consumer drains what remains, then `reserve` returns `None`.

### Typed layer

Behind a codec feature, a thin wrapper serialises `T` to bytes on push and back on
reserve:

```rust
let queue: Queue<Job> = Builder::new(store).capacity(1024).open_typed()?; // serde/bincode
queue.push(&job)?;
let item = queue.reserve()?;   // Reserved<Job>, Deref -> &Job
```

- Feature `serde` uses `serde` + `bincode`. Feature `rkyv` stores the `rkyv`
  encoding, and can expose the archived view without a full decode - the zero-copy
  path. The store stays pure bytes; the codec is the only thing that knows the shape.

## Concurrency

One `Mutex<Inner>` guards the in-memory state: `tail`, `head`, the `reserved` set,
`closed`, and a producers-waiting condvar. The rules:

- **The lock is never held across store I/O.** Under the lock a producer claims a
  sequence number and a capacity slot; the `commit` (the fsync) runs after the lock is
  released. Holding it across the fsync would serialise every producer at disk speed
  and defeat having multiple producers at all.
- **Backpressure.** When the queue is full (`unacked >= capacity`), `push` waits on a
  condvar; `ack` signals it after freeing a slot. In the `tokio` facade the whole
  call runs on `spawn_blocking`, so the blocking wait is on a blocking-pool thread,
  not the async worker. (Caveat: many simultaneously-blocked producers tie up
  blocking-pool threads; an async-native wait is a later refinement.)
- **Single consumer.** `reserve` walks forward from `head` with `seek`, skipping any
  seq already in the `reserved` set, marks the chosen seq reserved, and returns it.
  `ack` deletes the key and, if it was at `head`, advances `head` via `seek`; an
  out-of-order ack just deletes its key and lets `head` catch up later. Because
  `reserved` is in memory, a crash clears it and every unacked entry is reservable
  again from `head`.

No data race results, and it is worth being precise about why. Every access to the
shared in-memory state (`tail`, `head`, `reserved`, the count, `closed`) is under the
one mutex, so there is never unsynchronised shared access. Two producers committing at
once write *different* keys (their own claimed seqs) through a `Send + Sync` store
whose `commit` takes `&self` and synchronises internally, so those accesses do not
conflict either. The only thing the lock does not order is which commit reaches disk
first - and that is not a data race, just nondeterministic durability order, which the
design tolerates: a crash can leave a gap (seq 6 durable, seq 5 not), `reserve` skips
gaps via `seek`, and the producer of the missing seq never received `Ok`. A failed
`commit` re-locks to release its capacity slot; the claimed seq is never reused (a
benign gap). loom verifies exactly this handoff.

Lock-free is deliberately not a goal: the cost is dominated by the `commit` fsync,
so removing an uncontended in-memory lock would not move the number. (We learned this
the expensive way on `weighted-mpsc`.)

## Durability policies

- `Sync` - every `push` and `ack` commits durably (fsync each). Slowest, strongest.
- `Group` - group-commit. Producers add their put to a shared pending batch and wait;
  one flusher writes the batch with a single fsync and wakes all of them. Trades a
  little latency for a large throughput gain under load. This is the default and the
  knob the benchmarks exist to characterise.
- `None` - commit with `durable = false` (stays in the OS page cache). Survives a
  process crash but not a power loss. For workloads that only need
  restart-durability.

Implementation order: build `Sync` first (correct and simple), then layer `Group` and
`None`. The benchmark then shows the delta between them, which is the crate's story.

## Crash recovery

On `open`:

1. `head = seek([0x01])` -> smallest entry key, or the queue is empty.
2. `tail = seek_back([0x01, 0xFF..]) + 1`, or `0` if empty.
3. `reserved` starts empty. Every stored entry is therefore reservable - anything
   that was in flight when we crashed is simply redelivered. That is the at-least-once
   behaviour, and it needs no recovery code of its own.
4. Read / write the meta version record; refuse to open a store written by a newer
   format.

Write ordering that makes this correct:

- `push` commits the entry `Put` before returning. A crash before the commit means
  the item was never accepted (the caller has not gotten `Ok`); a crash after means
  the key exists and is recovered.
- `ack` commits the `Delete` before returning `Ok`. A crash before the delete is
  durable leaves the key present, so the item is redelivered - at-least-once, as
  intended.

## Testing and verification

- **loom** on the in-memory concurrency: lock, condvar wake, reserved-set handoff,
  and close, over a mock in-memory store. This is the part we own and where a lost
  wakeup would live.
- **Miri** on the unit and property tests for UB and data races.
- **Property tests** with a fault-injecting `Store` that can stop applying writes at
  an arbitrary point (simulated crash) and drop non-durable writes: assert no item is
  lost, no item vanishes without an ack, redelivery is bounded, and the queue reopens
  cleanly. Many producers on real OS threads, random sizes, tight capacity.
- **Benchmarks** (criterion): backend sweep (mem vs sled vs redb), durability sweep
  (`Sync` / `Group` / `None`), group-commit batch-size sweep, producer sweep
  (1 / 4 / 16 / 64), and a codec sweep (serde/bincode vs rkyv, including rkyv's
  zero-copy read path). The in-memory + `None` number is the baseline (our bookkeeping
  only); the on-disk + `Sync` number is the true cost of durability; the curve
  between them is the point.
