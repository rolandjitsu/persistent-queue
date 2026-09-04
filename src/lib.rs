//! A durable, at-least-once MPSC queue backed by in-memory and durable backends.
//!
//! Items are written to a [`Store`] and survive process and machine crashes.
//! Delivery is at-least-once: an item is removed only once the consumer acks it,
//! so a crash between handling and ack redelivers it. The core is synchronous and
//! runtime-agnostic.
//!
//! ```
//! use persistent_queue::{Builder, MemStore};
//!
//! let (tx, rx) = Builder::new(MemStore::new()).capacity(1024).open().unwrap();
//! tx.push(b"job").unwrap();
//! let item = rx.reserve().unwrap().unwrap();
//! assert_eq!(&*item, b"job");
//! item.ack().unwrap();
//! ```
//!
//! For typed messages, [`Builder::open_typed`] wraps the queue with a [`Codec`]
//! (`serde` + `bincode`, or `rkyv`, behind their features), and
//! [`Builder::open_archived`] (rkyv) reads an item as `&Archived<T>` without decoding
//! it. With the `tokio` feature, [`Builder::open_async`] gives async producer/consumer
//! handles that run store I/O on tokio's blocking pool and wait (for capacity, or the
//! next item) asynchronously.
//!
//! See `DESIGN.md` for the on-disk layout, crash recovery, and durability model.
#![warn(missing_docs)]

#[cfg(feature = "rkyv")]
mod archived;
#[cfg(feature = "tokio")]
mod async_queue;
mod codec;
mod error;
mod queue;
mod store;
mod sync;
mod typed;

pub use codec::{Codec, CodecError};
pub use error::{OpenError, PushError, TryPushError};
pub use queue::{Builder, Consumer, Durability, Ends, Producer, Reserved};
pub use store::{KeyValue, MemStore, Op, Store};
pub use typed::{
    ReserveError, TypedConsumer, TypedEnds, TypedProducer, TypedPushError, TypedReserved,
};

#[cfg(feature = "rkyv")]
pub use archived::{
    Archivable, ArchivedConsumer, ArchivedEnds, ArchivedProducer, ArchivedReserved,
};
#[cfg(feature = "tokio")]
pub use async_queue::{AsyncConsumer, AsyncEnds, AsyncProducer, AsyncReserved};
#[cfg(feature = "serde")]
pub use codec::Bincode;
#[cfg(feature = "rkyv")]
pub use codec::Rkyv;

#[cfg(feature = "redb")]
pub use store::RedbStore;
#[cfg(feature = "rocksdb")]
pub use store::RocksStore;
#[cfg(feature = "sled")]
pub use store::SledStore;
