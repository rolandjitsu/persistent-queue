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
//! See `DESIGN.md` for the on-disk layout, crash recovery, and durability model.
#![warn(missing_docs)]

mod error;
mod queue;
mod store;

pub use error::{OpenError, PushError, TryPushError};
pub use queue::{Builder, Consumer, Durability, Ends, Producer, Reserved};
pub use store::{KeyValue, MemStore, Op, Store};

#[cfg(feature = "redb")]
pub use store::RedbStore;
#[cfg(feature = "sled")]
pub use store::SledStore;
