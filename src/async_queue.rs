//! Async facade over the sync core, behind the `tokio` feature.
//!
//! Store I/O (the fsync in `push`/`ack`, the seek in `reserve`) runs on
//! `spawn_blocking`, so it never blocks an async worker. Waiting is async: a full
//! `push` and an empty `reserve` `.await` a [`Notify`] and re-check, so a blocked
//! producer or consumer costs a task, not a parked blocking-pool thread.

use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::{JoinError, spawn_blocking};

use crate::error::{OpenError, PushError, TryPushError};
use crate::queue::{Builder, Consumer, Producer, Reserved};
use crate::store::Store;

// Wakeups shared by the async handles: `room` fires when a slot frees (ack), `items`
// when something becomes reservable (push, nack, drop) or the queue drains on close.
struct Signals {
    room: Notify,
    items: Notify,
}

/// The async producer/consumer pair returned by [`Builder::open_async`].
pub type AsyncEnds<S> = (AsyncProducer<S>, AsyncConsumer<S>);

impl<S: Store + 'static> Builder<S> {
    /// Open the queue with an async (tokio) producer/consumer pair.
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() {
    /// use persistent_queue::{Builder, MemStore};
    ///
    /// let (tx, rx) = Builder::new(MemStore::new()).open_async().await.unwrap();
    /// tx.push(b"job".to_vec()).await.unwrap();
    ///
    /// let item = rx.reserve().await.unwrap().unwrap();
    /// assert_eq!(&*item, b"job");
    /// item.ack().await.unwrap();
    /// # }
    /// ```
    pub async fn open_async(self) -> Result<AsyncEnds<S>, OpenError<S::Error>> {
        let (producer, consumer) = join(spawn_blocking(move || self.open()).await)?;
        let signals = Arc::new(Signals {
            room: Notify::new(),
            items: Notify::new(),
        });
        Ok((
            AsyncProducer {
                inner: producer,
                signals: Arc::clone(&signals),
            },
            AsyncConsumer {
                inner: Arc::new(consumer),
                signals,
            },
        ))
    }
}

/// The async producer half. Clone it for multiple producers.
pub struct AsyncProducer<S> {
    inner: Producer<S>,
    signals: Arc<Signals>,
}

impl<S> Clone for AsyncProducer<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            signals: Arc::clone(&self.signals),
        }
    }
}

impl<S: Store + 'static> AsyncProducer<S> {
    /// Append `value`, awaiting (without holding a thread) while the queue is full.
    pub async fn push(&self, value: Vec<u8>) -> Result<(), PushError<S::Error>> {
        let value: Arc<[u8]> = Arc::from(value);
        let mut waker = std::pin::pin!(self.signals.room.notified());
        loop {
            // Register as a waiter before the attempt, so an ack or close that fires
            // between the attempt and the await is not missed.
            waker.as_mut().enable();
            let inner = self.inner.clone();
            let value = Arc::clone(&value);
            match join(spawn_blocking(move || inner.try_push(&value)).await) {
                Ok(()) => {
                    self.signals.items.notify_one();
                    return Ok(());
                }
                Err(TryPushError::Full) => {}
                Err(TryPushError::Closed) => return Err(PushError::Closed),
                Err(TryPushError::Store(e)) => return Err(PushError::Store(e)),
            }
            waker.as_mut().await;
            waker.set(self.signals.room.notified());
        }
    }

    /// Append `value`, or return [`TryPushError::Full`] without waiting.
    pub async fn try_push(&self, value: Vec<u8>) -> Result<(), TryPushError<S::Error>> {
        let inner = self.inner.clone();
        let result = join(spawn_blocking(move || inner.try_push(&value)).await);
        if result.is_ok() {
            self.signals.items.notify_one();
        }
        result
    }

    /// Close the queue; further pushes fail and the consumer drains what remains.
    pub fn close(&self) {
        self.inner.close();
        self.signals.room.notify_waiters();
        self.signals.items.notify_waiters();
    }

    /// Number of unacked items currently in the queue.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the queue holds no unacked items.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// The async consumer half. Single consumer, so it does not implement `Clone`.
pub struct AsyncConsumer<S> {
    inner: Arc<Consumer<S>>,
    signals: Arc<Signals>,
}

impl<S: Store + 'static> AsyncConsumer<S> {
    /// Reserve the oldest item, awaiting (without holding a thread) until one is
    /// available. Returns `None` only once the queue is closed and fully drained.
    pub async fn reserve(&self) -> Result<Option<AsyncReserved<S>>, S::Error> {
        let mut waker = std::pin::pin!(self.signals.items.notified());
        loop {
            waker.as_mut().enable();
            let consumer = Arc::clone(&self.inner);
            match join(spawn_blocking(move || consumer.reserve()).await)? {
                Some(inner) => {
                    return Ok(Some(AsyncReserved {
                        inner: Some(inner),
                        signals: Arc::clone(&self.signals),
                    }));
                }
                None if self.inner.is_drained() => return Ok(None),
                None => {}
            }
            waker.as_mut().await;
            waker.set(self.signals.items.notified());
        }
    }
}

/// A reserved item from the async consumer. Derefs to its bytes; [`ack`](Self::ack)
/// removes it, [`nack`](Self::nack) or drop returns it for redelivery.
pub struct AsyncReserved<S: Store> {
    inner: Option<Reserved<S>>,
    signals: Arc<Signals>,
}

impl<S: Store + 'static> AsyncReserved<S> {
    /// The item's sequence number: a stable id that survives redelivery.
    pub fn seq(&self) -> u64 {
        self.inner
            .as_ref()
            .expect("reserved already consumed")
            .seq()
    }

    /// Remove the item from the queue, committed per the durability policy.
    pub async fn ack(mut self) -> Result<(), S::Error> {
        let reserved = self.inner.take().expect("reserved already consumed");
        let result = join(spawn_blocking(move || reserved.ack()).await);
        if result.is_ok() {
            self.signals.room.notify_one();
            self.signals.items.notify_one();
        }
        result
    }

    /// Return the item for redelivery without removing it.
    pub fn nack(mut self) {
        if let Some(reserved) = self.inner.take() {
            reserved.nack();
            self.signals.items.notify_one();
        }
    }
}

impl<S: Store> Deref for AsyncReserved<S> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.inner.as_ref().expect("reserved already consumed")
    }
}

impl<S: Store> Drop for AsyncReserved<S> {
    fn drop(&mut self) {
        // Release before the wake, so the seq is reservable by the time a woken
        // `reserve` looks for it.
        if let Some(reserved) = self.inner.take() {
            reserved.nack();
            self.signals.items.notify_one();
        }
    }
}

// A spawn_blocking task cannot be cancelled, so a JoinError is always a panic;
// re-raise it on the caller's task instead of swallowing it.
fn join<T>(joined: Result<T, JoinError>) -> T {
    joined.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}
