//! The typed queue layer: encode on push, decode on reserve, over any [`Codec`].

use std::error::Error as StdError;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::codec::{Codec, CodecError};
use crate::error::{OpenError, PushError};
use crate::queue::{Builder, Consumer, Producer, Reserved};
use crate::store::Store;

/// The producer/consumer pair returned by [`Builder::open_typed`].
pub type TypedEnds<S, T, C> = (TypedProducer<S, T, C>, TypedConsumer<S, T, C>);

impl<S: Store> Builder<S> {
    /// Open a typed queue that encodes values with `codec`.
    ///
    /// Returns a typed producer/consumer pair over the same backend as
    /// [`open`](Builder::open); the configured `capacity` and `durability` apply
    /// unchanged.
    pub fn open_typed<T, C>(self, codec: C) -> Result<TypedEnds<S, T, C>, OpenError<S::Error>>
    where
        C: Codec<T> + Clone,
    {
        let (producer, consumer) = self.open()?;
        Ok((
            TypedProducer {
                inner: producer,
                codec: codec.clone(),
                _marker: PhantomData,
            },
            TypedConsumer {
                inner: consumer,
                codec,
                _marker: PhantomData,
            },
        ))
    }
}

/// The producer half of a typed queue. Clone it for multiple producers.
pub struct TypedProducer<S, T, C> {
    inner: Producer<S>,
    codec: C,
    _marker: PhantomData<fn(T)>,
}

impl<S, T, C: Clone> Clone for TypedProducer<S, T, C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S: Store, T, C: Codec<T>> TypedProducer<S, T, C> {
    /// Encode and push `value`, waiting while the queue is at capacity.
    pub fn push(&self, value: &T) -> Result<(), TypedPushError<S::Error>> {
        let bytes = self.codec.encode(value).map_err(TypedPushError::Encode)?;
        self.inner.push(&bytes).map_err(|e| match e {
            PushError::Closed => TypedPushError::Closed,
            PushError::Store(e) => TypedPushError::Store(e),
        })
    }

    /// Close the queue; further pushes fail and the consumer drains what remains.
    pub fn close(&self) {
        self.inner.close();
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

/// The consumer half of a typed queue. Single consumer.
pub struct TypedConsumer<S, T, C> {
    inner: Consumer<S>,
    codec: C,
    _marker: PhantomData<fn() -> T>,
}

impl<S: Store, T, C: Codec<T>> TypedConsumer<S, T, C> {
    /// Reserve and decode the oldest item, or `None` if there is nothing to deliver.
    pub fn reserve(&self) -> Result<Option<TypedReserved<S, T>>, ReserveError<S::Error>> {
        match self.inner.reserve().map_err(ReserveError::Store)? {
            Some(reserved) => {
                let value = self.codec.decode(&reserved).map_err(ReserveError::Decode)?;
                Ok(Some(TypedReserved {
                    inner: reserved,
                    value,
                }))
            }
            None => Ok(None),
        }
    }
}

/// A reserved, decoded item. Derefs to the value; [`ack`](TypedReserved::ack) removes
/// it while [`nack`](TypedReserved::nack) or drop returns it for redelivery.
pub struct TypedReserved<S: Store, T> {
    inner: Reserved<S>,
    value: T,
}

impl<S: Store, T> TypedReserved<S, T> {
    /// The item's sequence number, stable across redeliveries.
    pub fn seq(&self) -> u64 {
        self.inner.seq()
    }

    /// Remove the item from the queue.
    pub fn ack(self) -> Result<(), S::Error> {
        self.inner.ack()
    }

    /// Return the item for redelivery without removing it.
    pub fn nack(self) {
        self.inner.nack();
    }
}

impl<S: Store, T> Deref for TypedReserved<S, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

/// Error from [`TypedProducer::push`].
#[derive(Debug)]
pub enum TypedPushError<E> {
    /// Encoding the value failed.
    Encode(CodecError),
    /// The queue was closed.
    Closed,
    /// The backend store failed.
    Store(E),
}

impl<E: fmt::Display> fmt::Display for TypedPushError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypedPushError::Encode(e) => write!(f, "{e}"),
            TypedPushError::Closed => write!(f, "queue is closed"),
            TypedPushError::Store(e) => write!(f, "store error: {e}"),
        }
    }
}

impl<E: StdError + 'static> StdError for TypedPushError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            TypedPushError::Encode(e) => Some(e),
            TypedPushError::Store(e) => Some(e),
            TypedPushError::Closed => None,
        }
    }
}

/// Error from [`TypedConsumer::reserve`].
#[derive(Debug)]
pub enum ReserveError<E> {
    /// The backend store failed.
    Store(E),
    /// Decoding the reserved item failed.
    Decode(CodecError),
}

impl<E: fmt::Display> fmt::Display for ReserveError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReserveError::Store(e) => write!(f, "store error: {e}"),
            ReserveError::Decode(e) => write!(f, "{e}"),
        }
    }
}

impl<E: StdError + 'static> StdError for ReserveError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ReserveError::Store(e) => Some(e),
            ReserveError::Decode(e) => Some(e),
        }
    }
}
