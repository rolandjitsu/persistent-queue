//! Zero-copy rkyv reads (behind the `rkyv` feature): reserve an item and read it as
//! `&Archived<T>` without decoding it. The producer encodes `T` with rkyv; the
//! consumer aligns and validates the bytes once at reserve, then reads fields in place.

use std::marker::PhantomData;

use rkyv::api::high::HighValidator;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Portable, Serialize};

use crate::codec::CodecError;
use crate::error::{OpenError, PushError};
use crate::queue::{Builder, Consumer, Producer, Reserved};
use crate::store::Store;
use crate::typed::{ReserveError, TypedPushError};

/// A message type usable with the zero-copy rkyv layer: it can be rkyv-serialized, and
/// its archived form can be validated and read in place. Blanket-implemented, so users
/// only derive rkyv's `Archive` / `Serialize` / `Deserialize` on their type.
pub trait Archivable:
    Archive<Archived: Portable + for<'a> CheckBytes<HighValidator<'a, Error>>>
    + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, Error>>
{
}

impl<T> Archivable for T where
    T: Archive<Archived: Portable + for<'a> CheckBytes<HighValidator<'a, Error>>>
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, Error>>
{
}

/// The producer/consumer pair returned by [`Builder::open_archived`].
pub type ArchivedEnds<S, T> = (ArchivedProducer<S, T>, ArchivedConsumer<S, T>);

impl<S: Store> Builder<S> {
    /// Open a queue whose consumer reads items as `&Archived<T>` without decoding
    /// them (rkyv zero-copy). The producer rkyv-encodes `T`.
    pub fn open_archived<T: Archivable>(self) -> Result<ArchivedEnds<S, T>, OpenError<S::Error>> {
        let (producer, consumer) = self.open()?;
        Ok((
            ArchivedProducer {
                inner: producer,
                _marker: PhantomData,
            },
            ArchivedConsumer {
                inner: consumer,
                _marker: PhantomData,
            },
        ))
    }
}

/// The producer half of a zero-copy rkyv queue. Clone it for multiple producers.
pub struct ArchivedProducer<S, T> {
    inner: Producer<S>,
    _marker: PhantomData<fn(T)>,
}

impl<S, T> Clone for ArchivedProducer<S, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S: Store, T: Archivable> ArchivedProducer<S, T> {
    /// Encode and push `value`, waiting while the queue is at capacity.
    pub fn push(&self, value: &T) -> Result<(), TypedPushError<S::Error>> {
        let bytes = rkyv::to_bytes::<Error>(value)
            .map_err(|e| TypedPushError::Encode(CodecError::new(e)))?;
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

/// The consumer half of a zero-copy rkyv queue. Single consumer.
pub struct ArchivedConsumer<S, T> {
    inner: Consumer<S>,
    _marker: PhantomData<fn() -> T>,
}

impl<S: Store, T: Archivable> ArchivedConsumer<S, T> {
    /// Reserve the oldest item, or `None` if there is nothing to deliver. The bytes
    /// are aligned and validated here, so [`ArchivedReserved::get`] is then free.
    pub fn reserve(&self) -> Result<Option<ArchivedReserved<S, T>>, ReserveError<S::Error>> {
        match self.inner.reserve().map_err(ReserveError::Store)? {
            Some(reserved) => {
                // The store returns an unaligned `Vec<u8>`; copy it into an aligned
                // buffer once, and validate it so `get` can skip the checks.
                let mut aligned = AlignedVec::<16>::new();
                aligned.extend_from_slice(&reserved);
                rkyv::access::<T::Archived, Error>(&aligned)
                    .map_err(|e| ReserveError::Decode(CodecError::new(e)))?;
                Ok(Some(ArchivedReserved {
                    inner: reserved,
                    aligned,
                    _marker: PhantomData,
                }))
            }
            None => Ok(None),
        }
    }
}

/// A reserved item read in place as `&Archived<T>`. [`ack`](Self::ack) removes it,
/// [`nack`](Self::nack) or drop returns it for redelivery.
pub struct ArchivedReserved<S: Store, T> {
    inner: Reserved<S>,
    aligned: AlignedVec,
    _marker: PhantomData<fn() -> T>,
}

impl<S: Store, T: Archivable> ArchivedReserved<S, T> {
    /// The archived view of the item, read directly from the buffer (no decode).
    pub fn get(&self) -> &T::Archived {
        // Validated in `reserve` over this same immutable buffer, so the unchecked
        // access is sound.
        unsafe { rkyv::access_unchecked::<T::Archived>(&self.aligned) }
    }

    /// The item's sequence number: a stable id that survives redelivery.
    pub fn seq(&self) -> u64 {
        self.inner.seq()
    }

    /// Remove the item from the queue, committed per the durability policy.
    pub fn ack(self) -> Result<(), S::Error> {
        self.inner.ack()
    }

    /// Return the item for redelivery without removing it.
    pub fn nack(self) {
        self.inner.nack();
    }
}
