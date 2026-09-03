//! The [`Codec`] trait and built-in codecs for the typed queue layer.

use std::error::Error as StdError;
use std::fmt;

/// Encodes a message type to bytes for the store and decodes it back.
///
/// The typed layer ([`TypedProducer`](crate::TypedProducer) /
/// [`TypedConsumer`](crate::TypedConsumer)) is generic over this trait: implement it
/// for a custom format, or use a built-in like [`Bincode`] behind the `serde` feature.
pub trait Codec<T> {
    /// Encode `value` to bytes.
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError>;
    /// Decode a value from `bytes`.
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
}

/// An encode or decode failure, carrying the underlying codec's message.
#[derive(Debug)]
pub struct CodecError(String);

impl CodecError {
    /// Build a codec error from anything printable, e.g. the codec's own error.
    pub fn new(error: impl fmt::Display) -> Self {
        Self(error.to_string())
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "codec error: {}", self.0)
    }
}

impl StdError for CodecError {}

/// A [`Codec`] that encodes with serde and bincode. Requires the `serde` feature.
///
/// ```
/// use persistent_queue::{Bincode, Builder, MemStore};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq)]
/// struct Job {
///     id: u64,
///     name: String,
/// }
///
/// let (tx, rx) = Builder::new(MemStore::new()).open_typed(Bincode).unwrap();
/// tx.push(&Job { id: 1, name: "build".into() }).unwrap();
///
/// let item = rx.reserve().unwrap().unwrap();
/// assert_eq!(*item, Job { id: 1, name: "build".into() });
/// item.ack().unwrap();
/// ```
#[cfg(feature = "serde")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Bincode;

#[cfg(feature = "serde")]
impl<T> Codec<T> for Bincode
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        bincode::serialize(value).map_err(CodecError::new)
    }

    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError> {
        bincode::deserialize(bytes).map_err(CodecError::new)
    }
}

/// A [`Codec`] that encodes with rkyv. Requires the `rkyv` feature. The message type
/// must derive rkyv's `Archive`, `Serialize`, and `Deserialize`.
///
/// ```
/// use persistent_queue::{Builder, MemStore, Rkyv};
///
/// #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
/// struct Job {
///     id: u64,
///     name: String,
/// }
///
/// let (tx, rx) = Builder::new(MemStore::new()).open_typed(Rkyv).unwrap();
/// tx.push(&Job { id: 1, name: "build".into() }).unwrap();
///
/// let item = rx.reserve().unwrap().unwrap();
/// assert_eq!(*item, Job { id: 1, name: "build".into() });
/// item.ack().unwrap();
/// ```
#[cfg(feature = "rkyv")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Rkyv;

#[cfg(feature = "rkyv")]
impl<T> Codec<T> for Rkyv
where
    T: rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(value)
            .map(|bytes| bytes.to_vec())
            .map_err(CodecError::new)
    }

    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError> {
        // rkyv needs an aligned buffer; the store hands back a plain `Vec<u8>`.
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned).map_err(CodecError::new)
    }
}
