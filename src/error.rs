use std::error::Error as StdError;
use std::fmt;

/// Error from opening a queue over a store.
#[derive(Debug)]
pub enum OpenError<E> {
    /// The backend store failed.
    Store(E),
    /// The store was written by an unsupported on-disk format version.
    UnsupportedVersion(u8),
}

impl<E: fmt::Display> fmt::Display for OpenError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::Store(e) => write!(f, "store error: {e}"),
            OpenError::UnsupportedVersion(v) => {
                write!(f, "unsupported on-disk format version: {v}")
            }
        }
    }
}

impl<E: StdError + 'static> StdError for OpenError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            OpenError::Store(e) => Some(e),
            OpenError::UnsupportedVersion(_) => None,
        }
    }
}

/// Error from a blocking [`Producer::push`](crate::Producer::push).
#[derive(Debug)]
pub enum PushError<E> {
    /// The queue was closed.
    Closed,
    /// The backend store failed.
    Store(E),
}

impl<E: fmt::Display> fmt::Display for PushError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Closed => write!(f, "queue is closed"),
            PushError::Store(e) => write!(f, "store error: {e}"),
        }
    }
}

impl<E: StdError + 'static> StdError for PushError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            PushError::Store(e) => Some(e),
            PushError::Closed => None,
        }
    }
}

/// Error from [`Producer::try_push`](crate::Producer::try_push).
#[derive(Debug)]
pub enum TryPushError<E> {
    /// The queue is at capacity.
    Full,
    /// The queue was closed.
    Closed,
    /// The backend store failed.
    Store(E),
}

impl<E: fmt::Display> fmt::Display for TryPushError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryPushError::Full => write!(f, "queue is full"),
            TryPushError::Closed => write!(f, "queue is closed"),
            TryPushError::Store(e) => write!(f, "store error: {e}"),
        }
    }
}

impl<E: StdError + 'static> StdError for TryPushError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            TryPushError::Store(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn store_err() -> io::Error {
        io::Error::other("boom")
    }

    #[test]
    fn open_error_display_and_source() {
        assert!(
            OpenError::Store(store_err())
                .to_string()
                .contains("store error")
        );
        let bad = OpenError::<io::Error>::UnsupportedVersion(9);
        assert!(bad.to_string().contains("version: 9"));
        assert!(OpenError::Store(store_err()).source().is_some());
        assert!(bad.source().is_none());
    }

    #[test]
    fn push_error_display_and_source() {
        assert!(
            PushError::<io::Error>::Closed
                .to_string()
                .contains("closed")
        );
        assert!(
            PushError::Store(store_err())
                .to_string()
                .contains("store error")
        );
        assert!(PushError::Store(store_err()).source().is_some());
        assert!(PushError::<io::Error>::Closed.source().is_none());
    }

    #[test]
    fn try_push_error_display_and_source() {
        assert!(TryPushError::<io::Error>::Full.to_string().contains("full"));
        assert!(
            TryPushError::<io::Error>::Closed
                .to_string()
                .contains("closed")
        );
        assert!(
            TryPushError::Store(store_err())
                .to_string()
                .contains("store error")
        );
        assert!(TryPushError::Store(store_err()).source().is_some());
        assert!(TryPushError::<io::Error>::Full.source().is_none());
    }
}
