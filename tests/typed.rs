use persistent_queue::{Builder, Codec, CodecError, MemStore, ReserveError};

// A custom codec proves the typed layer works without the `serde` feature.
#[derive(Clone)]
struct Identity;

impl Codec<Vec<u8>> for Identity {
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, CodecError> {
        Ok(value.clone())
    }
    fn decode(&self, bytes: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(bytes.to_vec())
    }
}

#[derive(Clone)]
struct FailDecode;

impl Codec<u8> for FailDecode {
    fn encode(&self, value: &u8) -> Result<Vec<u8>, CodecError> {
        Ok(vec![*value])
    }
    fn decode(&self, _bytes: &[u8]) -> Result<u8, CodecError> {
        Err(CodecError::new("always fails"))
    }
}

#[test]
fn custom_codec_roundtrips() {
    let (tx, rx) = Builder::new(MemStore::new()).open_typed(Identity).unwrap();
    tx.push(&b"hello".to_vec()).unwrap();

    let item = rx.reserve().unwrap().unwrap();
    assert_eq!(*item, b"hello".to_vec());
    item.ack().unwrap();

    assert!(rx.reserve().unwrap().is_none());
}

#[test]
fn reserve_surfaces_decode_errors() {
    let (tx, rx) = Builder::new(MemStore::new())
        .open_typed(FailDecode)
        .unwrap();
    tx.push(&1u8).unwrap();

    match rx.reserve() {
        Err(ReserveError::Decode(_)) => {}
        Err(ReserveError::Store(_)) => panic!("unexpected store error"),
        Ok(_) => panic!("expected a decode error"),
    }
}

#[cfg(feature = "serde")]
mod serde_codec {
    use persistent_queue::{Bincode, Builder, MemStore};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct Job {
        id: u64,
        name: String,
    }

    #[test]
    fn bincode_roundtrips_in_order() {
        let (tx, rx) = Builder::new(MemStore::new()).open_typed(Bincode).unwrap();
        tx.push(&Job {
            id: 1,
            name: "a".into(),
        })
        .unwrap();
        tx.push(&Job {
            id: 2,
            name: "b".into(),
        })
        .unwrap();

        let first = rx.reserve().unwrap().unwrap();
        assert_eq!(
            *first,
            Job {
                id: 1,
                name: "a".into()
            }
        );
        first.ack().unwrap();

        let second = rx.reserve().unwrap().unwrap();
        assert_eq!(second.id, 2);
        second.ack().unwrap();

        assert!(rx.reserve().unwrap().is_none());
    }

    #[test]
    fn nack_redelivers_same_value_and_seq() {
        let (tx, rx) = Builder::new(MemStore::new()).open_typed(Bincode).unwrap();
        tx.push(&Job {
            id: 9,
            name: "x".into(),
        })
        .unwrap();

        let first = rx.reserve().unwrap().unwrap();
        let seq = first.seq();
        first.nack();

        let again = rx.reserve().unwrap().unwrap();
        assert_eq!(again.seq(), seq);
        assert_eq!(
            *again,
            Job {
                id: 9,
                name: "x".into()
            }
        );
        again.ack().unwrap();
    }
}

#[cfg(feature = "rkyv")]
mod rkyv_codec {
    use persistent_queue::{Builder, MemStore, Rkyv};

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Job {
        id: u64,
        name: String,
    }

    #[test]
    fn rkyv_roundtrips_in_order() {
        let (tx, rx) = Builder::new(MemStore::new()).open_typed(Rkyv).unwrap();
        tx.push(&Job {
            id: 1,
            name: "a".into(),
        })
        .unwrap();
        tx.push(&Job {
            id: 2,
            name: "b".into(),
        })
        .unwrap();

        let first = rx.reserve().unwrap().unwrap();
        assert_eq!(
            *first,
            Job {
                id: 1,
                name: "a".into()
            }
        );
        first.ack().unwrap();

        let second = rx.reserve().unwrap().unwrap();
        assert_eq!(second.id, 2);
        second.ack().unwrap();

        assert!(rx.reserve().unwrap().is_none());
    }
}
