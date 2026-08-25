//! Concurrency primitives, swapped for loom's under `--cfg loom` so the model
//! checker can explore every interleaving.

#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex};
