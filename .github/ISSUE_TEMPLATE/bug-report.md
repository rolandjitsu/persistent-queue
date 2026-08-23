---
name: Bug Report
about: Report something that does not work as documented
title: "[BUG] "
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what goes wrong.

**Minimal reproducible example**
The smallest snippet that shows the problem:

```rust
use persistent_queue::{Builder, MemStore};

let (tx, rx) = Builder::new(MemStore::new()).capacity(1024).open().unwrap();
tx.push(b"job").unwrap();
let item = rx.reserve().unwrap().unwrap();
item.ack().unwrap();
```

**Expected behavior**
What you expected to happen instead.

**Environment**
- persistent-queue version:
- backend (mem / sled / redb) and its version:
- rustc version (`rustc --version`):
- OS / arch:

**Additional context**
Anything else that helps: a backtrace (`RUST_BACKTRACE=1`), whether it reproduces
under `--release`, and the `capacity` / `Durability` settings in use.
