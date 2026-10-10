# Changelog

All notable changes are documented here. This file is managed by
[knope](https://knope.tech/) from the Conventional Commits on `main`; do not edit it by
hand.
## 0.2.0 (2026-10-10)

### Breaking Changes

- redb 4 cannot open a database written by redb 2. The
v2 on-disk format was dropped in redb 3.0; migrate an existing store
with redb 2.6's `Database::upgrade()` before upgrading. redb types also
appear in `RedbStore`'s public API, so bumping its major version is
breaking for dependents.

## 0.1.5 (2026-09-04)

### Features

- add an rkyv codec behind the rkyv feature
- add a zero-copy rkyv read path via open_archived

## 0.1.4 (2026-09-03)

### Features

- add a tokio async facade behind the tokio feature

## 0.1.3 (2026-08-30)

### Features

- add a durable_acks builder knob to relax ack fsync
- add a max_bytes bound on total unacked bytes

## 0.1.2 (2026-08-27)

### Features

- add a typed queue layer with pluggable codecs

## 0.1.1 (2026-08-25)

### Features

- implement the durable at-least-once MPSC queue
