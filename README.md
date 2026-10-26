# TinyDB

A small educational, disk-backed key-value database written in Rust.

TinyDB includes an append-only journal with checksums, crash recovery, batch
writes, explicit compaction, and an interactive shell. It has no external
dependencies.

## Run

Install Rust with support for the 2024 edition, then run:

```sh
cargo run -- tinydb.data
```

Example shell commands:

```text
set greeting "hello world"
get greeting
keys
stats
delete greeting
compact
quit
```

Writes sync to disk by default. Use `cargo run -- --sync manual tinydb.data`
to sync explicitly with `flush`; normal shell exit also flushes in manual mode.

## Development

```sh
cargo build
cargo test
```

Equivalent `make build`, `make test`, and `make run` commands are available.

See [DESIGN.md](DESIGN.md) for storage details and implemented iterations.
Concurrent access to one database by multiple instances or processes is unsupported.
