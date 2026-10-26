# TinyDB design and iteration plan

TinyDB is an educational Rust key-value database. The aim is to grow a working
storage engine in small iterations, recording each iteration's behavior and
design decisions here.

## Iteration 1: durable key-value operations

**Status: implemented.**

### Scope

- Open or create a database file.
- `set`, `get`, and `delete` operations on byte keys and values.
- An interactive command-line shell using UTF-8 text for convenience.
- Rebuild the in-memory index by replaying the database journal on open.
- Sync each mutation to disk before updating the in-memory index.
- Ignore and truncate an incomplete final journal record on recovery.

### Storage format

The file is an append-only sequence of records. Each record starts with an
operation byte (`1` for put, `2` for delete), followed by a little-endian
32-bit key length. Put records also carry a little-endian 32-bit value length.
The key and then optional value bytes follow. Fields are capped at 64 MiB.
Later records override earlier records for the same key.

### Current limitations

- The index lives entirely in memory, so opening a large journal takes time and
  memory proportional to the number of live keys and journal size.
- Compaction is explicit; TinyDB does not compact automatically.
- There is no file locking or coordination between separate `Db` instances or
  processes. Concurrent access to the same database is unsupported.
- The shell accepts UTF-8 text, while the library API accepts arbitrary bytes.

## Planned iterations

### Iteration 2: log compaction

**Status: implemented.**

Add an explicit compact operation that writes the current key/value set to a
temporary journal and atomically replaces the old file. Preserve recovery if
the process stops during compaction.

The temporary journal is created beside the database, fully synced, then
renamed over the old journal. The containing directory is synced after rename.
An interrupted compaction therefore leaves either the original journal or the
complete compacted journal at the database path; an unrenamed temporary file
may remain and is ignored when opening the database.

### Iteration 3: record integrity

**Status: implemented.**

New journals begin with an eight-byte magic/version header. Each record ends
with a CRC-32 checksum over its operation, lengths, key, and value. Recovery
discards a record only when it reaches EOF before the record is complete
(including its checksum); a complete record with a bad checksum is reported as
corruption. Legacy unversioned journals are replayed and upgraded to the new
format when opened.

### Iteration 4: batch operations

**Status: implemented.**

`Db::write_batch` accepts set and delete operations inside one checksummed
batch record, syncs the journal once, and only then updates the in-memory index.
Recovery applies the batch only after reading and validating the complete
outer record and every nested record. A truncated batch at EOF is discarded as
a unit. Batches are capped at 256 MiB; each key and value retains the 64 MiB
field limit.

### Iteration 5: operational polish

**Status: implemented.**

`Db::iter` exposes borrowed key/value pairs and `Db::stats` reports live key
count, live key/value byte totals, and journal size. `DbOptions` selects
`SyncMode::EveryWrite` (the default) or `SyncMode::Manual`; in manual mode,
`flush` syncs pending mutations. The shell supports quoted arguments, keys,
stats, compaction, and flush, plus `--sync always|manual`; manual mode flushes on
normal shell exit.

Locking was reviewed and remains out of scope: the implementation has no lock
file or inter-process coordination, so concurrent access by multiple database
instances or processes is unsupported.

## Design principles

- Keep the storage format explicit and easy to inspect in code.
- Make durability behavior part of each operation's documented contract.
- Treat format changes as migrations: version them and retain recovery paths.
- Prefer a small, understandable implementation over early optimization.
