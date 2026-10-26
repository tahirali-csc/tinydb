//! A small append-only key-value database for learning how storage engines work.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);


const PUT: u8 = 1;
const DELETE: u8 = 2;
const BATCH: u8 = 3;
const MAX_FIELD_LEN: u32 = 64 * 1024 * 1024;
const MAX_BATCH_LEN: usize = 256 * 1024 * 1024;
const HEADER: &[u8; 8] = b"TINYDB\x02\0";

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    InvalidRecord(&'static str),
    FieldTooLarge,
    BatchTooLarge,
}

/// Controls when journal writes are synced to stable storage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncMode {
    /// Sync every mutation before returning success.
    #[default]
    EveryWrite,
    /// Write mutations immediately but require [`Db::flush`] for durability.
    Manual,
}

/// Configuration used when opening a database.
#[derive(Clone, Copy, Debug, Default)]
pub struct DbOptions {
    pub sync_mode: SyncMode,
}

/// A snapshot of in-memory key/value totals and current journal size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbStats {
    pub key_count: usize,
    pub key_bytes: u64,
    pub value_bytes: u64,
    pub journal_bytes: u64,
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::InvalidRecord(message) => write!(f, "invalid database record: {message}"),
            Self::FieldTooLarge => write!(f, "key or value exceeds the 64 MiB record limit"),
            Self::BatchTooLarge => write!(f, "batch exceeds the 256 MiB batch limit"),
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self { Self::Io(error) => Some(error), _ => None }
    }
}

impl From<io::Error> for DbError {
    fn from(error: io::Error) -> Self { Self::Io(error) }
}

/// An append-only key-value store with a rebuilt in-memory key/value index.
/// Durability is controlled by [`SyncMode`].
pub struct Db {
    path: PathBuf,
    values: HashMap<Vec<u8>, Vec<u8>>,
    log: File,
    sync_mode: SyncMode,
}

impl Db {
    /// Open or create a database at `path`, replaying its journal into memory.
    /// An incomplete final record (for example, from a power loss during a
    /// write) is discarded. Corruption before the final record is an error.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_options(path, DbOptions::default())
    }

    /// Open a database with explicit durability settings.
    pub fn open_with_options(path: impl AsRef<Path>, options: DbOptions) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let mut reader = BufReader::new(OpenOptions::new().read(true).write(true).create(true).open(&path)?);
        let mut values = HashMap::new();
        let mut valid_len = 0u64;

        let mut prefix = [0u8; 8];
        let prefix_len = reader.read(&mut prefix)?;
        let mut versioned = prefix_len == HEADER.len() && &prefix == HEADER;
        let partial_header = prefix_len < HEADER.len() && prefix[..prefix_len] == HEADER[..prefix_len];
        if partial_header && prefix_len > 0 {
            // A crash while first creating the version header leaves no
            // records to preserve, so treat that partial header as empty.
            reader.get_mut().set_len(0)?;
            reader.seek(std::io::SeekFrom::Start(0))?;
            versioned = false;
        } else if prefix_len > 0 && !versioned && prefix[0] != PUT && prefix[0] != DELETE {
            return Err(DbError::InvalidRecord("unknown journal format or version"));
        }
        if versioned {
            valid_len = HEADER.len() as u64;
        } else {
            reader.seek(std::io::SeekFrom::Start(0))?;
        }

        loop {
            let result = if versioned { read_record(&mut reader) } else { read_legacy_record(&mut reader) };
            match result {
                Ok(Some(record)) => {
                    apply(&mut values, record);
                    valid_len = reader.stream_position()?;
                }
                Ok(None) => break,
                Err(ReadRecordError::Truncated) => {
                    reader.get_mut().set_len(valid_len)?;
                    break;
                }
                Err(ReadRecordError::Invalid(message)) => return Err(DbError::InvalidRecord(message)),
                Err(ReadRecordError::Io(error)) => return Err(DbError::Io(error)),
            }
        }

        // Upgrade old, unversioned journals in place after replaying them.
        // New and empty databases receive the version header immediately.
        let log = if !versioned {
            replace_with_compacted_journal(&path, &values)?
        } else {
            OpenOptions::new().append(true).read(true).open(&path)?
        };
        Ok(Self { path, values, log, sync_mode: options.sync_mode })
    }

    /// Store `value` under `key`, replacing any previous value. In
    /// [`SyncMode::EveryWrite`] mode, success means the journal was synced.
    /// In [`SyncMode::Manual`] mode, call [`Db::flush`] for that guarantee.
    pub fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), DbError> {
        let key = key.as_ref();
        let value = value.as_ref();
        check_field(key)?;
        check_field(value)?;
        write_record(&mut self.log, PUT, key, Some(value))?;
        self.sync_if_needed()?;
        self.values.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Retrieve a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<&[u8]> {
        self.values.get(key.as_ref()).map(Vec::as_slice)
    }

    /// Remove a key. Returns whether the key existed. Durability follows the
    /// selected [`SyncMode`].
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<bool, DbError> {
        let key = key.as_ref();
        check_field(key)?;
        let existed = self.values.contains_key(key);
        write_record(&mut self.log, DELETE, key, None)?;
        self.sync_if_needed()?;
        self.values.remove(key);
        Ok(existed)
    }

    /// Apply multiple mutations as one all-or-nothing batch.
    /// In [`SyncMode::EveryWrite`] mode the journal is synced once before the
    /// in-memory index changes. In [`SyncMode::Manual`] mode, call [`Db::flush`]
    /// to make the batch durable. An empty batch is a no-op.
    pub fn write_batch(&mut self, operations: &[BatchOp]) -> Result<(), DbError> {
        if operations.is_empty() { return Ok(()); }
        let mut payload = Vec::new();
        for operation in operations {
            let encoded_len = match operation {
                BatchOp::Set(key, value) => {
                    check_field(key)?;
                    check_field(value)?;
                    13usize + key.len() + value.len()
                }
                BatchOp::Delete(key) => {
                    check_field(key)?;
                    9usize + key.len()
                }
            };
            if payload.len().checked_add(encoded_len).filter(|n| *n <= MAX_BATCH_LEN).is_none() {
                return Err(DbError::BatchTooLarge);
            }
            match operation {
                BatchOp::Set(key, value) => write_record_to_writer(&mut payload, PUT, key, Some(value))?,
                BatchOp::Delete(key) => write_record_to_writer(&mut payload, DELETE, key, None)?,
            }
        }
        write_record(&mut self.log, BATCH, &payload, None)?;
        self.sync_if_needed()?;
        for operation in operations {
            match operation {
                BatchOp::Set(key, value) => { self.values.insert(key.clone(), value.clone()); }
                BatchOp::Delete(key) => { self.values.remove(key); }
            }
        }
        Ok(())
    }

    /// Sync all pending journal mutations. Required for durability when using
    /// [`SyncMode::Manual`].
    pub fn flush(&mut self) -> Result<(), DbError> {
        self.log.sync_data()?;
        Ok(())
    }

    /// Iterate over the current key/value pairs. Hash map iteration order is
    /// unspecified; returned slices borrow from this database.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.values.iter().map(|(key, value)| (key.as_slice(), value.as_slice()))
    }

    /// Return live data totals and the current on-disk journal size.
    pub fn stats(&self) -> Result<DbStats, DbError> {
        let mut stats = DbStats {
            key_count: self.values.len(),
            journal_bytes: self.path.metadata()?.len(),
            ..DbStats::default()
        };
        for (key, value) in &self.values {
            stats.key_bytes += key.len() as u64;
            stats.value_bytes += value.len() as u64;
        }
        Ok(stats)
    }

    fn sync_if_needed(&mut self) -> Result<(), DbError> {
        if self.sync_mode == SyncMode::EveryWrite { self.flush()?; }
        Ok(())
    }

    /// Rewrite the journal to contain only the current key/value pairs.
    /// The compacted file is synced before it atomically replaces the journal.
    pub fn compact(&mut self) -> Result<(), DbError> {
        let (temp_path, mut temp_file) = create_compaction_file(&self.path)?;

        let result = (|| {
            write_header(&mut temp_file)?;
            write_live_values(&mut temp_file, &self.values)?;
            temp_file.sync_all()?;

            // Keep an open handle to the new inode before replacing the old
            // path. After rename, mutations must append to this file.
            let new_log = OpenOptions::new().append(true).read(true).open(&temp_path)?;
            std::fs::rename(&temp_path, &self.path)?;
            self.log = new_log;

            // Persist the directory entry update as well as the file contents.
            let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
            File::open(parent)?.sync_all()?;
            Ok(())
        })();

        if result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
        }
        result
    }

    /// The journal file used by this database.
    pub fn path(&self) -> &Path { &self.path }
}

/// A set or delete mutation for [`Db::write_batch`].
pub enum BatchOp {
    Set(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

fn create_compaction_file(path: &Path) -> Result<(PathBuf, File), DbError> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let name = path.file_name().ok_or(DbError::InvalidRecord("database path has no file name"))?;

    loop {
        let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let mut temp_name = name.to_os_string();
        temp_name.push(format!(".{}.{}.compact", std::process::id(), id));
        let temp_path = parent.join(temp_name);
        match OpenOptions::new().write(true).read(true).create_new(true).open(&temp_path) {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(DbError::Io(error)),
        }
    }
}

fn replace_with_compacted_journal(path: &Path, values: &HashMap<Vec<u8>, Vec<u8>>) -> Result<File, DbError> {
    let (temp_path, mut temp_file) = create_compaction_file(path)?;
    let result = (|| {
        write_header(&mut temp_file)?;
        write_live_values(&mut temp_file, values)?;
        temp_file.sync_all()?;
        let log = OpenOptions::new().append(true).read(true).open(&temp_path)?;
        std::fs::rename(&temp_path, path)?;
        let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(log)
    })();
    if result.is_err() { let _ = std::fs::remove_file(&temp_path); }
    result
}

fn write_header(file: &mut File) -> Result<(), DbError> {
    file.write_all(HEADER)?;
    Ok(())
}

fn write_live_values(file: &mut File, values: &HashMap<Vec<u8>, Vec<u8>>) -> Result<(), DbError> {
    for (key, value) in values { write_record(file, PUT, key, Some(value))?; }
    Ok(())
}

fn check_field(field: &[u8]) -> Result<(), DbError> {
    if field.len() > MAX_FIELD_LEN as usize { Err(DbError::FieldTooLarge) } else { Ok(()) }
}

fn apply(values: &mut HashMap<Vec<u8>, Vec<u8>>, record: Record) {
    match record {
        Record::Put(key, value) => { values.insert(key, value); }
        Record::Delete(key) => { values.remove(&key); }
        Record::Batch(records) => for record in records { apply(values, record); },
    }
}

enum Record { Put(Vec<u8>, Vec<u8>), Delete(Vec<u8>), Batch(Vec<Record>) }
enum ReadRecordError { Truncated, Invalid(&'static str), Io(io::Error) }

// Record encoding: operation byte, key length (u32 LE), value length (u32 LE
// for PUT only), then key bytes and value bytes. DELETE has no value length.
fn write_record(file: &mut File, operation: u8, key: &[u8], value: Option<&[u8]>) -> Result<(), DbError> {
    write_record_to_writer(file, operation, key, value)
}

fn write_record_to_writer(file: &mut impl Write, operation: u8, key: &[u8], value: Option<&[u8]>) -> Result<(), DbError> {
    let op = [operation];
    let key_len = (key.len() as u32).to_le_bytes();
    let value_len = value.map(|v| (v.len() as u32).to_le_bytes());
    let mut checksum = !0u32;
    checksum = crc32_update(checksum, &op);
    checksum = crc32_update(checksum, &key_len);
    if let Some(len) = &value_len { checksum = crc32_update(checksum, len); }
    checksum = crc32_update(checksum, key);
    if let Some(value) = value { checksum = crc32_update(checksum, value); }
    file.write_all(&op)?;
    file.write_all(&key_len)?;
    if let Some(len) = &value_len { file.write_all(len)?; }
    file.write_all(key)?;
    if let Some(value) = value { file.write_all(value)?; }
    file.write_all(&(!checksum).to_le_bytes())?;
    Ok(())
}

fn read_record(reader: &mut impl Read) -> Result<Option<Record>, ReadRecordError> {
    let mut op = [0u8; 1];
    match reader.read(&mut op) {
        Ok(0) => return Ok(None),
        Ok(_) => {},
        Err(error) => return Err(ReadRecordError::Io(error)),
    }
    let operation = op[0];
    if operation != PUT && operation != DELETE && operation != BATCH { return Err(ReadRecordError::Invalid("unknown operation")); }
    let mut checksum = crc32_update(!0u32, &op);
    let key_len = read_u32_checked(reader, &mut checksum)?;
    if operation == BATCH {
        if key_len as usize > MAX_BATCH_LEN { return Err(ReadRecordError::Invalid("batch length exceeds limit")); }
        let mut payload = vec![0; key_len as usize];
        read_exact_checked(reader, &mut payload, &mut checksum)?;
        check_checksum(reader, !checksum)?;
        let mut cursor = io::Cursor::new(payload.as_slice());
        let mut records = Vec::new();
        while (cursor.position() as usize) < payload.len() {
            match read_record(&mut cursor)? {
                Some(record @ (Record::Put(_, _) | Record::Delete(_))) => records.push(record),
                _ => return Err(ReadRecordError::Invalid("invalid nested batch record")),
            }
        }
        if records.is_empty() { return Err(ReadRecordError::Invalid("empty batch record")); }
        return Ok(Some(Record::Batch(records)));
    }
    if key_len > MAX_FIELD_LEN { return Err(ReadRecordError::Invalid("key length exceeds limit")); }
    let value_len = if operation == PUT { read_u32_checked(reader, &mut checksum)? } else { 0 };
    if value_len > MAX_FIELD_LEN { return Err(ReadRecordError::Invalid("value length exceeds limit")); }
    let mut key = vec![0; key_len as usize];
    read_exact_checked(reader, &mut key, &mut checksum)?;
    if operation == DELETE {
        check_checksum(reader, !checksum)?;
        return Ok(Some(Record::Delete(key)));
    }
    let mut value = vec![0; value_len as usize];
    read_exact_checked(reader, &mut value, &mut checksum)?;
    check_checksum(reader, !checksum)?;
    Ok(Some(Record::Put(key, value)))
}

fn read_legacy_record(reader: &mut impl Read) -> Result<Option<Record>, ReadRecordError> {
    let mut op = [0u8; 1];
    match reader.read(&mut op) {
        Ok(0) => return Ok(None),
        Ok(_) => {},
        Err(error) => return Err(ReadRecordError::Io(error)),
    }
    let operation = op[0];
    if operation != PUT && operation != DELETE { return Err(ReadRecordError::Invalid("unknown operation")); }
    let key_len = read_u32(reader)?;
    if key_len > MAX_FIELD_LEN { return Err(ReadRecordError::Invalid("key length exceeds limit")); }
    let value_len = if operation == PUT { read_u32(reader)? } else { 0 };
    if value_len > MAX_FIELD_LEN { return Err(ReadRecordError::Invalid("value length exceeds limit")); }
    let mut key = vec![0; key_len as usize];
    read_exact_record(reader, &mut key)?;
    if operation == DELETE { return Ok(Some(Record::Delete(key))); }
    let mut value = vec![0; value_len as usize];
    read_exact_record(reader, &mut value)?;
    Ok(Some(Record::Put(key, value)))
}

fn read_u32_checked(reader: &mut impl Read, checksum: &mut u32) -> Result<u32, ReadRecordError> {
    let mut bytes = [0; 4];
    read_exact_record(reader, &mut bytes)?;
    *checksum = crc32_update(*checksum, &bytes);
    Ok(u32::from_le_bytes(bytes))
}

fn read_exact_checked(reader: &mut impl Read, bytes: &mut [u8], checksum: &mut u32) -> Result<(), ReadRecordError> {
    read_exact_record(reader, bytes)?;
    *checksum = crc32_update(*checksum, bytes);
    Ok(())
}

fn check_checksum(reader: &mut impl Read, expected: u32) -> Result<(), ReadRecordError> {
    let actual = read_u32(reader)?;
    if actual != expected { return Err(ReadRecordError::Invalid("record checksum mismatch")); }
    Ok(())
}

fn crc32_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 { crc = if crc & 1 != 0 { (crc >> 1) ^ 0xedb88320 } else { crc >> 1 }; }
    }
    crc
}

fn read_u32(reader: &mut impl Read) -> Result<u32, ReadRecordError> {
    let mut bytes = [0; 4];
    read_exact_record(reader, &mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_exact_record(reader: &mut impl Read, bytes: &mut [u8]) -> Result<(), ReadRecordError> {
    match reader.read_exact(bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Err(ReadRecordError::Truncated),
        Err(error) => Err(ReadRecordError::Io(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchOp, Db, DbError, DbOptions, SyncMode};
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    fn test_db_path() -> PathBuf {
        let id = super::TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("tinydb-test-{}-{id}.data", std::process::id()))
    }

    #[test]
    fn set_and_get_value() {
        let path = test_db_path();
        let mut db = Db::open(&path).unwrap();

        db.set("fruit", "apple").unwrap();

        assert_eq!(db.get("fruit"), Some(&b"apple"[..]));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn values_survive_reopening() {
        let path = test_db_path();
        {
            let mut db = Db::open(&path).unwrap();
            db.set("language", "Rust").unwrap();
        }

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get("language"), Some(&b"Rust"[..]));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn delete_removes_value() {
        let path = test_db_path();
        let mut db = Db::open(&path).unwrap();
        db.set("fruit", "apple").unwrap();

        assert!(db.delete("fruit").unwrap());
        assert_eq!(db.get("fruit"), None);
        assert!(!db.delete("fruit").unwrap());
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn batch_applies_multiple_changes() {
        let path = test_db_path();
        let mut db = Db::open(&path).unwrap();

        db.write_batch(&[
            BatchOp::Set(b"one".to_vec(), b"first".to_vec()),
            BatchOp::Set(b"two".to_vec(), b"second".to_vec()),
            BatchOp::Delete(b"one".to_vec()),
        ])
        .unwrap();

        assert_eq!(db.get("one"), None);
        assert_eq!(db.get("two"), Some(&b"second"[..]));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn setting_existing_key_replaces_value() {
        let path = test_db_path();
        let mut db = Db::open(&path).unwrap();
        db.set("color", "blue").unwrap();
        db.set("color", "green").unwrap();

        assert_eq!(db.get("color"), Some(&b"green"[..]));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn iteration_returns_all_live_pairs() {
        let path = test_db_path();
        let mut db = Db::open(&path).unwrap();
        db.set("a", "one").unwrap();
        db.set("b", "two").unwrap();

        let mut pairs: Vec<_> = db.iter().collect();
        pairs.sort_unstable_by(|left, right| left.0.cmp(right.0));
        assert_eq!(pairs, vec![(&b"a"[..], &b"one"[..]), (&b"b"[..], &b"two"[..])]);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn stats_report_live_keys_and_bytes() {
        let path = test_db_path();
        let mut db = Db::open(&path).unwrap();
        db.set("a", "123").unwrap();
        db.set("bc", "45").unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.key_count, 2);
        assert_eq!(stats.key_bytes, 3);
        assert_eq!(stats.value_bytes, 5);
        assert!(stats.journal_bytes > 0);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn compacted_database_can_be_reopened_and_written_to() {
        let path = test_db_path();
        {
            let mut db = Db::open(&path).unwrap();
            db.set("keep", "value").unwrap();
            db.set("remove", "old").unwrap();
            db.delete("remove").unwrap();
            db.compact().unwrap();
            db.set("after", "compact").unwrap();
        }

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get("keep"), Some(&b"value"[..]));
        assert_eq!(db.get("remove"), None);
        assert_eq!(db.get("after"), Some(&b"compact"[..]));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn manual_sync_mode_supports_flush_and_recovery() {
        let path = test_db_path();
        {
            let mut db = Db::open_with_options(
                &path,
                DbOptions { sync_mode: SyncMode::Manual },
            )
            .unwrap();
            db.set("saved", "after flush").unwrap();
            db.flush().unwrap();
        }

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get("saved"), Some(&b"after flush"[..]));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn incomplete_final_record_is_discarded() {
        let path = test_db_path();
        {
            let mut db = Db::open(&path).unwrap();
            db.set("good", "value").unwrap();
        }
        let original_len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[super::PUT]) // Start a record, then simulate a truncated write.
            .unwrap();

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get("good"), Some(&b"value"[..]));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_journal_is_read_and_upgraded() {
        let path = test_db_path();
        let mut legacy = Vec::new();
        legacy.push(super::PUT);
        legacy.extend_from_slice(&4u32.to_le_bytes());
        legacy.extend_from_slice(&5u32.to_le_bytes());
        legacy.extend_from_slice(b"name");
        legacy.extend_from_slice(b"Alice");
        std::fs::write(&path, legacy).unwrap();

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get("name"), Some(&b"Alice"[..]));
        drop(db);
        let rewritten = std::fs::read(&path).unwrap();
        assert!(rewritten.starts_with(super::HEADER));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn checksum_corruption_is_reported() {
        let path = test_db_path();
        {
            let mut db = Db::open(&path).unwrap();
            db.set("key", "value").unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        std::fs::write(&path, bytes).unwrap();

        assert!(matches!(Db::open(&path), Err(DbError::InvalidRecord(_))));
        std::fs::remove_file(path).unwrap();
    }
}
