use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{WalError, WalResult};

/// Magic number to identify valid WAL files.
const WAL_MAGIC: u32 = 0x42_4C_49_54; // "BLIT"

/// Current WAL version.
const WAL_VERSION: u32 = 1;

/// Size of the WAL header in bytes.
const WAL_HEADER_SIZE: usize = 16; // magic(4) + version(4) + seq(8)

/// Size of a WAL entry header (before data).
const ENTRY_HEADER_SIZE: usize = 4 + 8 + 1 + 4 + 8 + 4 + 4; // magic + seq + type + table_len + row_id + data_len + checksum

/// Maximum WAL file size before rotation (64 MB).
const MAX_WAL_SIZE: u64 = 64 * 1024 * 1024;

/// Type of WAL entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntryType {
    /// Insert a new row.
    Insert = 1,
    /// Update an existing row.
    Update = 2,
    /// Delete a row.
    Delete = 3,
    /// Transaction committed.
    Commit = 4,
    /// Transaction rolled back.
    Rollback = 5,
}

impl EntryType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Insert),
            2 => Some(Self::Update),
            3 => Some(Self::Delete),
            4 => Some(Self::Commit),
            5 => Some(Self::Rollback),
            _ => None,
        }
    }
}

/// A single WAL entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub entry_type: EntryType,
    pub table: String,
    pub row_id: u64,
    pub data: Vec<u8>,
}

/// Write-ahead log for BlitzDB.
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
    reader: Option<File>,
    next_sequence: u64,
    current_size: u64,
}

impl WriteAheadLog {
    /// Open or create a WAL at the given path.
    pub fn open(path: impl AsRef<Path>) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let current_size = file.metadata()?.len();

        let mut wal = Self {
            path,
            writer: BufWriter::with_capacity(64 * 1024, file),
            reader: None,
            next_sequence: 1,
            current_size,
        };

        // If file is empty, write header
        if current_size == 0 {
            wal.write_header()?;
        } else {
            // Recover sequence number from existing entries
            wal.recover_sequence()?;
        }

        Ok(wal)
    }

    /// Write the WAL header.
    fn write_header(&mut self) -> WalResult<()> {
        let header = WalHeader {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            sequence: 0,
        };
        header.write_to(&mut self.writer)?;
        self.writer.flush()?;
        self.current_size += WAL_HEADER_SIZE as u64;
        Ok(())
    }

    /// Recover the next sequence number by scanning existing entries.
    fn recover_sequence(&mut self) -> WalResult<()> {
        let mut file = File::open(&self.path)?;
        let mut header = WalHeader::read_from(&mut file)?;

        if header.magic != WAL_MAGIC {
            return Err(WalError::Corruption("invalid WAL magic number".into()));
        }

        let mut max_seq = 0u64;

        loop {
            match WalEntryRaw::read_from(&mut file) {
                Ok(raw) => {
                    if raw.sequence > max_seq {
                        max_seq = raw.sequence;
                    }
                }
                Err(WalError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        self.next_sequence = max_seq + 1;
        Ok(())
    }

    /// Append an insert entry.
    pub fn append_insert(
        &mut self,
        table: &str,
        row_id: u64,
        data: &[u8],
    ) -> WalResult<u64> {
        let seq = self.next_sequence;
        self.append_entry(WalEntry {
            sequence: seq,
            entry_type: EntryType::Insert,
            table: table.to_string(),
            row_id,
            data: data.to_vec(),
        })?;
        self.next_sequence += 1;
        Ok(seq)
    }

    /// Append an update entry.
    pub fn append_update(
        &mut self,
        table: &str,
        row_id: u64,
        data: &[u8],
    ) -> WalResult<u64> {
        let seq = self.next_sequence;
        self.append_entry(WalEntry {
            sequence: seq,
            entry_type: EntryType::Update,
            table: table.to_string(),
            row_id,
            data: data.to_vec(),
        })?;
        self.next_sequence += 1;
        Ok(seq)
    }

    /// Append a delete entry.
    pub fn append_delete(&mut self, table: &str, row_id: u64) -> WalResult<u64> {
        let seq = self.next_sequence;
        self.append_entry(WalEntry {
            sequence: seq,
            entry_type: EntryType::Delete,
            table: table.to_string(),
            row_id,
            data: Vec::new(),
        })?;
        self.next_sequence += 1;
        Ok(seq)
    }

    /// Append a commit entry.
    pub fn append_commit(&mut self, tx_id: u64) -> WalResult<u64> {
        let seq = self.next_sequence;
        self.append_entry(WalEntry {
            sequence: seq,
            entry_type: EntryType::Commit,
            table: String::new(),
            row_id: tx_id,
            data: Vec::new(),
        })?;
        self.next_sequence += 1;
        Ok(seq)
    }

    /// Append a rollback entry.
    pub fn append_rollback(&mut self, tx_id: u64) -> WalResult<u64> {
        let seq = self.next_sequence;
        self.append_entry(WalEntry {
            sequence: seq,
            entry_type: EntryType::Rollback,
            table: String::new(),
            row_id: tx_id,
            data: Vec::new(),
        })?;
        self.next_sequence += 1;
        Ok(seq)
    }

    /// Append a raw entry to the WAL.
    fn append_entry(&mut self, entry: WalEntry) -> WalResult<()> {
        let raw = WalEntryRaw::from_entry(&entry);
        raw.write_to(&mut self.writer)?;
        self.writer.flush()?;

        // Sync to disk for durability
        self.writer.get_ref().sync_all()?;

        self.current_size += raw.serialized_size() as u64;
        Ok(())
    }

    /// Flush any buffered writes.
    pub fn sync(&mut self) -> WalResult<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Read all entries from the WAL (for recovery).
    pub fn read_all(&mut self) -> WalResult<Vec<WalEntry>> {
        let mut file = File::open(&self.path)?;
        let header = WalHeader::read_from(&mut file)?;

        if header.magic != WAL_MAGIC {
            return Err(WalError::Corruption("invalid WAL magic number".into()));
        }

        let mut entries = Vec::new();

        loop {
            match WalEntryRaw::read_from(&mut file) {
                Ok(raw) => {
                    entries.push(raw.to_entry()?);
                }
                Err(WalError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
    }

    /// Truncate the WAL (after snapshot).
    pub fn truncate(&mut self) -> WalResult<()> {
        // Flush and sync
        self.sync()?;

        // Reopen the file in truncate mode
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = BufWriter::with_capacity(64 * 1024, file);
        self.current_size = 0;

        // Write fresh header
        self.write_header()?;

        Ok(())
    }

    /// Get the current WAL file size.
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// Get the next sequence number.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Check if WAL needs rotation.
    pub fn needs_rotation(&self) -> bool {
        self.current_size >= MAX_WAL_SIZE
    }
}

/// WAL file header.
struct WalHeader {
    magic: u32,
    version: u32,
    sequence: u64,
}

impl WalHeader {
    fn write_to(&self, writer: &mut impl Write) -> WalResult<()> {
        writer.write_all(&self.magic.to_le_bytes())?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.sequence.to_le_bytes())?;
        Ok(())
    }

    fn read_from(reader: &mut impl Read) -> WalResult<Self> {
        let mut buf = [0u8; 4];

        reader.read_exact(&mut buf)?;
        let magic = u32::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let version = u32::from_le_bytes(buf);

        let mut seq_buf = [0u8; 8];
        reader.read_exact(&mut seq_buf)?;
        let sequence = u64::from_le_bytes(seq_buf);

        Ok(Self {
            magic,
            version,
            sequence,
        })
    }
}

/// Raw serialized WAL entry.
struct WalEntryRaw {
    magic: u32,
    sequence: u64,
    entry_type: u8,
    table: String,
    row_id: u64,
    data: Vec<u8>,
    checksum: u32,
}

impl WalEntryRaw {
    fn from_entry(entry: &WalEntry) -> Self {
        let mut raw = Self {
            magic: WAL_MAGIC,
            sequence: entry.sequence,
            entry_type: entry.entry_type as u8,
            table: entry.table.clone(),
            row_id: entry.row_id,
            data: entry.data.clone(),
            checksum: 0,
        };
        raw.checksum = raw.compute_checksum();
        raw
    }

    fn compute_checksum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.sequence.to_le_bytes());
        hasher.update(&[self.entry_type]);
        hasher.update(self.table.as_bytes());
        hasher.update(&self.row_id.to_le_bytes());
        hasher.update(&self.data);
        hasher.finalize()
    }

    fn serialized_size(&self) -> usize {
        ENTRY_HEADER_SIZE + self.table.len() + self.data.len()
    }

    fn write_to(&self, writer: &mut impl Write) -> WalResult<()> {
        writer.write_all(&self.magic.to_le_bytes())?;
        writer.write_all(&self.sequence.to_le_bytes())?;
        writer.write_all(&[self.entry_type])?;
        writer.write_all(&(self.table.len() as u32).to_le_bytes())?;
        writer.write_all(self.table.as_bytes())?;
        writer.write_all(&self.row_id.to_le_bytes())?;
        writer.write_all(&(self.data.len() as u32).to_le_bytes())?;
        writer.write_all(&self.data)?;
        writer.write_all(&self.checksum.to_le_bytes())?;
        Ok(())
    }

    fn read_from(reader: &mut impl Read) -> WalResult<Self> {
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        reader.read_exact(&mut buf4)?;
        let magic = u32::from_le_bytes(buf4);

        reader.read_exact(&mut buf8)?;
        let sequence = u64::from_le_bytes(buf8);

        let mut type_buf = [0u8; 1];
        reader.read_exact(&mut type_buf)?;
        let entry_type = type_buf[0];

        reader.read_exact(&mut buf4)?;
        let table_len = u32::from_le_bytes(buf4) as usize;

        let mut table = vec![0u8; table_len];
        reader.read_exact(&mut table)?;
        let table = String::from_utf8(table)
            .map_err(|e| WalError::Corruption(format!("invalid table name: {}", e)))?;

        reader.read_exact(&mut buf8)?;
        let row_id = u64::from_le_bytes(buf8);

        reader.read_exact(&mut buf4)?;
        let data_len = u32::from_le_bytes(buf4) as usize;

        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        reader.read_exact(&mut buf4)?;
        let checksum = u32::from_le_bytes(buf4);

        let raw = Self {
            magic,
            sequence,
            entry_type,
            table,
            row_id,
            data,
            checksum,
        };

        // Verify checksum
        let expected = raw.compute_checksum();
        if raw.checksum != expected {
            return Err(WalError::Corruption(format!(
                "checksum mismatch: expected {:#010x}, got {:#010x}",
                expected, raw.checksum
            )));
        }

        Ok(raw)
    }

    fn to_entry(self) -> WalResult<WalEntry> {
        let entry_type = EntryType::from_u8(self.entry_type)
            .ok_or_else(|| WalError::Corruption(format!("invalid entry type: {}", self.entry_type)))?;

        Ok(WalEntry {
            sequence: self.sequence,
            entry_type,
            table: self.table,
            row_id: self.row_id,
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_create_and_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WriteAheadLog::open(&path).unwrap();

        let seq = wal.append_insert("users", 1, b"hello").unwrap();
        assert_eq!(seq, 1);

        let seq = wal.append_insert("users", 2, b"world").unwrap();
        assert_eq!(seq, 2);

        let seq = wal.append_commit(1).unwrap();
        assert_eq!(seq, 3);
    }

    #[test]
    fn test_wal_read_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WriteAheadLog::open(&path).unwrap();
        wal.append_insert("users", 1, b"hello").unwrap();
        wal.append_delete("users", 1).unwrap();
        wal.append_commit(1).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].entry_type, EntryType::Insert);
        assert_eq!(entries[1].entry_type, EntryType::Delete);
        assert_eq!(entries[2].entry_type, EntryType::Commit);
    }

    #[test]
    fn test_wal_recover_sequence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Create WAL with entries
        {
            let mut wal = WriteAheadLog::open(&path).unwrap();
            wal.append_insert("users", 1, b"hello").unwrap();
            wal.append_insert("users", 2, b"world").unwrap();
        }

        // Reopen and verify sequence recovery
        let mut wal = WriteAheadLog::open(&path).unwrap();
        assert_eq!(wal.next_sequence(), 3);

        // New entries should continue from recovered sequence
        let seq = wal.append_commit(1).unwrap();
        assert_eq!(seq, 3);
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WriteAheadLog::open(&path).unwrap();
        wal.append_insert("users", 1, b"hello").unwrap();
        assert!(wal.size() > 0);

        wal.truncate().unwrap();
        assert_eq!(wal.size(), 16); // Just the header
    }

    #[test]
    fn test_wal_checksum_corruption_detection() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WriteAheadLog::open(&path).unwrap();
        wal.append_insert("users", 1, b"hello").unwrap();

        // Corrupt the file by flipping a byte in the entry data area
        drop(wal);
        let mut bytes = fs::read(&path).unwrap();
        if bytes.len() > WAL_HEADER_SIZE + 10 {
            bytes[WAL_HEADER_SIZE + 10] ^= 0xFF;
            fs::write(&path, &bytes).unwrap();
        }

        // Try to read - corruption is detected either at open or read time
        let result = WriteAheadLog::open(&path);
        if result.is_ok() {
            let mut wal = result.unwrap();
            let result = wal.read_all();
            assert!(result.is_err(), "expected corruption error from read_all");
        }
        // If open fails, that's also acceptable corruption detection
    }
}
