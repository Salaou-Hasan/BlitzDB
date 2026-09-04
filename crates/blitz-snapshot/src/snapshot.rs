use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{SnapshotError, SnapshotResult};

/// Magic number for snapshot files.
const SNAPSHOT_MAGIC: u32 = 0x42_53_4E_41; // "BSNA"

/// Current snapshot version.
const SNAPSHOT_VERSION: u32 = 1;

/// A snapshot of the database state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub tables: Vec<SnapshotTable>,
    pub next_row_ids: Vec<(String, u64)>,
}

/// Snapshot of a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTable {
    pub name: String,
    pub rows: Vec<SnapshotRow>,
}

/// Snapshot of a single row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRow {
    pub id: u64,
    pub values: Vec<(String, serde_json::Value)>,
}

/// Manages snapshot creation and loading.
pub struct SnapshotManager {
    directory: PathBuf,
}

impl SnapshotManager {
    /// Create a new snapshot manager for the given directory.
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// Ensure the snapshot directory exists.
    pub fn ensure_directory(&self) -> SnapshotResult<()> {
        fs::create_dir_all(&self.directory)?;
        Ok(())
    }

    /// Create a snapshot from in-memory data.
    pub fn create_snapshot(
        &self,
        tables: &[(String, Vec<(u64, Vec<(String, serde_json::Value)>)>)],
        next_row_ids: &[(String, u64)],
        wal_sequence: u64,
    ) -> SnapshotResult<PathBuf> {
        self.ensure_directory()?;

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            tables: tables
                .iter()
                .map(|(name, rows)| SnapshotTable {
                    name: name.clone(),
                    rows: rows
                        .iter()
                        .map(|(id, values)| SnapshotRow {
                            id: *id,
                            values: values.clone(),
                        })
                        .collect(),
                })
                .collect(),
            next_row_ids: next_row_ids.to_vec(),
        };

        // Write to temporary file first
        let temp_path = self.directory.join("snapshot.tmp");
        let final_path = self.snapshot_path(wal_sequence);

        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            // Write header
            writer.write_all(&SNAPSHOT_MAGIC.to_le_bytes())?;
            writer.write_all(&SNAPSHOT_VERSION.to_le_bytes())?;
            writer.write_all(&wal_sequence.to_le_bytes())?;

            // Write serialized snapshot using JSON (bincode doesn't support serde_json::Value)
            let data = serde_json::to_vec(&snapshot)
                .map_err(|e| SnapshotError::SerializationError(e.to_string()))?;
            writer.write_all(&(data.len() as u64).to_le_bytes())?;
            writer.write_all(&data)?;
            writer.flush()?;

            // Sync to disk
            writer.get_ref().sync_all()?;
        }

        // Atomic rename
        fs::rename(&temp_path, &final_path)?;

        tracing::info!("Snapshot created at {:?}", final_path);
        Ok(final_path)
    }

    /// Load the most recent snapshot.
    pub fn load_latest(&self) -> SnapshotResult<Option<(Snapshot, u64)>> {
        let path = match self.find_latest_snapshot()? {
            Some(p) => p,
            None => return Ok(None),
        };

        let (snapshot, wal_seq) = self.load_snapshot(&path)?;
        Ok(Some((snapshot, wal_seq)))
    }

    /// Load a specific snapshot file.
    pub fn load_snapshot(&self, path: &Path) -> SnapshotResult<(Snapshot, u64)> {
        let mut file = File::open(path)?;

        // Read header
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        file.read_exact(&mut buf4)?;
        let magic = u32::from_le_bytes(buf4);
        if magic != SNAPSHOT_MAGIC {
            return Err(SnapshotError::Corruption("invalid snapshot magic".into()));
        }

        file.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        if version != SNAPSHOT_VERSION {
            return Err(SnapshotError::Corruption(format!(
                "unsupported snapshot version: {}",
                version
            )));
        }

        file.read_exact(&mut buf8)?;
        let wal_sequence = u64::from_le_bytes(buf8);

        file.read_exact(&mut buf8)?;
        let data_len = u64::from_le_bytes(buf8) as usize;

        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data)?;

        let snapshot: Snapshot = serde_json::from_slice(&data)
            .map_err(|e| SnapshotError::SerializationError(e.to_string()))?;

        Ok((snapshot, wal_sequence))
    }

    /// Find the most recent snapshot file.
    fn find_latest_snapshot(&self) -> SnapshotResult<Option<PathBuf>> {
        let mut latest: Option<(u64, PathBuf)> = None;

        if !self.directory.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) == Some("snap") {
                if let Some(seq) = self.parse_snapshot_sequence(&path) {
                    match &latest {
                        Some((best_seq, _)) if seq <= *best_seq => {}
                        _ => latest = Some((seq, path)),
                    }
                }
            }
        }

        Ok(latest.map(|(_, p)| p))
    }

    /// Parse the sequence number from a snapshot filename.
    fn parse_snapshot_sequence(&self, path: &Path) -> Option<u64> {
        let filename = path.file_stem()?.to_str()?;
        let seq_str = filename.strip_prefix("snapshot_")?;
        seq_str.parse().ok()
    }

    /// Generate the path for a snapshot with the given sequence.
    fn snapshot_path(&self, sequence: u64) -> PathBuf {
        self.directory.join(format!("snapshot_{:020}.snap", sequence))
    }

    /// List all snapshots in order.
    pub fn list_snapshots(&self) -> SnapshotResult<Vec<(u64, PathBuf)>> {
        let mut snapshots = Vec::new();

        if !self.directory.exists() {
            return Ok(snapshots);
        }

        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) == Some("snap") {
                if let Some(seq) = self.parse_snapshot_sequence(&path) {
                    snapshots.push((seq, path));
                }
            }
        }

        snapshots.sort_by_key(|(seq, _)| *seq);
        Ok(snapshots)
    }

    /// Delete old snapshots, keeping only the most recent N.
    pub fn prune(&self, keep: usize) -> SnapshotResult<usize> {
        let snapshots = self.list_snapshots()?;
        let mut deleted = 0;

        if snapshots.len() > keep {
            for (_, path) in &snapshots[..snapshots.len() - keep] {
                fs::remove_file(path)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_snapshot_create_and_load() {
        let dir = tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path());

        let tables = vec![
            (
                "users".to_string(),
                vec![
                    (
                        1u64,
                        vec![
                            ("name".to_string(), serde_json::json!("Alice")),
                            ("email".to_string(), serde_json::json!("alice@example.com")),
                        ],
                    ),
                    (
                        2u64,
                        vec![
                            ("name".to_string(), serde_json::json!("Bob")),
                            ("email".to_string(), serde_json::json!("bob@example.com")),
                        ],
                    ),
                ],
            ),
        ];

        let next_row_ids = vec![("users".to_string(), 3)];
        let path = manager.create_snapshot(&tables, &next_row_ids, 100).unwrap();

        let (snapshot, wal_seq) = manager.load_snapshot(&path).unwrap();
        assert_eq!(wal_seq, 100);
        assert_eq!(snapshot.tables.len(), 1);
        assert_eq!(snapshot.tables[0].rows.len(), 2);
    }

    #[test]
    fn test_snapshot_load_latest() {
        let dir = tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path());

        let tables = vec![("users".to_string(), vec![])];
        let next_row_ids = vec![];

        manager.create_snapshot(&tables, &next_row_ids, 10).unwrap();
        manager.create_snapshot(&tables, &next_row_ids, 20).unwrap();
        manager.create_snapshot(&tables, &next_row_ids, 30).unwrap();

        let (snapshot, wal_seq) = manager.load_latest().unwrap().unwrap();
        assert_eq!(wal_seq, 30);
    }

    #[test]
    fn test_snapshot_prune() {
        let dir = tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path());

        let tables = vec![("users".to_string(), vec![])];
        let next_row_ids = vec![];

        for i in 0..5 {
            manager.create_snapshot(&tables, &next_row_ids, i).unwrap();
        }

        let deleted = manager.prune(2).unwrap();
        assert_eq!(deleted, 3);

        let snapshots = manager.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 2);
    }
}
