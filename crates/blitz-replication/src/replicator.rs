use blitz_types::id::RowId;
use blitz_types::value::Value;
use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use uuid::Uuid;

/// A replication operation log entry.
#[derive(Debug, Clone)]
pub struct OpEntry {
    pub id: Uuid,
    pub seq: u64,
    pub timestamp: DateTime<Utc>,
    pub op: Operation,
}

#[derive(Debug, Clone)]
pub enum Operation {
    Insert {
        table: String,
        row_id: RowId,
        data: std::collections::HashMap<String, Value>,
    },
    Update {
        table: String,
        row_id: RowId,
        data: std::collections::HashMap<String, Value>,
    },
    Delete {
        table: String,
        row_id: RowId,
    },
}

/// Replication state for a peer.
#[derive(Debug, Clone)]
pub struct PeerState {
    pub peer_id: String,
    pub last_applied_seq: u64,
    pub lag: u64,
}

/// Replicator that manages an oplog and peer sync state.
pub struct Replicator {
    oplog: VecDeque<OpEntry>,
    max_oplog_size: usize,
    next_seq: u64,
    peers: std::collections::HashMap<String, PeerState>,
}

impl Replicator {
    pub fn new() -> Self {
        Self {
            oplog: VecDeque::new(),
            max_oplog_size: 10000,
            next_seq: 1,
            peers: std::collections::HashMap::new(),
        }
    }

    /// Set the maximum oplog size.
    pub fn with_max_oplog_size(mut self, max: usize) -> Self {
        self.max_oplog_size = max;
        self
    }

    /// Append an operation to the oplog.
    pub fn append(&mut self, op: Operation) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;

        let entry = OpEntry {
            id: Uuid::new_v4(),
            seq,
            timestamp: Utc::now(),
            op,
        };

        self.oplog.push_back(entry);

        // Trim oplog if too large
        while self.oplog.len() > self.max_oplog_size {
            self.oplog.pop_front();
        }

        seq
    }

    /// Get entries since a given sequence number.
    pub fn entries_since(&self, since_seq: u64) -> Vec<&OpEntry> {
        self.oplog
            .iter()
            .filter(|e| e.seq > since_seq)
            .collect()
    }

    /// Get the latest sequence number.
    pub fn latest_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    /// Get the oplog size.
    pub fn oplog_size(&self) -> usize {
        self.oplog.len()
    }

    /// Register a peer.
    pub fn register_peer(&mut self, peer_id: impl Into<String>) {
        let peer_id = peer_id.into();
        self.peers.entry(peer_id.clone()).or_insert(PeerState {
            peer_id,
            last_applied_seq: 0,
            lag: 0,
        });
    }

    /// Update peer's last applied sequence.
    pub fn update_peer(&mut self, peer_id: &str, seq: u64) {
        let latest = self.latest_seq();
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.last_applied_seq = seq;
            peer.lag = latest.saturating_sub(seq);
        }
    }

    /// Get peer states.
    pub fn peer_states(&self) -> Vec<&PeerState> {
        self.peers.values().collect()
    }

    /// Get peer lag.
    pub fn peer_lag(&self, peer_id: &str) -> Option<u64> {
        self.peers.get(peer_id).map(|p| p.lag)
    }

    /// Remove a peer.
    pub fn remove_peer(&mut self, peer_id: &str) -> bool {
        self.peers.remove(peer_id).is_some()
    }
}

impl Default for Replicator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_replicator_append() {
        let mut repl = Replicator::new();
        let seq1 = repl.append(Operation::Insert {
            table: "users".into(),
            row_id: RowId::new(1),
            data: HashMap::new(),
        });
        let seq2 = repl.append(Operation::Delete {
            table: "users".into(),
            row_id: RowId::new(1),
        });
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(repl.latest_seq(), 2);
        assert_eq!(repl.oplog_size(), 2);
    }

    #[test]
    fn test_entries_since() {
        let mut repl = Replicator::new();
        repl.append(Operation::Insert {
            table: "t".into(),
            row_id: RowId::new(1),
            data: HashMap::new(),
        });
        repl.append(Operation::Insert {
            table: "t".into(),
            row_id: RowId::new(2),
            data: HashMap::new(),
        });
        repl.append(Operation::Insert {
            table: "t".into(),
            row_id: RowId::new(3),
            data: HashMap::new(),
        });

        let entries = repl.entries_since(1);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 2);
        assert_eq!(entries[1].seq, 3);
    }

    #[test]
    fn test_oplog_trimming() {
        let mut repl = Replicator::new().with_max_oplog_size(3);
        for i in 0..5 {
            repl.append(Operation::Insert {
                table: "t".into(),
                row_id: RowId::new(i),
                data: HashMap::new(),
            });
        }
        assert_eq!(repl.oplog_size(), 3);
        assert_eq!(repl.latest_seq(), 5);
    }

    #[test]
    fn test_peer_management() {
        let mut repl = Replicator::new();
        repl.register_peer("node1");
        repl.register_peer("node2");

        repl.append(Operation::Insert {
            table: "t".into(),
            row_id: RowId::new(1),
            data: HashMap::new(),
        });
        repl.append(Operation::Insert {
            table: "t".into(),
            row_id: RowId::new(2),
            data: HashMap::new(),
        });

        repl.update_peer("node1", 1);
        repl.update_peer("node2", 2);

        assert_eq!(repl.peer_lag("node1"), Some(1));
        assert_eq!(repl.peer_lag("node2"), Some(0));
        assert_eq!(repl.peer_states().len(), 2);
    }

    #[test]
    fn test_remove_peer() {
        let mut repl = Replicator::new();
        repl.register_peer("node1");
        assert!(repl.remove_peer("node1"));
        assert!(!repl.remove_peer("node1"));
    }
}
