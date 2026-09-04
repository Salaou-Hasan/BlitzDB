use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Node status in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

/// A node in the cluster.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub status: NodeStatus,
    pub role: NodeRole,
    pub metadata: HashMap<String, String>,
    pub last_heartbeat: DateTime<Utc>,
    pub joined_at: DateTime<Utc>,
}

/// Role of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
    Observer,
}

impl Node {
    pub fn new(id: impl Into<String>, address: impl Into<String>, port: u16) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            name: String::new(),
            address: address.into(),
            port,
            status: NodeStatus::Alive,
            role: NodeRole::Follower,
            metadata: HashMap::new(),
            last_heartbeat: now,
            joined_at: now,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_role(mut self, role: NodeRole) -> Self {
        self.role = role;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn is_alive(&self) -> bool {
        self.status == NodeStatus::Alive
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

/// Cluster membership manager.
pub struct ClusterMembership {
    nodes: HashMap<String, Node>,
    local_id: String,
}

impl ClusterMembership {
    pub fn new(local_id: impl Into<String>) -> Self {
        Self {
            nodes: HashMap::new(),
            local_id: local_id.into(),
        }
    }

    /// Add a node to the cluster.
    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.id.clone(), node);
    }

    /// Remove a node from the cluster.
    pub fn remove_node(&mut self, id: &str) -> bool {
        self.nodes.remove(id).is_some()
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id)
    }

    /// Get the local node.
    pub fn local_node(&self) -> Option<&Node> {
        self.nodes.get(&self.local_id)
    }

    /// Get all alive nodes.
    pub fn alive_nodes(&self) -> Vec<&Node> {
        self.nodes.values().filter(|n| n.is_alive()).collect()
    }

    /// Get all nodes.
    pub fn all_nodes(&self) -> Vec<&Node> {
        self.nodes.values().collect()
    }

    /// Get the leader node.
    pub fn leader(&self) -> Option<&Node> {
        self.nodes
            .values()
            .find(|n| n.role == NodeRole::Leader && n.is_alive())
    }

    /// Get nodes by role.
    pub fn nodes_by_role(&self, role: &NodeRole) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|n| n.role == *role)
            .collect()
    }

    /// Update a node's heartbeat.
    pub fn heartbeat(&mut self, id: &str) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.last_heartbeat = Utc::now();
            node.status = NodeStatus::Alive;
            true
        } else {
            false
        }
    }

    /// Mark a node as suspect (missed heartbeats).
    pub fn mark_suspect(&mut self, id: &str) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.status = NodeStatus::Suspect;
            true
        } else {
            false
        }
    }

    /// Mark a node as dead.
    pub fn mark_dead(&mut self, id: &str) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.status = NodeStatus::Dead;
            true
        } else {
            false
        }
    }

    /// Get the number of alive nodes.
    pub fn alive_count(&self) -> usize {
        self.nodes.values().filter(|n| n.is_alive()).count()
    }

    /// Get the total number of nodes.
    pub fn total_count(&self) -> usize {
        self.nodes.len()
    }

    /// Check if quorum is available (majority of alive nodes).
    pub fn has_quorum(&self) -> bool {
        let alive = self.alive_count() as u64;
        let total = self.total_count() as u64;
        if total == 0 {
            return false;
        }
        alive > total / 2
    }

    /// Get the local node ID.
    pub fn local_id(&self) -> &str {
        &self.local_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster() -> ClusterMembership {
        let mut cluster = ClusterMembership::new("node1");
        cluster.add_node(
            Node::new("node1", "127.0.0.1", 7001)
                .with_name("Node 1")
                .with_role(NodeRole::Leader),
        );
        cluster.add_node(
            Node::new("node2", "127.0.0.1", 7002)
                .with_name("Node 2")
                .with_role(NodeRole::Follower),
        );
        cluster.add_node(
            Node::new("node3", "127.0.0.1", 7003)
                .with_name("Node 3")
                .with_role(NodeRole::Follower),
        );
        cluster
    }

    #[test]
    fn test_cluster_creation() {
        let cluster = test_cluster();
        assert_eq!(cluster.total_count(), 3);
        assert_eq!(cluster.alive_count(), 3);
        assert_eq!(cluster.local_id(), "node1");
    }

    #[test]
    fn test_node_properties() {
        let node = Node::new("n1", "10.0.0.1", 8080)
            .with_name("Test")
            .with_role(NodeRole::Leader)
            .with_metadata("zone", "us-east-1");

        assert_eq!(node.address(), "10.0.0.1:8080");
        assert!(node.is_alive());
        assert_eq!(node.role, NodeRole::Leader);
        assert_eq!(node.metadata.get("zone").unwrap(), "us-east-1");
    }

    #[test]
    fn test_leader_election() {
        let cluster = test_cluster();
        let leader = cluster.leader().unwrap();
        assert_eq!(leader.id, "node1");
    }

    #[test]
    fn test_heartbeat() {
        let mut cluster = test_cluster();
        cluster.mark_suspect("node2");
        assert_eq!(cluster.get_node("node2").unwrap().status, NodeStatus::Suspect);

        cluster.heartbeat("node2");
        assert_eq!(cluster.get_node("node2").unwrap().status, NodeStatus::Alive);
    }

    #[test]
    fn test_node_death() {
        let mut cluster = test_cluster();
        cluster.mark_dead("node3");
        assert_eq!(cluster.alive_count(), 2);
        assert!(cluster.has_quorum()); // 2/3 is majority
    }

    #[test]
    fn test_quorum() {
        let mut cluster = test_cluster();
        assert!(cluster.has_quorum()); // 3/3

        cluster.mark_dead("node3");
        assert!(cluster.has_quorum()); // 2/3 is majority

        cluster.mark_dead("node2");
        assert!(!cluster.has_quorum()); // 1/3
    }

    #[test]
    fn test_nodes_by_role() {
        let cluster = test_cluster();
        let followers = cluster.nodes_by_role(&NodeRole::Follower);
        assert_eq!(followers.len(), 2);
    }

    #[test]
    fn test_remove_node() {
        let mut cluster = test_cluster();
        assert!(cluster.remove_node("node3"));
        assert_eq!(cluster.total_count(), 2);
        assert!(!cluster.remove_node("node3"));
    }

    #[test]
    fn test_local_node() {
        let cluster = test_cluster();
        let local = cluster.local_node().unwrap();
        assert_eq!(local.id, "node1");
    }
}
