//! # oxide-federation
//!
//! Multi-node GPU cluster federation with ternary health states,
//! gossip-based health propagation, work stealing, quorum decisions,
//! and graceful degradation.

use std::collections::HashMap;

/// Ternary health state for a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Health {
    Healthy = 1,
    Recovering = 0,
    Offline = -1,
}

impl Health {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            1 => Some(Health::Healthy),
            0 => Some(Health::Recovering),
            -1 => Some(Health::Offline),
            _ => None,
        }
    }

    /// Returns true if the node can accept work.
    pub fn can_accept_work(&self) -> bool {
        matches!(self, Health::Healthy)
    }
}

/// A single node in the GPU cluster.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub health: Health,
    /// Total GPU capacity (e.g., number of GPU slots).
    pub capacity: u32,
    /// Currently allocated GPU units.
    pub allocated: u32,
    /// Network latency in milliseconds.
    pub latency_ms: f64,
}

impl ClusterNode {
    pub fn new(id: impl Into<String>, health: Health, capacity: u32, latency_ms: f64) -> Self {
        Self {
            id: id.into(),
            health,
            capacity,
            allocated: 0,
            latency_ms,
        }
    }

    /// Available (free) capacity.
    pub fn available(&self) -> u32 {
        self.capacity.saturating_sub(self.allocated)
    }

    /// Utilization ratio in [0.0, 1.0+].
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        self.allocated as f64 / self.capacity as f64
    }

    /// Whether the node is overloaded (>= threshold).
    pub fn is_overloaded(&self, threshold: f64) -> bool {
        self.utilization() >= threshold
    }

    /// Allocate `units` of work. Returns true on success.
    pub fn allocate(&mut self, units: u32) -> bool {
        if units <= self.available() {
            self.allocated += units;
            true
        } else {
            false
        }
    }

    /// Release `units` of work.
    pub fn release(&mut self, units: u32) {
        self.allocated = self.allocated.saturating_sub(units);
    }
}

/// A gossip message propagating health information.
#[derive(Debug, Clone)]
pub struct GossipMessage {
    pub source: String,
    pub target: String,
    pub health: Health,
    /// Round / TTL for gossip propagation.
    pub round: u32,
}

/// Result of a quorum vote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumResult {
    /// Quorum reached with the given count of +1 (healthy) votes.
    Approved { healthy_votes: usize, total_voters: usize },
    /// Quorum not reached.
    Rejected { healthy_votes: usize, total_voters: usize },
    /// Not enough voters to form a quorum.
    InsufficientVoters { total: usize },
}

/// The federation manager tracks cluster state and routes work.
#[derive(Debug, Clone)]
pub struct FederationManager {
    nodes: HashMap<String, ClusterNode>,
    gossip_log: Vec<GossipMessage>,
    overload_threshold: f64,
}

impl FederationManager {
    pub fn new(overload_threshold: f64) -> Self {
        Self {
            nodes: HashMap::new(),
            gossip_log: Vec::new(),
            overload_threshold,
        }
    }

    /// Register a node with the federation.
    pub fn register_node(&mut self, node: ClusterNode) {
        self.nodes.insert(node.id.clone(), node);
    }

    /// Remove a node from the federation.
    pub fn remove_node(&mut self, id: &str) -> Option<ClusterNode> {
        self.nodes.remove(id)
    }

    /// Get a node by id.
    pub fn get_node(&self, id: &str) -> Option<&ClusterNode> {
        self.nodes.get(id)
    }

    /// Get a mutable reference to a node.
    pub fn get_node_mut(&mut self, id: &str) -> Option<&mut ClusterNode> {
        self.nodes.get_mut(id)
    }

    /// All registered nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &ClusterNode> {
        self.nodes.values()
    }

    /// Nodes that can accept work (healthy).
    pub fn healthy_nodes(&self) -> Vec<&ClusterNode> {
        self.nodes.values().filter(|n| n.health.can_accept_work()).collect()
    }

    /// Count nodes by health state.
    pub fn health_counts(&self) -> (usize, usize, usize) {
        let mut healthy = 0;
        let mut recovering = 0;
        let mut offline = 0;
        for n in self.nodes.values() {
            match n.health {
                Health::Healthy => healthy += 1,
                Health::Recovering => recovering += 1,
                Health::Offline => offline += 1,
            }
        }
        (healthy, recovering, offline)
    }

    // ---- Gossip protocol simulation ----

    /// Simulate one round of gossip: every node broadcasts its health
    /// to every other node. Returns the list of gossip messages generated.
    pub fn gossip_round(&mut self) -> Vec<GossipMessage> {
        let snapshots: Vec<(String, Health)> = self
            .nodes
            .values()
            .map(|n| (n.id.clone(), n.health))
            .collect();

        let mut messages = Vec::new();
        for (source_id, health) in &snapshots {
            for target_id in self.nodes.keys() {
                if source_id != target_id {
                    messages.push(GossipMessage {
                        source: source_id.clone(),
                        target: target_id.clone(),
                        health: *health,
                        round: 1,
                    });
                }
            }
        }

        self.gossip_log.extend(messages.clone());
        messages
    }

    /// Perform `n` rounds of gossip, propagating health state.
    /// In each round, nodes learn the health of all peers.
    pub fn gossip_n_rounds(&mut self, n: u32) -> Vec<GossipMessage> {
        let mut all = Vec::new();
        for round in 1..=n {
            let mut msgs = self.gossip_round();
            for m in &mut msgs {
                m.round = round;
            }
            all.extend(msgs);
        }
        all
    }

    // ---- Work stealing ----

    /// Steal work from the most overloaded node and redistribute to
    /// underloaded nodes. Returns the number of units redistributed.
    pub fn steal_work(&mut self, units: u32) -> u32 {
        // Find the most overloaded healthy node.
        let overload_threshold = self.overload_threshold;
        let donor_id = self
            .nodes
            .values()
            .filter(|n| n.health.can_accept_work() && n.is_overloaded(overload_threshold))
            .max_by(|a, b| a.utilization().partial_cmp(&b.utilization()).unwrap())
            .map(|n| n.id.clone());

        let Some(donor_id) = donor_id else {
            return 0;
        };

        let actual_units = {
            let donor = self.nodes.get(&donor_id).unwrap();
            units.min(donor.allocated)
        };

        // Find recipients: healthy and underloaded, sorted by lowest utilization.
        let recipients: Vec<String> = {
            let mut recs: Vec<_> = self
                .nodes
                .values()
                .filter(|n| {
                    n.id != donor_id
                        && n.health.can_accept_work()
                        && !n.is_overloaded(overload_threshold)
                })
                .collect();
            recs.sort_by(|a, b| a.utilization().partial_cmp(&b.utilization()).unwrap());
            recs.into_iter().map(|n| n.id.clone()).collect()
        };

        if recipients.is_empty() {
            return 0;
        }

        // Release from donor.
        if let Some(donor) = self.nodes.get_mut(&donor_id) {
            donor.release(actual_units);
        }

        // Distribute evenly across recipients.
        let per_recipient = actual_units / recipients.len() as u32;
        let remainder = actual_units % recipients.len() as u32;
        let mut distributed = 0u32;

        for (i, rid) in recipients.iter().enumerate() {
            let grant = per_recipient + if (i as u32) < remainder { 1 } else { 0 };
            if let Some(r) = self.nodes.get_mut(rid) {
                let fit = grant.min(r.available());
                r.allocate(fit);
                distributed += fit;
            }
        }

        distributed
    }

    // ---- Quorum ----

    /// Run a quorum decision: requires majority of +1 (healthy) votes
    /// among non-offline nodes.
    pub fn quorum_decision(&self) -> QuorumResult {
        let total = self.nodes.len();
        if total == 0 {
            return QuorumResult::InsufficientVoters { total: 0 };
        }

        let healthy_votes = self.nodes.values().filter(|n| n.health == Health::Healthy).count();
        let active = total - self.nodes.values().filter(|n| n.health == Health::Offline).count();

        if active == 0 {
            return QuorumResult::InsufficientVoters { total };
        }

        // Majority of active nodes must be healthy.
        let needed = active / 2 + 1;
        if healthy_votes >= needed {
            QuorumResult::Approved {
                healthy_votes,
                total_voters: active,
            }
        } else {
            QuorumResult::Rejected {
                healthy_votes,
                total_voters: active,
            }
        }
    }

    // ---- Graceful degradation ----

    /// Mark a node offline and redistribute its allocated work to
    /// remaining healthy nodes. Returns units redistributed.
    pub fn degrade_node(&mut self, node_id: &str) -> u32 {
        let work_to_redistribute = {
            let node = match self.nodes.get_mut(node_id) {
                Some(n) => n,
                None => return 0,
            };
            node.health = Health::Offline;
            let work = node.allocated;
            node.allocated = 0;
            work
        };

        if work_to_redistribute == 0 {
            return 0;
        }

        // Distribute to healthy nodes sorted by available capacity (most first).
        let recipients: Vec<String> = {
            let mut recs: Vec<_> = self
                .nodes
                .values()
                .filter(|n| n.id != node_id && n.health.can_accept_work())
                .collect();
            recs.sort_by(|a, b| b.available().cmp(&a.available()));
            recs.into_iter().map(|n| n.id.clone()).collect()
        };

        let mut remaining = work_to_redistribute;
        for rid in &recipients {
            if remaining == 0 {
                break;
            }
            if let Some(r) = self.nodes.get_mut(rid) {
                let fit = remaining.min(r.available());
                r.allocate(fit);
                remaining -= fit;
            }
        }

        work_to_redistribute - remaining
    }

    /// Graceful degradation: move all nodes with health == Offline to
    /// have zero allocation, redistributing their work.
    pub fn graceful_degradation(&mut self) -> u32 {
        let offline_ids: Vec<String> = self
            .nodes
            .values()
            .filter(|n| n.health == Health::Offline && n.allocated > 0)
            .map(|n| n.id.clone())
            .collect();

        let mut total = 0u32;
        for id in offline_ids {
            total += self.degrade_node(&id);
        }
        total
    }

    /// Route `units` of work to the best available node (lowest utilization
    /// among healthy nodes). Returns the node id chosen, or None.
    pub fn route_work(&mut self, units: u32) -> Option<String> {
        let best_id = self
            .nodes
            .values()
            .filter(|n| n.health.can_accept_work() && n.available() >= units)
            .min_by(|a, b| a.utilization().partial_cmp(&b.utilization()).unwrap())
            .map(|n| n.id.clone())?;

        if let Some(node) = self.nodes.get_mut(&best_id) {
            node.allocate(units);
        }
        Some(best_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> FederationManager {
        FederationManager::new(0.8)
    }

    fn make_cluster() -> FederationManager {
        let mut fm = FederationManager::new(0.8);
        let mut n1 = ClusterNode::new("gpu-0", Health::Healthy, 100, 1.2);
        n1.allocate(85); // overloaded at 85%
        let mut n2 = ClusterNode::new("gpu-1", Health::Healthy, 100, 2.5);
        n2.allocate(20); // 20%
        let mut n3 = ClusterNode::new("gpu-2", Health::Recovering, 50, 5.0);
        n3.allocate(10);
        let n4 = ClusterNode::new("gpu-3", Health::Offline, 80, 999.0);

        fm.register_node(n1);
        fm.register_node(n2);
        fm.register_node(n3);
        fm.register_node(n4);
        fm
    }

    #[test]
    fn test_ternary_health_values() {
        assert_eq!(Health::Healthy as i8, 1);
        assert_eq!(Health::Recovering as i8, 0);
        assert_eq!(Health::Offline as i8, -1);
        assert_eq!(Health::from_i8(1), Some(Health::Healthy));
        assert_eq!(Health::from_i8(0), Some(Health::Recovering));
        assert_eq!(Health::from_i8(-1), Some(Health::Offline));
        assert_eq!(Health::from_i8(42), None);
    }

    #[test]
    fn test_node_capacity_and_utilization() {
        let mut node = ClusterNode::new("n1", Health::Healthy, 100, 1.0);
        assert_eq!(node.available(), 100);
        assert_eq!(node.utilization(), 0.0);
        assert!(node.allocate(30));
        assert_eq!(node.available(), 70);
        assert!((node.utilization() - 0.3).abs() < 1e-9);
        assert!(!node.is_overloaded(0.8));
        assert!(node.allocate(50));
        assert!(node.is_overloaded(0.8));
    }

    #[test]
    fn test_gossip_propagation() {
        let mut fm = make_manager();
        fm.register_node(ClusterNode::new("a", Health::Healthy, 10, 1.0));
        fm.register_node(ClusterNode::new("b", Health::Recovering, 10, 2.0));
        fm.register_node(ClusterNode::new("c", Health::Offline, 10, 99.0));

        let msgs = fm.gossip_round();
        // Each node sends to every other: 3 * 2 = 6
        assert_eq!(msgs.len(), 6);
        assert!(msgs.iter().all(|m| m.round == 1));

        let msgs2 = fm.gossip_n_rounds(2);
        // 2 more rounds: 2 * 6 = 12
        assert_eq!(msgs2.len(), 12);
    }

    #[test]
    fn test_work_stealing_redistributes() {
        let mut fm = make_cluster();
        // gpu-0 is at 85% (overloaded at 0.8 threshold), gpu-1 at 20%.
        let stolen = fm.steal_work(30);
        assert!(stolen > 0);
        // gpu-0 should have released work.
        let gpu0 = fm.get_node("gpu-0").unwrap();
        assert!(gpu0.allocated < 85);
        // gpu-1 should have gained work.
        let gpu1 = fm.get_node("gpu-1").unwrap();
        assert!(gpu1.allocated > 20);
    }

    #[test]
    fn test_quorum_approved() {
        let mut fm = make_manager();
        fm.register_node(ClusterNode::new("a", Health::Healthy, 10, 1.0));
        fm.register_node(ClusterNode::new("b", Health::Healthy, 10, 1.0));
        fm.register_node(ClusterNode::new("c", Health::Healthy, 10, 1.0));
        match fm.quorum_decision() {
            QuorumResult::Approved { healthy_votes, total_voters } => {
                assert_eq!(healthy_votes, 3);
                assert_eq!(total_voters, 3);
            }
            _ => panic!("expected Approved"),
        }
    }

    #[test]
    fn test_quorum_rejected() {
        let mut fm = make_manager();
        fm.register_node(ClusterNode::new("a", Health::Healthy, 10, 1.0));
        fm.register_node(ClusterNode::new("b", Health::Recovering, 10, 1.0));
        fm.register_node(ClusterNode::new("c", Health::Offline, 10, 1.0));
        // Active = 2, need 2 healthy. Only 1 healthy => Rejected.
        match fm.quorum_decision() {
            QuorumResult::Rejected { healthy_votes, total_voters } => {
                assert_eq!(healthy_votes, 1);
                assert_eq!(total_voters, 2);
            }
            _ => panic!("expected Rejected"),
        }
    }

    #[test]
    fn test_graceful_degradation_redistributes() {
        let mut fm = make_manager();
        let mut n1 = ClusterNode::new("a", Health::Healthy, 100, 1.0);
        n1.allocate(50);
        let mut n2 = ClusterNode::new("b", Health::Healthy, 100, 2.0);
        n2.allocate(30);
        fm.register_node(n1);
        fm.register_node(n2);

        // Now mark 'a' offline with work — but degrade_node handles it.
        // First let's add a 3rd node to go offline with work.
        let mut n3 = ClusterNode::new("c", Health::Offline, 100, 5.0);
        n3.allocate(40);
        fm.register_node(n3);

        let redistributed = fm.graceful_degradation();
        assert_eq!(redistributed, 40);
        // n3 should have 0 allocated.
        assert_eq!(fm.get_node("c").unwrap().allocated, 0);
        // Work should be on healthy nodes.
        let total: u32 = fm.healthy_nodes().iter().map(|n| n.allocated).sum();
        assert_eq!(total, 120); // 50 + 30 + 40
    }

    #[test]
    fn test_degrade_node_marks_offline() {
        let mut fm = make_manager();
        let mut n1 = ClusterNode::new("a", Health::Healthy, 100, 1.0);
        n1.allocate(60);
        let mut n2 = ClusterNode::new("b", Health::Healthy, 100, 2.0);
        n2.allocate(10);
        fm.register_node(n1);
        fm.register_node(n2);

        let redist = fm.degrade_node("a");
        assert_eq!(redist, 60);
        assert_eq!(fm.get_node("a").unwrap().health, Health::Offline);
        assert_eq!(fm.get_node("a").unwrap().allocated, 0);
        // b should have picked up the work.
        assert_eq!(fm.get_node("b").unwrap().allocated, 70);
    }

    #[test]
    fn test_route_work_selects_least_utilized() {
        let mut fm = make_manager();
        let mut n1 = ClusterNode::new("a", Health::Healthy, 100, 1.0);
        n1.allocate(80);
        let mut n2 = ClusterNode::new("b", Health::Healthy, 100, 2.0);
        n2.allocate(10);
        fm.register_node(n1);
        fm.register_node(n2);

        let chosen = fm.route_work(5);
        assert_eq!(chosen, Some("b".to_string()));
        assert_eq!(fm.get_node("b").unwrap().allocated, 15);
    }
}
