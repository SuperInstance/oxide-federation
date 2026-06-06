# oxide-federation

Cross-cluster GPU federation with gossip health, work stealing, quorum decisions, and graceful degradation.

## Why This Exists

Single GPU clusters are easy. Multiple clusters across data centers, availability zones, or cloud providers are not. When you federate GPU resources, you face four problems simultaneously: how do nodes learn each other's state (gossip), how do you move work from overloaded to underloaded nodes (stealing), how do you make decisions without a single coordinator (quorum), and what happens when a node disappears (degradation).

The ternary health model solves the coordination problem cleanly. Each node is **Healthy** (+1, accepts work), **Recovering** (0, warming up), or **Offline** (-1, dead). Gossip propagates these states. Quorum requires a majority of healthy nodes among active voters. Work stealing respects the overload threshold. Degradation redistributes offline nodes' work automatically. No leader election, no distributed consensus protocol, no coordination service.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              FederationManager                        │
│  overload_threshold: 0.8                             │
│                                                      │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌────────┐│
│  │ gpu-0   │  │ gpu-1   │  │ gpu-2   │  │ gpu-3  ││
│  │ Healthy │  │ Healthy │  │ Recover │  │ Offline││
│  │ cap=100 │  │ cap=100 │  │ cap=50  │  │ cap=80 ││
│  │ alloc=85│  │ alloc=20│  │ alloc=10│  │alloc=0 ││
│  │ lat=1ms │  │ lat=3ms │  │ lat=5ms │  │lat=∞   ││
│  └─────────┘  └─────────┘  └─────────┘  └────────┘│
│                                                      │
│  gossip_round()        → Vec<GossipMessage>          │
│  steal_work(units)     → u32 (redistributed)         │
│  quorum_decision()     → QuorumResult                │
│  degrade_node(id)      → u32 (work moved)            │
│  route_work(units)     → Option<node_id>             │
└─────────────────────────────────────────────────────┘

Gossip Propagation:
  Node A broadcasts health → Node B, C, D
  Node B broadcasts health → Node A, C, D
  ... (N × N-1 messages per round)

Work Stealing:
  Find most overloaded healthy node (donor)
  Find underloaded healthy nodes (recipients)
  Release from donor, distribute evenly to recipients

Graceful Degradation:
  Node goes Offline → allocated work redistributed
  Recipients sorted by most available capacity first
```

**Key types:**

- `Health` — `Healthy(+1)`, `Recovering(0)`, `Offline(-1)`
- `ClusterNode` — id, health, capacity, allocation, latency
- `GossipMessage` — source, target, health, round number
- `QuorumResult` — `Approved`, `Rejected`, or `InsufficientVoters`
- `FederationManager` — the federation engine

## Usage

```rust
use oxide_federation::*;

let mut fm = FederationManager::new(0.8); // 80% overload threshold

// Register cluster nodes
fm.register_node(ClusterNode::new("us-east-0", Health::Healthy, 100, 1.2));
fm.register_node(ClusterNode::new("us-east-1", Health::Healthy, 100, 2.5));
fm.register_node(ClusterNode::new("eu-west-0", Health::Recovering, 50, 15.0));
fm.register_node(ClusterNode::new("ap-south-0", Health::Offline, 80, 200.0));

// Route work to best available node
let chosen = fm.route_work(10);
// Routes to us-east-0 or us-east-1 (lowest utilization among healthy)

// Gossip health propagation (3 rounds)
let messages = fm.gossip_n_rounds(3);
// Each node learns every other node's health

// Work stealing: redistribute from overloaded nodes
fm.get_node_mut("us-east-0").unwrap().allocate(85); // 85% = overloaded
fm.get_node_mut("us-east-1").unwrap().allocate(20); // 20% = underloaded
let stolen = fm.steal_work(30); // moves work from overloaded to underloaded

// Quorum decision
match fm.quorum_decision() {
    QuorumResult::Approved { healthy_votes, total_voters } => { /* proceed */ }
    QuorumResult::Rejected { healthy_votes, total_voters } => { /* can't act */ }
    QuorumResult::InsufficientVoters { total } => { /* not enough nodes */ }
}

// Graceful degradation when a node goes down
fm.get_node_mut("us-east-0").unwrap().allocate(60);
let redistributed = fm.degrade_node("us-east-0");
// Marks node Offline, moves 60 units to remaining healthy nodes
```

## API Reference

### `Health`

```rust
pub enum Health {
    Healthy = 1,    // Accepts work
    Recovering = 0, // Warming up
    Offline = -1,   // Dead, no work
}
```

- `from_i8(v: i8) -> Option<Self>`
- `can_accept_work() -> bool` — true for `Healthy` only

### `ClusterNode`

- `new(id, health, capacity, latency_ms) -> Self`
- `available() -> u32` / `utilization() -> f64` / `is_overloaded(threshold) -> bool`
- `allocate(units) -> bool` / `release(units)`

### `GossipMessage`

```rust
pub struct GossipMessage { pub source: String, pub target: String, pub health: Health, pub round: u32 }
```

### `QuorumResult`

```rust
pub enum QuorumResult {
    Approved { healthy_votes: usize, total_voters: usize },
    Rejected { healthy_votes: usize, total_voters: usize },
    InsufficientVoters { total: usize },
}
```

### `FederationManager`

- `new(overload_threshold: f64) -> Self`
- `register_node(node)` / `remove_node(id) -> Option<ClusterNode>`
- `get_node(id) -> Option<&ClusterNode>` / `get_node_mut(id) -> Option<&mut ClusterNode>`
- `nodes() -> Iterator` / `healthy_nodes() -> Vec<&ClusterNode>` / `health_counts() -> (healthy, recovering, offline)`
- `gossip_round() -> Vec<GossipMessage>` / `gossip_n_rounds(n) -> Vec<GossipMessage>`
- `steal_work(units) -> u32` — redistribute from most overloaded to least
- `quorum_decision() -> QuorumResult` — majority of active nodes must be healthy
- `degrade_node(id) -> u32` — mark offline, redistribute work
- `graceful_degradation() -> u32` — degrade all offline nodes with remaining work
- `route_work(units) -> Option<String>` — assign to least-utilized healthy node

## The Deeper Idea

This is the **federation layer** in the oxide stack's distributed architecture. The ternary health model (Healthy/Recovering/Offline) drives every distributed decision without requiring consensus protocols. Gossip is O(N²) per round but converges in O(log N) rounds — for typical GPU cluster sizes (4–64 nodes), one round is often sufficient.

The work stealing algorithm is deliberately simple: find the most overloaded healthy node, release work, distribute evenly to underloaded nodes sorted by lowest utilization. This avoids the complexity of two-phase commit or distributed transactions — the stealing is advisory, not atomic. If work is stolen twice in the same cycle, the worst case is temporary under-allocation, not inconsistency.

## Related Crates

- **oxide-health-monitor** — per-GPU health monitoring that feeds health signals into federation
- **oxide-capacity** — capacity planning that informs federation routing decisions
- **oxide-lease-grid** — spatial lease management for GPU resources within a federated node
- **oxide-tenancy** — multi-tenant isolation within federated cluster nodes
