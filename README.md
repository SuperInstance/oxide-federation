# oxide-federation

Multi-node GPU cluster federation with ternary health states. Gossip health, work stealing, quorum decisions, graceful degradation.

## Why This Matters

# oxide-federation
Multi-node GPU cluster federation with ternary health states,
gossip-based health propagation, work stealing, quorum decisions,
and graceful degradation.

## The Five-Layer Stack

This crate is part of the **Oxide Stack** — a distributed GPU runtime built on five layers:

```
┌─────────────────┐
│  cudaclaw        │  Persistent GPU kernels, warp consensus, SmartCRDT
├─────────────────┤
│  cuda-oxide      │  Flux → MIR → Pliron → NVVM → PTX compiler
├─────────────────┤
│  flux-core       │  Bytecode VM + A2A agent protocol
├─────────────────┤
│  pincher         │  "Vector DB as runtime, LLM as compiler"
├─────────────────┤
│  open-parallel   │  Async runtime (tokio fork)
└─────────────────┘
```

The key insight: **ternary values {-1, 0, +1} map directly to GPU compute**. They pack 16× denser than FP32, enable XNOR+popcount matmul, and conservation laws become compile-time checks.

## Design

Every value in this crate follows **ternary algebra** (Z₃):

| Value | Meaning | GPU Analog |
|-------|---------|------------|
| +1 | Positive / Active / Healthy | Warp vote yes |
| 0 | Neutral / Pending / Balanced | Warp vote abstain |
| -1 | Negative / Failed / Overloaded | Warp vote no |

This isn't arbitrary — ternary is the natural encoding for:
1. **BitNet b1.58** (Microsoft) — ternary LLMs at 60% less power
2. **GPU warp voting** — hardware ballot returns ternary consensus
3. **Conservation laws** — {-1, 0, +1} preserves quantity

## Key Types

```rust
pub enum Health
pub fn from_i8
pub fn can_accept_work
pub struct ClusterNode
pub fn new
pub fn available
pub fn utilization
pub fn is_overloaded
pub fn allocate
pub fn release
pub struct GossipMessage
pub enum QuorumResult
```

## Usage

```toml
[dependencies]
oxide-federation = "0.1.0"
```

```rust
use oxide_federation::*;
// See src/lib.rs tests for complete working examples
```

## Testing

```bash
git clone https://github.com/SuperInstance/oxide-federation.git
cd oxide-federation
cargo test    # 9 tests
```

## Stats

| Metric | Value |
|--------|-------|
| Tests | 9 |
| Lines of Rust | 562 |
| Public API | 28 items |

## License

Apache-2.0
