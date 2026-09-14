# AnvilMQ - AGENTS.md

## Project Overview
AnvilMQ is a high-performance, lightweight, embedded-database-backed queue engine written in Rust. It is designed to run locally within regional Kubernetes clusters alongside application workloads, providing sub-millisecond gRPC ingress, crash-safe atomic transactions via libSQL, built-in tenant/department rate-limiting, and native recursion circuit-breaking.

## Architectural Principles
1. **Local Execution, Global Telemetry:** Regional instances operate as isolated, autonomous islands. Cross-region visibility and aggregation happen asynchronously out-of-band.
2. **Embedded ACID Storage:** State transitions occur via transactions against an embedded `libSQL` (SQLite) database running in-process.
3. **Upstream Safety Controls:** Recursion loops are intercepted at enqueue using ancestry tracking and facet-based rate limiting.
4. **Lock-Free Observability:** Prometheus metrics expose atomic memory counters updated during database transactions.

## Core Tech Stack
- **Language:** Rust (Edition 2021)
- **Async Runtime:** tokio
- **Transport:** gRPC via tonic and Protocol Buffers
- **Storage:** libSQL (embedded SQLite with WAL)
- **Metrics:** Prometheus atomics via embedded axum

## Directory Structure
.
├── Cargo.toml
├── build.rs
├── proto
│   └── queue.proto
└── src
    ├── main.rs
    ├── db.rs
    ├── engine.rs
    ├── rate_limit.rs
    └── telemetry.rs

## Agent Development Rules
1. **No blocking I/O** inside async Tokio task loops. All libSQL calls must be awaited.
2. **Keep transactions tight** with explicit ACID boundaries.
3. **Enforce depth checks** before persisting to block recursion loops.
4. **Maintain protobuf backward compatibility** by never renumbering tags.
