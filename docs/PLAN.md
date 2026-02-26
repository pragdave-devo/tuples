# Tuples - Development Plan

## Context

Building a tuplespace system with pattern matching capabilities. Tuples arrive, get matched against filters/triggers, and fire playbook actions.

**Playbooks** are a series of actions with access to incoming tuples. Each playbook has its own triggers. We need to:
1. Store playbook results (including tuples written by agents during execution)
2. Replay stored tuples against new playbooks

## Design Decisions & Rationale

### Filter Matching Algorithm

Filters have three levels of matching:
1. **Exact** - key/value must match literally
2. **Wildcard** - key must exist, value ignored (existence check)
3. **Predicate** - value must satisfy a function `(value) -> bool`

**Approach:** Hybrid algorithm
- Build a **discrimination tree** indexed only on exact-match keys
- Branch on `type` first (most selective discriminator)
- At leaves, store patterns with their wildcards and predicates
- On tuple arrival:
  1. Traverse tree using exact matches → candidate patterns
  2. Filter candidates: check wildcard keys exist in tuple
  3. Filter candidates: evaluate predicates

Wildcards don't go in the tree — they become post-filter existence checks (cheap hash lookups).

Predicates can't be indexed in the general case, so we evaluate them on the reduced candidate set.

### Why JSON Schema (not Protobuf)

- Schemas are created dynamically via UI — no compile step
- Richer validation (regex, ranges, cross-field constraints)
- Schema is just JSON — easy to store, edit, introspect
- Predicates can reference schema metadata ("is this field numeric?")
- If storage size matters later: encode tuple content as MessagePack/CBOR

### Why FoundationDB

- Ordered K/V enables time-ordered tuple storage (uuid7 as key)
- Range scans for replay ("tuples from time X to Y")
- Built-in clustering (don't bolt it on later)
- Flexible primitive — can model secondary indexes as needed
- Record Layer available if we want relational-ish queries later

### Tuple Structure (Required Fields)

Every tuple must have:
- `uuid7` - time-sortable unique ID
- `trace_id` - correlates related tuples across a workflow
- `created_at` - timestamp
- `type` - string, maps to schema name

### Key Schema (FDB)

```
tuples/{type}/{uuid7} → content
by_trace/{trace_id}/{uuid7} → {type}
schemas/{type_name} → schema definition
filters/{filter_id} → filter definition
```

## Architecture Summary

- Rust implementation
- gRPC/Protobuf API (shared by CLI and future GUI)
- Long-running server on port 50051
- Incremental development with working stages
- JSON Schema for user-defined schemas
- FDB for persistence (behind adapters)

## Project Structure

Working directory: `~/play/pattern_match/main` (using worktrees for feature branches)

```
main/
  Cargo.toml              # Workspace root
  proto/
    tuples.proto          # gRPC service definitions
  crates/
    proto/                # Generated protobuf code
    server/               # Main server binary (tuplesd)
    cli/                  # CLI tool (tuples)
    core/                 # Domain logic (tuples, filters, schemas)
    storage/              # Storage adapters
```

## Stage 1: Server + CLI Foundation

**Goal:** Running server with CLI that can query its version.

### Files to Create

1. **Workspace Cargo.toml**
   - Workspace members
   - Shared dependencies

2. **proto/tuples.proto**
   ```protobuf
   service Tuples {
     rpc GetVersion(Empty) returns (VersionResponse);
   }
   ```

3. **crates/proto/**
   - Build script for tonic code generation
   - Re-export generated types

4. **crates/server/**
   - Main entry point
   - gRPC server setup (tonic)
   - Version endpoint implementation (port 50051)
   - Graceful shutdown handling

5. **crates/cli/**
   - Argument parsing (clap)
   - `version` subcommand
   - gRPC client connection

### Dependencies

- `tonic` - gRPC framework
- `prost` - Protobuf
- `tokio` - Async runtime
- `clap` - CLI parsing
- `anyhow` - Error handling

### Tests

- Server unit test: version response is correct
- Integration test: CLI can connect and get version

### Verification

```bash
# Terminal 1: Start server
cargo run --bin tuplesd

# Terminal 2: Query version
cargo run --bin tuples -- version
# Expected: prints version string
```

---

## Future Stages (Overview)

### Stage 2: Schema Registry
- Define Schema type (wrapping JSON Schema)
- Storage adapter trait
- In-memory adapter (for testing)
- gRPC endpoints: register_schema, get_schema, list_schemas
- CLI commands: schema register, schema get, schema list

### Stage 3: Tuple Storage
- Tuple type with required fields (uuid7, trace_id, created_at, type)
- Tuple validation against schema
- Storage adapter for tuples
- gRPC endpoints: put_tuple, get_tuple
- CLI commands: tuple put, tuple get

### Stage 4: Filter System
- Filter type with exact/wildcard/predicate conditions
- Discrimination tree for efficient matching
- gRPC endpoints: register_filter, match_tuple (for testing)
- CLI commands: filter register, filter test

### Stage 5: FDB Integration
- FDB storage adapter implementation
- Key schema design
- Swap in FDB adapter

### Stage 6: Triggers & Playbooks
- Trigger = filter + action
- Playbook = collection of triggers + state
- Tuple arrival fires matching triggers

---

## Conventions

- No warnings (`#![deny(warnings)]` in lib.rs files)
- All public items documented
- Short functions (< 30 lines preferred)
- Small parameter lists (max 3-4, use structs for more)
- Tests alongside code (`#[cfg(test)]` modules)
- Integration tests in `tests/` directories
- Binary names: `tuplesd` (server daemon), `tuples` (CLI)

## Open Questions (for future stages)

- **Predicate types:** Are arbitrary `(value) -> bool` functions required, or can we constrain to known types (ranges, regex, set membership) for potential indexing?
- **Subset selection for replay:** What metadata will users filter on? Time range is clear; what else?
- **Action types:** What happens when a trigger fires? (Future stage, TBD)

## References

- **Rete algorithm** - Forgy 1979. Optimizes rule matching by caching partial matches. May be overkill here since we match single tuples (no joins), but the alpha network concept (single-condition tests) is relevant.
- **FoundationDB Record Layer** - Higher-level relational model on FDB if needed later.
