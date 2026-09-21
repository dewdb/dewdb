# DewDB

**The distributed document database in a single binary.**

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-edition%202024-orange.svg)](Cargo.toml)
[![Protocol](https://img.shields.io/badge/protocol-HTTP%2FREST-informational.svg)](docs/documentation.md#16-rest-api-reference)

DewDB is a distributed JSON document database written in Rust, with replication, sharding, leader
election, failover, secondary indexes, aggregation and change data capture built into the process.
One binary, one JSON config file per node, local files for storage, HTTP as the protocol. There is
no external coordinator, no separate metadata service, and no driver to install.

---

## Install

### Windows, PowerShell

```powershell
iwr https://windows.dewdb.com -useb | iex
```

```powershell
dewdb --version
dewdb init
dewdb
```

This installs DewDB for the current user under `%LOCALAPPDATA%\DewDB` and adds it to your user
`PATH`, so it does not need Administrator privileges. Open a new terminal after installing to pick
up the `PATH` change.

### Windows, GitHub Releases

Releases are published at
[github.com/dewdb/dewdb/releases/latest](https://github.com/dewdb/dewdb/releases/latest).

1. Download the Windows x86_64 ZIP from the latest release.
2. Extract it.
3. Run:

```powershell
.\dewdb.exe --version
.\dewdb.exe init
.\dewdb.exe
```

Each release page carries a `SHA256SUMS.txt` if you want to verify the download before extracting.

### Build from source

```bash
cargo build --release
```

```bash
./target/release/dewdb --config examples/dew.json
```

[`examples/dew.json`](examples/dew.json) is a ready-made single-node config on port 8090. With no
`--config`, the binary reads `dew.json` in the working directory.

---

## Quickstart

`dewdb init` writes a starter `dew.json`:

```json
{
  "node_id": "n1",
  "role": "shard",
  "shard_role": "primary",
  "listen_addr": "127.0.0.1:8081",
  "data_dir": "./data"
}
```

That is all a single node needs. It leads itself, commits on its own fsync, and is ready
immediately. Collections are created on first use, with no schema and no create step.

```bash
curl -s -X PUT localhost:8081/collections/users/docs/u1 \
  -H 'content-type: application/json' \
  -d '{"value":{"name":"ada","age":36,"profile":{"city":"london"}}}'
```

```bash
curl -s --get localhost:8081/collections/users/query \
  --data-urlencode 'filter={"age":{"$gte":30},"profile.city":"london"}' \
  --data-urlencode 'sort=age:desc' \
  --data-urlencode 'fields=name,profile.city'
```

```json
{"items":[{"name":"ada","profile":{"city":"london"}}],"next_cursor":null}
```

The full walkthrough, from this node to a replicated, sharded, authenticated cluster, is in
[docs/getting_started.md](docs/getting_started.md). Every command in it runs as written.

---

## Why DewDB

**Distribution lives in the database.** DewDB keeps replication, failover, sharding, routing and
change streams inside the database process, without a separate coordinator service. Consensus,
ownership and routing run in the same process that stores the documents.

**One process, one file, one protocol.** A node is `dewdb --config dew.json`. Storage is a
directory. The wire format is HTTP and JSON, so curl, a browser, or any HTTP client is a first-class
client, and there is nothing to install on the application side.

**Strong consistency.** Writes commit by quorum and readers see committed state only. A
single-document write is atomic: it becomes durable as one frame or not at all. `read=quorum` is
linearizable at the page's starting point, established through a read index rather than assumed
from a lease.

**The API tells you what actually happened.** A write that is durable but did not meet its write
concern answers `202` with `acks` and `required` in the body, not `200`. A shard that does not own a
key answers `409` naming the real owner. An unknown `?w=`, an unparseable sort, or a cursor that
does not belong to its query is a `400` rather than a silent default.

**Permissive licence.** Apache 2.0, including for redistribution inside software you ship to
someone else's infrastructure.

---

## What you get

| Area | Capability |
|---|---|
| Data model | Schemaless JSON documents in auto-created collections, string keys |
| API | REST over HTTP: CRUD, merge patch, bulk writes, query, aggregation, collection and index admin, cluster control |
| Query | 17 field operators (equality, comparison, `$exists`, `$type`, `$prefix`/`$suffix`/`$contains`, `$all`/`$size`/`$elemMatch`, `$not`) composed with `$and`/`$or`/`$nor`, over dotted paths, with multi-key sort, projection and cursor paging |
| Indexes | Per-collection secondary indexes on dotted fields, replicated as log entries and carried across shard groups by a cluster-wide catalogue |
| Aggregation | `count`, `sum`, `avg`, `min`, `max`, grouped on dotted fields, merged across shards through the totals rather than the answers |
| Change streams | A resumable per-collection feed of committed inserts, updates, deletes and drops, filtered like a query, cluster-wide through a router |
| CDC delivery | The same feed over SSE, WebSocket, or webhooks that push signed batches with retries, capped backoff and a majority-committed delivery position |
| Durability | Append-only WAL, CRC per frame, group-commit fsync, torn-tail recovery |
| Consensus | Raft-shaped: monotonic terms, pre-vote elections, log-freshness checks, leader leases, joint-consensus membership changes |
| Replication | Leader-driven and pipelined, with tail truncation, gap repair and snapshot fallback |
| Write safety | `w=1｜majority｜all｜N` with `wtimeout`, and an explicit `202` when the concern is unmet |
| Scale-out | Consistent-hash ring or explicit ranges, online migration, optional auto-rebalancing |
| Operations | Structured logs, JSON and Prometheus metrics, health checks, background compaction |
| Security | API keys or bearer tokens, an admin tier over topology, drops and index definitions, shared secret on internal routes, hot credential reload |

---

## Replication and failover

Three nodes, each with its own port and data directory. The primary:

```json
{
  "node_id": "n1",
  "role": "shard",
  "shard_role": "primary",
  "listen_addr": "127.0.0.1:9501",
  "data_dir": "./n1",
  "replicas": ["http://127.0.0.1:9502", "http://127.0.0.1:9503"],
  "peers":    ["http://127.0.0.1:9502", "http://127.0.0.1:9503"]
}
```

A replica is the same file with `shard_role: "replica"` and `primary_addr` pointing at the leader.
Write with a quorum requirement:

```bash
curl -s -X PUT 'localhost:9501/collections/users/docs/u1?w=majority' \
  -H 'content-type: application/json' -d '{"value":{"name":"ada"}}'
```

Stop the leader. Within the heartbeat timeout plus election jitter, a survivor takes the term and
accepts writes, with no operator step and no external arbiter. A replica that fell behind repairs
from WAL frames, or takes a full collection snapshot if it fell behind further than retention.

## Sharding

A router is a node with `role: "router"`. It holds no data: it hashes `collection:key`, finds the
owning group in its cluster view, and forwards. Each group replicates and fails over independently.

```
                    ┌─► group A (leader + replicas)
client ──► router ──┤
                    └─► group B (leader + replicas)
```

Queries and aggregations fan out and merge: sorted pages are merged in order, and aggregate totals
are combined through their counts rather than by averaging averages. Adding a shard is
`POST /cluster/migrate`: keys move in paced batches while both sides keep serving.

## Change streams and CDC

```bash
curl -N --get localhost:8081/collections/users/changes \
  --data-urlencode 'filter={"profile.city":"london"}'
```

Events are published from the commit path and nowhere else, so a subscriber never sees a write a
leader change can take back. Each event carries its own position, and `?after=` resumes from it. A
position older than the buffer is refused with `410` and the floor that still works, rather than
silently skipped. The same feed is available as a WebSocket, or pushed to your endpoint as signed,
batched webhook deliveries. Their acknowledged position is majority-committed, so delivery survives
a failover.

---

## Consistency and durability

- Every mutation is a CRC-checked frame appended to a write-ahead log; the in-memory index maps a
  key to a frame location.
- A frame that is durable but not yet committed by a quorum is staged, not published. Readers and
  change streams see committed state only.
- Commit counts only entries from the current term, and membership moves through a joint
  configuration, so no two halves of a change can each reach a majority.
- Recovery truncates a torn tail rather than trusting it. A restart replays what was committed.
- Read preferences: default, `primary`, `quorum` (linearizable), `replica`.

## Scope

Current limitations:

| | |
|---|---|
| Transactions | Per document. Bulk writes are one group commit, accepted or refused whole; there are no multi-document transactions. |
| Conditional writes | Not yet. See Planned below. |
| Indexes | Single-field, by value, non-unique, up to eight per collection. A filter with no eligible index scans a key range. |
| Aggregation | `count`, `sum`, `avg`, `min`, `max` over up to four grouping fields, bounded at 10 000 groups, computed per request. |
| Joins | Done in the client. Cross-shard work is per-key or fan-out. |
| Sizes | 10 MiB per stored record, 2 MiB per request body, 10 000 documents per query page. |
| TLS and audit | Terminated and collected in front of the node today. |
| Change history | As far back as the per-collection change buffer holds. |

## Planned

- **TLS in the process.** Public and internal listeners terminating from a certificate named in the
  config, so replication does not need a proxy in front of it to be safe.
- **Conditional writes.** `If-Match` on `PUT`, `PATCH` and `DELETE` against the version a read
  returned, answered `412` when the document moved underneath it.

---

## Documentation

| | |
|---|---|
| [Getting started](docs/getting_started.md) | A walkthrough from one node to a sharded, authenticated cluster, with client snippets and troubleshooting. |
| [Your first application](docs/getting_started.md#10-your-first-application) | How a web application wires itself to DewDB: backend CRUD over `fetch`, queries that carry their ids, and browser realtime proxied through your own backend. |
| [Features](docs/features.md) | Capability by capability, each with the guarantee it gives and the bound that goes with it. |
| [Documentation](docs/documentation.md) | The reference manual: every config field, endpoint, status code, on-disk format and internal protocol. |

## Testing

```bash
cargo test
```

The suite is in-crate and runs against real nodes over real sockets: consensus and failover,
ownership handover, query and aggregation correctness against unindexed twins, change-stream resume
and webhook delivery. Benchmarks and the long-running durability scenarios (repeated crashes,
cluster churn under load, compaction interleaved with crashes, a WAL cut off mid-record) are held
back from the default run:

```bash
cargo test --release -- --ignored --nocapture bench
cargo test --release -- --ignored --nocapture soak
```

Set `DEWDB_SOAK_SEED=0x…` to run a different schedule than the fixed seed each scenario ships with.

## License

Apache License 2.0. See [LICENSE](LICENSE).
