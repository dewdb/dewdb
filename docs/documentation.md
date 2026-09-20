# DewDB Documentation

DewDB is a distributed JSON document database written in Rust. One binary, one JSON config file per
node, local files for storage, HTTP/REST as the native protocol. No external coordinator and no
separate metadata service.

This is the reference manual: what the code does, field by field and endpoint by endpoint.

- For a hands-on walkthrough, see [getting_started.md](getting_started.md).
- For the capability catalogue, see [features.md](features.md).

---

## Contents

1. [Overview](#1-overview)
2. [Data model](#2-data-model)
3. [Building and running](#3-building-and-running)
4. [Configuration](#4-configuration)
5. [On-disk layout](#5-on-disk-layout)
6. [Storage engine](#6-storage-engine)
6a. [Secondary indexes](#6a-secondary-indexes)
7. [Durability, commit and visibility](#7-durability-commit-and-visibility)
8. [Consensus](#8-consensus)
9. [Replication](#9-replication)
10. [Snapshot transfer](#10-snapshot-transfer)
11. [Flow control](#11-flow-control)
12. [Cluster metadata](#12-cluster-metadata)
13. [Ownership: ranges and the hash ring](#13-ownership-ranges-and-the-hash-ring)
14. [Online migration and rebalancing](#14-online-migration-and-rebalancing)
15. [Routing and failover](#15-routing-and-failover)
16. [REST API reference](#16-rest-api-reference)
17. [Querying](#17-querying)
17a. [Aggregation](#17a-aggregation)
17b. [Change streams](#17b-change-streams)
17c. [Webhook delivery](#17c-webhook-delivery)
18. [Security](#18-security)
19. [Operations and observability](#19-operations-and-observability)
20. [Repository layout](#20-repository-layout)
21. [Glossary](#21-glossary)

---

## 1. Overview

A DewDB process is one node. Its `role` decides what it does:

| Role | Stores data | Serves clients | Participates in elections |
|---|---|---|---|
| `shard` | yes | yes | yes, unless it is a learner |
| `router` | no | yes, by forwarding | no |

A shard node also has a `shard_role` of `primary` or `replica`. The node that currently holds
leadership accepts writes; the others reject direct writes with `403` and serve reads.

### Deployment shapes

**Standalone shard.** One node, no peers. It leads itself and commits on its own fsync.

```
client ──► shard (leader)
```

**Replicated group.** One leader plus replicas. Writes are appended locally and streamed to the
replicas; write concern decides how many acknowledgements the client waits for. If the leader stops
answering, the replicas hold an election and one of them takes over.

```
                 ┌─► replica
client ──► leader┤
                 └─► replica
```

**Sharded cluster.** Stateless routers in front of several shard groups. A router hashes
`collection:key`, finds the owning group in its cluster view, and forwards. Each group replicates
and fails over independently.

```
                    ┌─► group A (leader + replicas)
client ──► router ──┤
                    └─► group B (leader + replicas)
```

Every node — router included — carries a versioned cluster view, so routing, membership and
ownership are the same data structure everywhere.

### Design points that shape everything else

- **The log is the database.** Every mutation is a CRC-checked frame appended to a write-ahead log.
  The in-memory index maps a key to a frame location; reads resolve through it.
- **Visibility follows commitment.** A frame that is durable but not yet committed by a quorum is
  staged, not published. Readers never see it, so a leader change can still revoke it — and neither
  does the change stream, for the stronger reason that a subscriber cannot un-see an event.
- **The durable cluster view outranks config.** Config seeds a node's first boot. After that, the
  view on disk is authoritative and travels between nodes by version.
- **A shard checks ownership itself.** Views converge rather than switching in lockstep, so a shard
  refuses keys it does not own and names the real owner instead of writing a second copy.
- **The voting set is a log entry.** Membership moves through the `_config` log as a joint
  configuration and then its target, so no two halves of a change can each reach a majority on their
  own.

---

## 2. Data model

- A **collection** is a namespace of documents, created on first use; no schema, no create step.
- A **document** is any JSON value stored under a string **key**. Objects are the usual case;
  arrays, numbers and strings are stored as given.
- `POST` generates a UUIDv4 key; `PUT` uses the key you supply.
- A write is per document: there is no cross-document transaction and no per-document version or
  compare-and-set token.
- A single document write is atomic. A bulk write is one group commit and one replication round,
  reported per document, and its write concern is decided for the whole run — a replica that
  acknowledges the last frame holds every frame below it.
- Routing hashes `collection:key` with xxh64, so two collections spread independently.
- Names beginning with `_` are reserved for system logs — `_config` carries the voting set. The
  public API refuses them with `403`, and `GET /collections` does not list them.
- Every other collection name is 1–128 characters of `a-z0-9._-`, not `.`-prefixed, not ending in
  `.`, `.tmp` or `.old`; anything else is `400`. Uppercase letters, trailing dots and Windows
  reserved device names are rejected to enforce a portable canonical identity free of filesystem
  aliases. The name is checked after percent-decoding, so `%5Fconfig` is judged as `_config`, and
  again before it becomes a directory under the data root. Keys carry no such restriction.

A document has no reserved fields and no metadata stored alongside it: what you `PUT` is what you
`GET`.

---

## 3. Building and running

Requires a Rust toolchain (edition 2024).

```bash
cargo build --release
```

Run a node by pointing it at a config file (the default is `dew.json`):

```bash
./target/release/dewdb --config dew.json
```

With no `--config`, the node reads `./dew.json`; if that file does not exist it prints

```
No dew.json found.
Run `dewdb init` to create one, or use `dewdb --config <path>`.
```

and exits 2 without starting. A `--config` path that is not there, or cannot be read, exits 2 the
same way and names the path (`Config file not found: node.json`).

A config that *is* there but that the node will not run — malformed JSON, an unknown field, or a
rule `validate` refuses — exits 3 and prints the reason:

```
Invalid config JSON format: unknown field `dta_dir`, expected one of `node_id`, … at line 1 column 71
Invalid config map constraints: role must be 'shard' or 'router', got 'banana'
```

None of these is a crash: the exit codes are `0` success, `2` no usable config, `3` a config the node
refuses, and a panic (`101`) means a bug worth reporting.

`dewdb init` writes that starter file (and refuses to replace one that is already there), so a fresh
host needs no hand-written JSON to reach a running node. The file is compiled into the binary;
nothing else has to be unpacked beside it.

`--config` is the only argument that affects a running node; everything else is configuration.
`--version` (or `-V`) and `--help` (or `-h`, or `dewdb help`) are answered before any config file is
read, so both work on a host that has none:

```bash
./target/release/dewdb --version
# dewdb 1.0.0
./target/release/dewdb --help
# usage, the default config path, and the five fields a single node needs
```

`Ctrl-C` is handled: pending group-commit waiters are fsynced and the durable LSN is persisted
before the process exits.

### Test suite

```bash
cargo test
cargo test --release -- --ignored --nocapture bench
cargo test --release -- --ignored --nocapture soak
```

The suite includes a live multi-node harness that starts real nodes on loopback ports with
temporary data directories, so replication, election, failover, migration and snapshot paths are
exercised end to end. A cluster is handed to a test once it has converged — one leader, every
follower in contact with it — rather than after a fixed wait, and contact timeouts are slack unless
a test asks for a short one, so a busy machine cannot depose the leader a test was given.

It also carries a directional link-fault table (`src/chaos.rs`, test-only and compiled out of a
release build): nodes name themselves in the `x-dew-node` header they already stamp on internal
requests, and a cut link hangs at the receiving end, so the sender fails the way a partition makes
it fail. Partitions, asymmetric cuts and added delay are injectable.

Benchmarks and soak scenarios are `#[ignore]`d and excluded from the default run.

- **Benchmarks** (`src/bench.rs`) sample each scenario several times and report the median. Write
  concern, replication cost, concurrency scaling, batches, reads, queries, quorum width over 3/5/7
  voters, routed writes over 1–8 shards, and cross-shard query with and without the sorted merge.
- **Soak scenarios** (`src/soak.rs`) run long crash-and-churn schedules against a ledger of what the
  client was actually told: repeated crashes with writes in flight, cluster churn under concurrent
  `w=majority` writes with leader-weighted kills, compaction interleaved with crashes, and a WAL cut
  off mid-record — the machine-crash half an in-process kill cannot reach, since the page cache
  survives it. A `200`/`201` is asserted on, a `202` promises nothing and is only counted, and a
  request whose answer never arrived is evidence of neither. Each has a fixed seed so a failure
  replays; `DEWDB_SOAK_SEED=0x…` asks for a different schedule.

---

## 4. Configuration

One JSON object per node. Only the first three fields are required, plus `shard_role` for a shard
that should serve as primary.

```json
{
  "node_id": "shard-1",
  "role": "shard",
  "listen_addr": "127.0.0.1:8081",
  "shard_role": "primary",

  "membership_mode": "voter",
  "primary_addr": "http://127.0.0.1:8081",
  "replicas": ["http://127.0.0.1:8083"],
  "peers": ["http://127.0.0.1:8083", "http://127.0.0.1:8084"],

  "data_dir": "./data",
  "heartbeat_timeout_secs": 6,
  "election_delay_ms": 2000,

  "shard_map": [],
  "ring": { "vnodes": 128, "shards": [] },
  "allow_unsafe_ring_changes": false,

  "flow_control": {
    "max_uncommitted_frames": 4096,
    "max_inflight_requests": 16,
    "drive_interval_ms": 500
  },
  "maintenance": {
    "enabled": true,
    "interval_secs": 60,
    "compaction_dead_ratio": 0.4,
    "compaction_min_wal_bytes": 8388608,
    "snapshot_interval_secs": 300,
    "wal_retention_bytes": 67108864
  },
  "rebalance": { "enabled": false, "interval_secs": 10, "stabilization_secs": 30 },
  "data_movement": { "batch_size": 64, "batch_delay_ms": 5 },
  "read_cache": { "inline_max_value_bytes": 512, "inline_budget_bytes": 67108864 },
  "webhooks": {
    "enabled": true,
    "max_subscriptions": 32,
    "batch_max_events": 64,
    "batch_window_ms": 200,
    "request_timeout_ms": 10000,
    "initial_backoff_ms": 500,
    "max_backoff_ms": 30000
  },
  "logging": { "level": "info", "format": "text" },
  "auth": { "internal_secret": null, "api_keys": [], "admin_keys": [], "upstream_api_key": null }
}
```

### Identity and role

| Field | Default | Meaning |
|---|---|---|
| `node_id` | — | Name used in logs, metrics, and as the author of view changes. |
| `role` | — | `shard` or `router`. |
| `listen_addr` | — | Bind address, and this node's identity in the cluster (`host:port`). |
| `shard_role` | none | `primary` or `replica`. A `primary` with no peers bootstraps as leader. |
| `membership_mode` | `voter` | A `learner` never campaigns and is counted in no quorum. |
| `data_dir` | `./data` | Where WALs, indexes and metadata live. Must not be blank. |

### Group membership

| Field | Meaning |
|---|---|
| `primary_addr` | For a replica: whose heartbeats to poll and whose snapshots to pull. |
| `replicas` | For a leader: who receives frames and whose acknowledgements count. |
| `peers` | The voting set for elections. Lists the *other* nodes, never this one. |

The quorum set is the union of `replicas` and `peers` plus this node, so the election threshold and
the commit quorum can never be computed over different sets.

### Ownership

`shard_map` (explicit hash ranges) and `ring` (consistent hashing) are mutually exclusive at boot —
setting both is rejected rather than silently resolved. A router's `shard_map` must cover the whole
64-bit space with no overlap. A ring is validated for duplicate nodes, self-replication, a primary
listed as somebody's replica, and a replica assigned to two shards.

Both are **seeds**. After the first boot the durable view in `cluster.meta` decides, and a later
config edit is logged as ignored rather than applied. Delete `cluster.meta` to re-seed from config.

### Timing and limits

| Field | Default | Effect |
|---|---|---|
| `heartbeat_timeout_secs` | 6 | Silence from the leader before a follower stands for election. Must be at least 1. |
| `election_delay_ms` | 2000 | Upper bound on the per-node jitter before requesting votes. |
| `flow_control.max_uncommitted_frames` | 4096 | Durable-but-uncommitted frames allowed per collection before writes are refused. `0` disables the bound. |
| `flow_control.max_inflight_requests` | 16 | Node-wide cap on concurrent outbound replication requests. |
| `flow_control.drive_interval_ms` | 500 | How often the leader looks for replicas that are behind. |
| `maintenance.interval_secs` | 60 | Scheduler tick for compaction and index snapshots. |
| `maintenance.compaction_dead_ratio` | 0.4 | Dead-byte fraction that triggers compaction. |
| `maintenance.compaction_min_wal_bytes` | 8 MiB | Size floor below which compaction is not worth it. |
| `maintenance.snapshot_interval_secs` | 300 | How often an index snapshot is written. |
| `maintenance.wal_retention_bytes` | 64 MiB | Ceiling on the tail compaction keeps for a replica that is behind. Past it the replica resyncs from a snapshot. |
| `rebalance.*` | disabled | Automatic ring reconciliation after membership changes. |
| `data_movement.batch_size` | 64 | Documents per handover batch (1–1024). |
| `data_movement.batch_delay_ms` | 5 | Pause between handover batches during the bulk copy. |
| `read_cache.inline_max_value_bytes` | 512 | Largest value cached inline in an index entry. Also decides whether a pinned change feed resolves a change from the index or with a WAL read. |
| `read_cache.inline_budget_bytes` | 64 MiB | Total inline cache budget per collection. |
| `changefeed.buffer_events` | 1024 | Change events kept per collection, and so how far a subscriber may fall behind or be away before its position is refused. Must be at least 1. |
| `changefeed.idle_retention_ms` | 30000 | How long a feed keeps recording after its last subscriber leaves. `0` stops as soon as it goes. |
| `changefeed.max_subscribers` | 64 | Concurrent change streams per collection. Past it, `503`. Must be at least 1. |
| `webhooks.enabled` | true | When false, registering one is `501`. |
| `webhooks.max_subscriptions` | 32 | Webhook subscriptions this node holds, across every collection. Past it, `409`. |
| `webhooks.batch_max_events` | 64 | Events per delivery. Must be at least 1. |
| `webhooks.batch_window_ms` | 200 | How long a partly filled batch waits for more before it is sent. |
| `webhooks.request_timeout_ms` | 10000 | Deadline on one delivery attempt. Must be at least 1. |
| `webhooks.initial_backoff_ms` | 500 | First retry delay after a failed delivery; doubles from there. |
| `webhooks.max_backoff_ms` | 30000 | Ceiling on that doubling. Must be at least `initial_backoff_ms`. |
| `logging.level` / `.format` | `info` / `text` | `text` or `json`; `RUST_LOG` overrides the level. |
| `auth.*` | open | Credentials — see [Security](#18-security). |

### Boot validation and warnings

Fatal at boot: a key no field matches — the whole config tree is `deny_unknown_fields`, so a typo
fails the boot instead of silently taking a default — an unknown `role` or `shard_role`, an invalid
ring or shard map, both ownership models set, a blank `data_dir`, an unknown `membership_mode`, a
learner declared `primary`, an out-of-range maintenance ratio, rebalancing enabled on a router, an
unusable log level or format, non-printable credentials, and an unreadable `cluster.meta` or
`replication.meta`.

`ShardInfo` and `HashRing` are deliberately exempt from the unknown-key check: they are also the
wire format of the cluster view, which has to survive version skew between nodes.

Logged as warnings, because they are legal but usually mistakes:

- `peers` or `replicas` containing this node's own address, which inflates the majority threshold.
- `replicas` listing a node absent from `peers`: it receives frames but cannot vote.
- `replicas` set while `peers` is empty, so this node can never be replaced by an election.
- `primary_addr` that is not present in `peers`, so it cannot be voted for.
- A router shard with no `replica_urls`, where reads and writes cannot fail over.
- A node that is a primary for one range and a replica for another.
- A lone `voter` replica with no primary and no peers: it will elect itself once its timeout
  expires, and the warning names `membership_mode: "learner"` as the fix.
- A router whose `data_dir` contains collection directories.
- A missing `auth.internal_secret`, or an empty `auth.api_keys`.
- An empty `auth.admin_keys` while `auth.api_keys` is set, and the reverse.

### Example: three-node replicated group

```json
{ "node_id": "n1", "role": "shard", "shard_role": "primary",
  "listen_addr": "127.0.0.1:9501",
  "replicas": ["http://127.0.0.1:9502", "http://127.0.0.1:9503"],
  "peers":    ["http://127.0.0.1:9502", "http://127.0.0.1:9503"] }
```

```json
{ "node_id": "n2", "role": "shard", "shard_role": "replica",
  "listen_addr": "127.0.0.1:9502",
  "primary_addr": "http://127.0.0.1:9501",
  "peers": ["http://127.0.0.1:9501", "http://127.0.0.1:9503"] }
```

### Example: router in front of two groups

```json
{ "node_id": "router-1", "role": "router", "listen_addr": "127.0.0.1:8080",
  "shard_map": [
    { "start_hash": 0, "end_hash": 9223372036854775808,
      "node_url": "http://127.0.0.1:8081", "replica_urls": ["http://127.0.0.1:8083"] },
    { "start_hash": 9223372036854775808, "end_hash": 0,
      "node_url": "http://127.0.0.1:8082", "replica_urls": ["http://127.0.0.1:8084"] }
  ] }
```

A range with `start_hash == end_hash` claims the whole space and is only valid as the single entry.

### Example: ring cluster

```json
{ "node_id": "router-1", "role": "router", "listen_addr": "127.0.0.1:8080",
  "ring": { "vnodes": 128, "shards": [
    { "node_url": "http://127.0.0.1:8081", "replica_urls": ["http://127.0.0.1:8083"] },
    { "node_url": "http://127.0.0.1:8082", "replica_urls": ["http://127.0.0.1:8084"] }
  ] } }
```

---

## 5. On-disk layout

```
data_dir/
├── cluster.meta          topology: version, members, ranges, ring, migration plan, and the
│                         cluster index catalogue
├── replication.meta      durable term, vote, and whether this node was leading
├── progress.meta         per-replica send cursors (a hint, never quorum evidence)
├── lsn.meta              highest fsynced LSN, used as a boot hint
├── migration.meta        the last finished handover phase, so a restart can still clean up
│                         (node-local; the record that survives a leader change is in `_config`)
├── webhooks.meta         local mirror of the `_webhooks` catalogue and delivery counters
├── <collection>/
│   ├── wal-00001.log     append-only frames
│   ├── wal-00002.log
│   ├── index-current.bin key → frame location snapshot, plus a replay resume point
│   ├── applied.meta      applied watermark, drop tombstone, newest committed configuration
│   │                     and handover record, and the secondary index definitions
│   └── applied.pos       the applied watermark alone, rewritten in place on every commit
├── <collection>.tmp      staged snapshot directory during install (not listed as a collection)
├── <collection>.old      previous live directory during the install swap (not listed)
└── _config/              the log the voting set travels in
```

`applied.meta` carries the facts a compacted log can no longer prove. None of `drop`, `config`,
`handover` or `index` is ever in the key index, so compaction retires their frames like any other
superseded frame; recording the outcome next to the watermark is what lets replay find them again.
It is written through an fsynced staging file, because once the frames are retired it is the only
copy.

Nothing on disk holds a secondary index's *postings*. Only the definitions are recorded, and a node
that opens a collection rebuilds the postings from the keys it just replayed. See
[Secondary indexes](#6a-secondary-indexes).

`applied.pos` is the watermark on its own, because the two values change at completely different
rates: the position moves on every commit, the three fields beside it almost never. It is two fixed
512-byte slots written alternately, each carrying a sequence number and a CRC and pre-allocated so
no write changes the file's size — one `fsync` into blocks that already exist, where rewriting
`applied.meta` costs a file create, an `fsync` on a new file and a rename. The two slots replace the
staging file: a torn write damages only the slot being written, and the other still holds the
previous position. A commit writes only this file unless `dropped`, `config`, `handover` or the
index definitions changed, in which case it writes the full record instead. Boot takes whichever of
the two is further ahead, and compaction forces a full write before retiring anything.

Commit application returns an error when neither the position write nor its full-record fallback can
persist recovery state, or when a required full-record write fails. Public writes, bulk writes and
drops answer `500`; replication refuses a successful reply, and configuration changes report a
stalled commit. The in-memory change may already be visible. A retry checks the saved position and
metadata generation, so an unchanged commit index still retries a failed save. This is an
acknowledgement boundary, not a rollback of an entry the quorum has committed.

`webhooks.meta` caches registrations and node-local delivery counters; the committed catalogue in
`_webhooks` is authoritative once present. An acknowledged position also advances through a
majority-committed keyed put in the reserved `_webhooks` system collection before the local cursor
moves. That replicated entry is the failover boundary.

`cluster.meta` and `replication.meta` are written through a staging file that is fsynced before being
renamed into place. A reader that finds only the staging file uses it, because it exists only if the
process died between the fsync and the rename. A save never lowers the version or the term.

An unreadable `replication.meta` or `cluster.meta` is a hard boot failure rather than a silent fresh
start: continuing would forget a vote already cast, or route keys by a topology the cluster has
already left. Deleting the file is the deliberate way to reset it.

---

## 6. Storage engine

### Frame format

Every record is a 40-byte header followed by a JSON payload:

| Offset | Size | Field | Purpose |
|---|---|---|---|
| 0 | 4 | `len` | Payload length. |
| 4 | 4 | `crc` | CRC32 of the payload. |
| 8 | 8 | `term` | Leadership term that produced the frame. |
| 16 | 8 | `lsn` | Database-wide log sequence number. |
| 24 | 8 | `prev_lsn` | Predecessor **in this collection**. |
| 32 | 8 | `prev_term` | That predecessor's term. |

The payload is a tagged JSON entry. Two carry a key — `{"op":"put","key":…,"value":…,"ts":…}` and
`{"op":"del","key":…,"ts":…}` — and five do not: `{"op":"barrier","ts":…}`, the no-op a leader
appends on promotion; `{"op":"drop","ts":…}`, which removes every key;
`{"op":"config","config":{"voters":[…],"outgoing":[…]},"ts":…}`, a voting set;
`{"op":"handover","handover":{"id":…,"target":{…ring…}},"ts":…}`, what this group moved in a
handover; and `{"op":"index","change":{…}}`, a secondary index definition.

Three limits govern how big a record can be, and they are one widening chain rather than three
independent numbers:

| Limit | Value | Applies to |
|---|---|---|
| `MAX_PUBLIC_BODY` | 2 MiB | Any request body on the public API. A larger one gets `413`. |
| `MAX_RECORD_SIZE` | 10 MiB | The payload of one frame. A larger append is rejected. |
| `MAX_INTERNAL_BODY` | ~13.4 MiB | `POST /internal/replicate` and `POST /internal/migrate`. |

The internal limit is `MAX_RECORD_SIZE` plus a header, base64-encoded with room for the JSON
envelope, because replication has to carry any frame the log legally holds: a frame also enters the
log from a snapshot install or a handover, and barrier, drop, config and handover entries never pass
the public limit at all. The arithmetic lives in `storage/frame.rs` and the chain is asserted at
compile time.

A handover entry names the plan and the ring it moved keys *for*, never the keys — nothing bounds
how many moved. Cleanup asks the recorded ring which of the keys the node holds are no longer its
own, and acts only while that ring is in force, so an abandoned plan is inert.

A configuration entry is the one entry that takes effect where it is **appended** rather than where
it commits, because a leader must not decide a change using the membership the change replaces.

A drop is a log entry rather than a file deletion, so it commits by quorum, survives a leader
change, and reaches a replica that was down for it. Applying one empties the index and leaves the
collection a **tombstone**: the directory and the log stay, because that log is where a lagging
replica reads the drop from. `GET /collections` hides a tombstone; replication and compaction still
see it. Writing to the name again brings the collection back, with nothing below the drop in it.

LSNs come from one database-wide counter, and the chain is per collection, so a collection's LSNs
are sparse. That is why `prev_lsn` is carried explicitly: `lsn - 1` usually belongs to another
collection, and chaining globally would fake a gap on every replica.

### Write path

0. Sample the leadership term to append under, refusing if this node holds none. Taken here rather
   than in the handler: the handler's check precedes the write gate and the key locks.
1. Serialize the entry, compute its CRC, take the collection's append lock.
2. Rotate to a new WAL file if the active one has reached 50 MiB; the outgoing file is fsynced.
3. Allocate an LSN, sample `prev_lsn` and `prev_term` under the same lock, write header and payload.
4. Stage the frame into the pending map — under the same lock as the append, so a frame is never on
   disk while invisible to both the index and the pending set.
5. Join the group commit and wait for the fsync that covers this LSN.

Writes to distinct keys proceed in parallel: key locks are striped 64 ways by xxh64 of the key. A
bulk write sorts and dedupes its stripes before locking, so two keys sharing a stripe cannot
deadlock the batch against itself.

### Group commit

Waiters queue after appending, and one background task detaches a batch before starting its fsync.
Only that batch receives the sync result, including failures; later waiters remain queued for the
next sync. Forced shutdown flushes follow the same rule and propagate sync failures to their waiters
without raising durability. The task wakes on the first waiter, on a batch of 32, or on a 5 ms tick,
so a single write pays one fsync of latency while a busy collection amortises one fsync across many.
The fsync samples the log tail under the append lock and reports exactly the tail it covered.

### Read path

A read resolves the key in the index and then either returns the value from the inline cache, or
reads the frame back from the WAL and validates the recorded length and the CRC.

Values at or below `read_cache.inline_max_value_bytes` are cached inline in the index entry up to
`inline_budget_bytes`, so small-document reads and full-collection scans do not become one random
read per key.

`inline_budget_bytes` bounds resident inline memory, not just the committed part of it. A staged
frame's inline copy is charged to the budget when the frame is appended and released when it commits
or is truncated away. `/metrics` reports the two halves separately as `cache_bytes` and
`staged_cache_bytes`.

If a frame read fails validation, the location is stale by definition — compaction remaps the index
before retiring a WAL — so the read re-resolves from the index and retries, up to three times. A
location the index still agrees with is reported as a real error.

Reads and scans run on a blocking thread pool, and each WAL file is opened four times so concurrent
readers do not serialise on a single file handle.

### Recovery

At boot each collection loads its index snapshot if there is one and replays WAL files from the
snapshot's resume point, or replays everything if there is none. Replay stops at the first frame
with an implausible length, a short payload or a failed CRC, and truncates the file there: a torn
tail is the expected shape of a crash. Frames above the applied watermark are re-staged rather than
published, so an uncommitted tail survives the restart without becoming visible.

A divergent-tail replacement first reconciles any readable index snapshot whose saved tail is above
the cut. Its map and replay offset stay intact; its tail becomes the surviving predecessor. The
atomic save must succeed before pending entries or WAL bytes are removed. A restart before the cut
still replays the old tail; one after the cut but before the replacement recovers the predecessor.
Nonempty WAL replay takes precedence over saved tail metadata, including a lower LSN or a new term
at the same LSN. Empty later WALs do not erase the recovered tail, and the snapshot retains
committed positions retired by compaction. If an older snapshot claims an uncommitted tail but no
surviving replay frame supports it, open fails with `InvalidData` rather than advertising a history
it cannot establish; restore that collection from a trusted backup or re-seed the replica.

Once the snapshot is reconciled the cut itself is recorded durably in `wal.cut` — the cut location
plus the surviving predecessor's term and LSN — before any pending entry or WAL byte moves, and the
record is removed only when every doomed frame is gone. Applying a cut is idempotent, so a failure
part-way is retried rather than recomputed. Until it completes the collection is fenced: appends
from either the leader or a replicating peer, and snapshot streaming, are refused, because the
writer's tail still names frames the log no longer has. Boot completes a recorded cut before the WAL
scan, and fails the open if it cannot reach the files. `wal.cut` is not a WAL file and is ignored by
compaction, snapshot streaming and replay. Its removal is synced to the parent directory, including
when a retry finds the marker absent; a sync failure keeps the write fence or aborts collection
open. Directory opening for this sync is best effort on Windows.

The database opens every collection before handing out any LSN, because `lsn.meta` records only the
committed prefix and a tail above it would otherwise be allocated a second time.

### Compaction

Compaction rewrites the live keys of the frozen WALs into one new file, carries forward the tail a
replica still needs, and retires the originals:

1. Refuse if a compaction or snapshot transfer is already running, or if uncommitted frames are
   pending — retiring their WAL would lose them. The pending check is repeated under the append
   lock, which is what makes it authoritative.
2. Plan what to keep (see Retention) and refuse a run that would not reclaim enough to pay for its
   own rewrite. Planning happens before the rotation, so a refusal costs nothing.
3. Under the append lock: fsync, then switch the writer to a brand-new WAL. Writes continue there
   throughout; nothing is paused.
4. Copy the live frames into a temporary file, byte for byte with their original headers, then the
   retained tail after them, and fsync it.
5. Remap the index entries that still point at the copied locations. An entry overwritten in the
   meantime keeps the newer value. Keys inside the tail move by position instead.
6. Publish the retirement watermark under the index write lock, save an index snapshot, drain the
   read-handle pool, then unlink the frozen files.

The index snapshot is the pivot. Until it lands, the previous snapshot plus the intact frozen WALs
still describe the collection; once it lands, a failed unlink is wasted disk rather than a
resurrected key. Files that cannot be unlinked are counted and logged.

#### Retention

A run is given a floor at the lowest position any replication target has acknowledged for the
collection, and the frames above that floor are copied into the compacted output rather than
dropped — so a replica a few frames behind repairs from frames rather than paying a full-collection
snapshot. A node with no replication target keeps nothing.

The tail is bounded by `maintenance.wal_retention_bytes`. A target below what fits resyncs from a
snapshot. The bound gives up the oldest of the tail first — whole files, then frames — so what is
kept is always the contiguous newest end, the only shape a repair can chain over.

Retention is also why the scheduled path refuses a run reclaiming less than
`compaction_min_wal_bytes`: a pinned tail is superseded frames, so it holds `dead_ratio` above the
threshold by its own existence, and a replica stuck at one position would otherwise have the same
bytes rewritten every tick. `POST /collections/:name/compact` runs the rewrite regardless.

The retained tail goes *after* the relocated keys, which are all below the floor by construction, so
the compacted WAL comes out LSN-ordered. That is the invariant the next run reads: LSNs rise across
WAL ids and within each file, so the frames above any position are one contiguous byte range at the
end of the frozen set, and the next boundary is one walk away.

A replica below the floor finds a broken chain and takes a snapshot — which is why compaction runs
on the leader only, both on the scheduled path and through `POST /collections/:name/compact`.

### Background maintenance

The scheduler wakes every `interval_secs` and, per collection:

- compacts when this node is the leader, the WAL is at least `compaction_min_wal_bytes`, the
  dead-byte ratio is at least `compaction_dead_ratio`, and the run would reclaim at least
  `compaction_min_wal_bytes` after retention takes its share;
- saves an index snapshot when `snapshot_interval_secs` has passed or a compaction just ran, and
  skips it when nothing has been appended since the last one.

Snapshots are never gated on leadership: they only add a file, and a replica that never snapshots
replays every WAL at boot.

---

## 6a. Secondary indexes

A secondary index maps a dotted field's values to the keys holding them, so a filter on that field
reads the documents it might match instead of every document in the range.

It has two halves with different durability, and the split is the whole design:

| Half | Where it lives | Why |
|---|---|---|
| The **definition** — name and field | `LogEntry::Index`, and `applied.meta` beside the watermark | It is cluster state: it has to commit under a quorum, survive a leader change, reach a replica that was down for it, and outlive the compaction that retires its frame. |
| The **postings** — value → keys, and the reverse map | Memory only, rebuilt when the collection opens | They are derived from committed keys. Nothing persisted means nothing that can disagree with the documents, whatever order a crash interrupted. |

### The definition path

`POST /collections/:name/indexes` appends a `LogEntry::Index` on the leader and returns when the
entry meets its write concern, exactly as a collection drop does. Replicas receive it on the
ordinary replication path.

Two moments matter and they are not the same one:

- **Appended** is when the definition comes into force. Every entry appended above it stages the
  values that index asks for, computed where the document is still in hand, so committing a write
  later costs no read.
- **Committed** is when the index is registered and its build starts. The build walks what is
  committed *below* the entry — which, because entries drain in LSN order, is exactly the set the
  staged values do not cover.

Together those two mean no key is missed and none is filed twice. A truncation that removes a staged
`Index` entry removes the definition with it, because the definitions in force are read as the
committed set with the staged changes laid over it.

A committed collection drop clears the definitions along with the documents.

### The build

A newly registered index is `building`: maintained by every write, and selected by the planner once
it finishes, so a query is never answered from a half-filled index.

The build walks the key range in chunks, reads the documents *without* the index lock, and files
them under it. A write landing in between wins, always: the index records which keys live writes
touched during the build, and the walk skips them, because what the walk holds for such a key is the
value that write already replaced. That is checked under the same lock the write takes.

A build runs per collection on its own task, so it does not sit on the commit path, and a restart
mid-build starts over — there is nothing partial on disk to reconcile.

### Maintenance

Every path that changes a key meets at one place: the commit that publishes it into the key index.
`apply_committed` updates the postings under the same write lock, so no reader can see a key without
its postings or a posting without its key. That covers create, replace, merge-patch, delete, bulk
write, a replica applying a leader's frame, and boot replay, without any of them knowing an index
exists.

Compaction needs no index work at all: it moves frames, and a posting names a key.

A document that does not have the indexed path is not filed, which is sound because a filter
condition cannot match a field a document does not have. An explicit `null` is a value and is filed.
A write that removes the field removes the posting.

### Planning

For each condition in the filter naming an indexed, ready field:

| Condition | Candidates |
|---|---|
| A literal — `{"age": 30}`, including object and array literals | The keys filed under exactly that value. |
| `$in` | The union over its members. |
| `$gt` / `$gte` / `$lt` / `$lte` | The keys in that band, clamped to numbers, since all booleans sort below every number and all strings above. |
| `$prefix` | The strings from the prefix up to its successor. |
| `$type` with one name | That type's whole band. |
| `$exists: true` | Every posting, which the selectivity gate then judges. |
| `$ne`, `$nin`, `$contains`, `$suffix`, `$all`, `$size`, `$elemMatch`, `$not`, or a field with no index | Nothing; the condition is left to the filter. |

The narrowest of the eligible plans wins; a tie goes to the lower index name, so two shards asked
the same question plan the same way. The planner falls back to the full scan when the candidate set
is more than half the collection and larger than 64 keys.

Whatever it chooses, the caller re-applies the **whole** filter to every candidate it reads. An
index therefore changes which keys are read, never which rows come back; the test suite asserts that
by running each filter against a second, unindexed collection and comparing.

Candidates arrive key-ordered and deduplicated, so `start`, `end` and an unsorted cursor apply on
top of them exactly as they apply to a scan, and a page resumes where the scan would have. A sorted
page uses an index for its *filter* only.

Only conditions reachable through `$and` are eligible: a branch of an `$or` constrains nothing on
its own, so planning on one would drop the rows the other branches match. Where several operators
sit in one condition, the narrowest is probed and the rest are left to the filter — probing a subset
of an AND returns a superset, which is what the re-applied filter trims.

### Bounds

| Bound | Value |
|---|---|
| Indexes per collection | 8 |
| Index name | 1–64 bytes of `A-Za-z0-9_-` |
| Field path | 1–256 bytes, at most 16 dot-separated segments, no segment empty or `$`-prefixed |
| Collections in the catalogue | 4096 |

Indexes are single-field, non-unique, and by value: an array is filed as one value rather than
element-wise.

### Across shard groups: the catalogue

`LogEntry::Index` is a per-collection log, and a collection exists on a shard group only once a key
for it lands there. That makes the log the right home for the definition and the wrong home for the
*fact that the collection has one*: a router fans a create out to every group that holds the
collection, a group holding none of it answers `404`, and a group that takes its first key
afterwards would have no definition.

So `ClusterMetadata` carries a third thing beside members and the ring: an **index catalogue**, one
entry per collection.

| Field | Meaning |
|---|---|
| `version` | Bumped on every change to *this collection's* entry. |
| `updated_by` | The node that made the change; the tiebreak at equal versions. |
| `indexes` | The definitions the collection should have, in full. |

Three properties, and each is doing work:

- **It is merged, not replaced.** When two views meet, the newer entry per collection wins, in both
  directions, whichever view won on topology. A view that loses on version still hands over a
  definition the winner lacks, and a view that wins does not take away definitions it never had.
- **It is versioned per collection, and recording one moves no topology version.** A schema change
  is not a routing decision.
- **A collection the catalogue does not name is left alone.** A dropped collection keeps an
  *emptied* entry rather than losing it, so a node that missed the drop cannot win the merge and put
  the definitions back.
- **An entry no node can act on is dropped, not a reason to refuse the view.** The per-entry rules —
  collection name, index count, index names, field paths — are checked by `unaddressable_entry`, and
  `sanitize_catalog` removes what fails them wherever a catalogue arrives from disk or off the wire,
  before the version comparison and before the fingerprint. Those rules tighten between releases, so
  refusing the document would let one stale entry take a whole node's routing view down during a
  rolling upgrade. Each drop is logged at `warn`. The one catalogue rule that still fails the
  document is the 4096-collection bound.

It travels by gossip rather than by publication, because publication is version-ordered and shards
handed their topology by config never adopt anyone's view. Every node, router included, exchanges
`/internal/cluster` with the peers it can name once every `CATALOG_SYNC_INTERVAL_SECS` (3), pulling
their catalogue and handing back its own when a fingerprint differs.

The same round carries the **topology** half of a peer's view, in both directions, and it closes the
one gap no other poller reaches: `router_probe_task` runs on routers only, `heartbeat_poll_task`
runs on a node that is not leader and polls its own primary, and a leader's `leader_contact_task`
probes only its own replicas — so two shard leaders would otherwise have no poller between them. The
two halves stay independent: a view is adopted only if it wins the order, and its catalogue is
merged whoever won, so a shard still on a config seed keeps learning definitions without ever
adopting a topology.

### Reconciliation

On the same tick, a shard **leader** compares each catalogue entry against `active_index_specs()`
for that collection and closes the gap through `local_index_change` — the ordinary quorum append.
Definitions are matched on name *and* field, so a redefinition made elsewhere is a create here.

That covers every way a group's ownership can change: a new shard, a migration, a rebalance, a
restart. It is a loop rather than an event, so nothing has to remember to call it, and a group in
step does no work.

The write it issues and any admin request for the same collection are serialized on one lock, and
both record the catalogue *before* appending to the log. So the log can only be behind the
catalogue, never ahead of it — a definition present in a group's log and missing from the catalogue
is exactly what a dropped index looks like.

While a group is missing or rebuilding an index, its half of a query is a scan: the same rows, more
slowly, never a partial answer. `GET /collections/:name/indexes` on a router reports an index as
`building` when any group that answered is still building it *or does not hold it at all*.

Every group must return a valid index listing or `404` before merging. The router tries the
effective primary, original primary, then replicas, once per distinct candidate. Exhausting a group
returns `502` naming it, including on invalid JSON, a missing `indexes` array, or an error status. A
`404` excludes that group from counts and readiness; all groups absent returns `404`. Replica
fallback reads its local catalogue and can lag; this endpoint does not use quorum reads.

The catalogue converges deterministically rather than being agreed, which is the property
`ClusterMetadata::supersedes` already has: two definitions of the same index made at the same moment
on different nodes settle on one of the two.

---

## 7. Durability, commit and visibility

Three watermarks, deliberately distinct:

| Watermark | Meaning |
|---|---|
| **Durable LSN** | fsynced on this node |
| **Commit index** | held by a quorum, tracked per collection |
| **Applied LSN** | reflected in the index, and therefore readable |

A write becomes durable, then committed, then visible. Until it is committed it lives in the pending
map keyed by LSN; applying a commit watermark drains that map in log order into the index and
persists the new applied watermark.

This is what makes an uncommitted write revocable. A leader that appends, fsyncs, and then loses
leadership before reaching a quorum leaves entries no client ever read, and the new leader's history
wins. The rule survives restarts: the watermark records how far the index reflects the log, so boot
replays the whole tail but publishes only up to that point. It is persisted **before the client is
told the write succeeded** — `Collection::rewind_to` uses it as the floor below which a truncation
is refused, so a watermark lagging a crash would return published entries to the pending set and let
the next leader delete them.

Three read paths exist, and they mean different things:

- **committed read** — what clients get. `?read=quorum` is this read plus a leadership check; see
  [Read index and leader leases](#read-index-and-leader-leases).
- **newest durable read** — used only by read-modify-write (`PATCH`), because merging into the
  committed value would drop a write that is still in flight.
- **raw entry resolution** — no visibility opinion; used by internal machinery.

The absence of `applied.meta` means "no consensus history here": replay and publish everything. Both
local and replicated appends require the first full watermark to be persisted before changing the
WAL. Initialization shares the watermark writer lock with later saves and marks itself complete only
after the write succeeds; concurrent appends wait, and failure returns an error without allocating
an LSN or staging a frame. For a legacy collection the first watermark preserves the already
replayed prefix, while new entries remain uncommitted. A position without an `applied.meta` beside
it is still treated as having no consensus history.

Absence and damage mean opposite things, so they are answered differently. A file that is present
but does not parse is an error that fails the open, not a `None` that would be read as absence —
that reading turns damage into "publish the entire log", which is the one outcome the watermark
exists to prevent. Recovery is to restore the file or the collection directory, not to delete it.

The same line is drawn inside `applied.pos`. Its size must be exactly 1024 bytes, including both
512-byte slots; an existing zero-length, truncated or oversized file fails recovery without being
resized. Only a complete zero-filled slot is unwritten. Bad magic, a bad checksum or nonzero data in
an otherwise unwritten slot is damage. One valid slot still supplies the position; damage with no
valid slot fails recovery instead of falling back to an older `applied.meta`. An absent file or two
complete zero-filled slots still mean no position has been recorded. Creation pre-allocates only a
newly created file; an interrupted creation that leaves a short file requires recovery like any
other unexpected truncation.

---

## 8. Consensus

Leadership is Raft-shaped: monotonic terms, one vote per term, majority elections with a pre-vote
round, log-freshness checks, a commit rule that only counts the current term's entries, membership
changes through a joint configuration, and leases carrying reads.

### Durable term and vote

`replication.meta` holds `(term, is_leader, voted_for)` and is written as one value — a term without
its vote would permit a double vote after a restart. A node never acts on a term or a vote before it
is durable: a candidacy that cannot be persisted is abandoned, and a vote that cannot be persisted
is denied rather than promised.

The save lock covers both reading the durable record and publishing its replacement. Older terms are
ignored; a same-term save that clears or changes an existing vote returns an error without writing.
A first vote, repeated vote, or leadership change retaining that vote is allowed, and a higher term
may start with no vote or a new candidate. Unreadable metadata also fails the save, since its vote
cannot safely be checked.

A node that was leading rejoins as a follower unless it is a solo primary, because the cluster may
have moved on while it was down.

### Election

A follower with no leader contact for `heartbeat_timeout_secs` waits a per-node jitter — hashed from
`node_id` and the clock, bounded by `election_delay_ms`, so a shared timeout does not split the vote
every round — then probes its peers for a live leader at a term no lower than its own, and follows
it if there is one.

Otherwise it holds a **pre-vote** round before touching its own term. `POST /internal/pre-vote` asks
the question the real vote asks — log freshness, membership, and whether the voter owes its leader
silence — against a read lock, no term change, no recorded vote and no disk. A candidate short of a
quorum of willing voters keeps its term and vote: an isolated node does not inflate its term every
cycle, and a partitioned or removed one does not depose a healthy leader on its way past. A peer
that answers `404` is counted as willing, so a half-upgraded cluster elects exactly as it did
before. Both rounds share the same asking path, so the pre-vote gets the same early exit on a quorum
and the same higher-term detection the real vote has.

Only with the pre-vote won does the node increment its term, vote for itself, persist that, and
request votes. A lone voter — one whose own vote is already a quorum — skips the round entirely.

A voter grants a vote when the candidate's term is at least its own, it has not already voted for
someone else in that term, it owes no leader silence, and the candidate's log is at least as fresh
as its own. Freshness is compared **per collection**: one leader serves every collection, so a
candidate behind on any single collection could lose that collection's committed entries. A
database-wide `(term, lsn)` summary is also sent, and is used when a peer reports no per-collection
map.

**Complementary histories.** A failed pre-vote triggers history recovery from the current voting
configuration, including both halves of a joint configuration. The candidate reads each reachable
peer's collection tails and selects the greatest `(last_term, last_lsn)` for each log. It downloads
only histories newer than its own, using the bounded snapshot stream, then retries the election on
the next watchdog round. So X committed through leader+A and Y committed through leader+B does not
leave A and B refusing each other after the leader fails.

The received snapshot is opened and checked against the offered tail, the local tail, the local
commit watermark and the current term before installation. A changed voting configuration aborts the
transfer. Only a strictly newer history can replace local state. If the donor has not learned a
commit already applied here, the staged copy applies through that local watermark and saves its
index before installation. Entries above both watermarks remain uncommitted. The `_config` log is
recovered first; its configuration is installed before votes resume, and a changed electorate ends
this recovery pass. Recovery serializes with local writes, incoming replication and snapshot
installation; votes cannot inspect an installation in progress. No recovery response counts as a
vote or a quorum acknowledgment.

Missing recovery endpoints, transfer failures and invalid histories do not bypass voting checks and
are retried. A rolling upgrade requires the peers supplying the missing histories to support the
recovery endpoints. Collection enumeration or open errors refuse election requests rather than
advertising an incomplete history.

Learners never campaign and never vote. The restriction comes from config as well as from the
cluster view, which is what covers the window between boot and admission.

On winning, a candidate re-checks that its term and self-vote are unchanged, takes the same voting
set the election counted as its commit quorum, seeds each replica's send cursor from its own log
tail, and records leadership. Losing, or seeing a higher term, steps the node down.

It also appends a barrier to every collection whose tail it cannot commit — a durable-but-
uncommitted tail from a previous leader has no current-term entry, and committing one above it is
what commits it. That check is re-run on every driver tick rather than only at promotion, since a
collection being resynced at that instant is absent from the listing.

### Stepping down

Any higher term deposes a leader — seen in a heartbeat reply, a replication rejection, a vote
request, or a repair response. Demotion clears leadership, forgets the vote, resets quorum progress
(evidence belongs to the term it was gathered in), restarts the follower watchdog, and resyncs from
the newly discovered leader.

Granting a vote at a higher term is itself a step down, with one difference: there is no new leader
to name yet, because the candidate has not won. Such a node holds no primary at all, so the watchdog
looks for one on every tick while it has nobody to poll, and follows the winner as soon as there is
one. Standing for election still waits out the contact timeout, or a node would campaign against the
candidate it just voted for.

Three things reset the failover clock: a heartbeat from the leader, an accepted replication request,
and a successful resync. All of them are gated on the sender still leading at a term this node has
not superseded, so a deposed node cannot hold off the election that would resolve a split.

### Losing contact with the quorum

Every signal above is inbound, and an asymmetric partition leaves all of it healthy while nothing
this leader sends arrives. So a leader also gathers outbound evidence: it probes every replication
target on a quarter of the contact window, each reply stamps that replica's progress, and a leader
that cannot show a reply from a majority of the configuration within `heartbeat_timeout_secs`
relinquishes leadership.

That is a step down inside its own term: the term does not move and the vote is kept, because a node
that forgot voting for itself could seat a second leader in that term. Probes are detached rather
than awaited — waiting on a request into a cut link would age every healthy replica's contact by the
timeout before the check reads it.

### Commit rule

The commit index is per collection: an acknowledgement of LSN 7 for `users` says nothing about
`orders`. The leader sorts what it and each voting replica hold, takes the majority position, and
never moves the watermark backwards — a resynced replica can report a lower match than before.

It also refuses to commit below the first LSN this leader appended to that collection in its own
term. A majority holding a prior-term entry is not proof a later leader will keep it; entries below
that floor commit indirectly, once a current-term entry above them commits.

On promotion the committed watermark is seeded from each collection's applied LSN rather than zero:
anything already published must have been committed by a previous leader.

A leader with no replicas commits on its own durability. A learner's acknowledgement is recorded but
never counted.

While a configuration change is in flight the commit position is the **lower** of the two halves'
majority positions: an entry only one half holds is one the other half's next leader can still
overwrite.

### Configuration changes

The voting set is a `Configuration` — `voters`, plus `outgoing` while a change is in flight — and it
travels in the `_config` log, one per shard group, replicated and committed on the ordinary path.
Until a node has ever written one, the set is derived from the cluster view, and from config before
any real view has arrived.

`POST /cluster/configuration` takes the set an operator wants, not a delta: joint consensus makes an
arbitrary set change safe, so the set is the primitive and add/remove is arithmetic over
`GET /cluster/configuration`. A change is two entries, Raft §6:

1. The **joint** entry names both halves. From the moment it is appended, every quorum decision —
   commit index, election tally, `w=majority` — needs a majority of *each* half separately.
2. Once the joint entry commits, the leader appends the **target**: the incoming half alone.

Requests and automatic resume use a shared per-node configuration-change mutex. It covers reading
the current set, validation, leadership handover when required, and both append/commit steps. A
queued request rechecks leadership and validates against the configuration then in force. The task
that owns the gate outlives a cancelled client request, so cancellation cannot let another change
pass a blocking append still running. A completed retry adds no entries. After acquiring the same
gate, automatic resume rereads the committed joint entry and refuses to append over a pending entry
or a configuration that has already changed. The API also guards publication of the cluster view and
skips publishing an older result if a later transition has overtaken it.

Members of the outgoing half keep receiving frames and keep being asked for votes for as long as
they still decide anything. A node joining the set has its send cursor started from nothing rather
than from the leader's tail, so the driver sees the gap and ships it the log.

Refused, with nothing appended: an empty voting set; a voting set naming one node twice; a different
change while one is already joint; a previous entry that has not committed yet; and a voter that is
not already a member — admit it as a learner first so it can catch up before it narrows every
majority. A change that matches the set in force is a no-op.

Voter identity is the `host:port`, case-insensitively, the same rule the ring and the member list
use. Every majority — election tally, commit watermark, CheckQuorum, `w=majority` — is counted over
the physical nodes a half names rather than over its entries, because a duplicate raises the
threshold it is itself counted against. A set built here is deduped on construction, and one decoded
from a log frame or a snapshot has each half collapsed on the way in, so a change a pre-fix leader
left joint is still finishable by retrying it.

A change whose target set does not name **this** node is neither refused nor applied here. Raft
allows the leader to append it and requires it to step down once the target commits, on a log it
would by then have no standing to replicate; so leadership moves first, to a voter the change keeps,
and the answer is `409` naming that node. Nothing is appended either way, so a handover that fails
leaves this node leading and the request safe to retry.

If an entry is durable but does not reach a quorum the answer is `503`, not `500`: the entry is in
force, the change is neither applied nor undone, and retrying the same change finishes it. A leader
that inherits a *committed* joint configuration finishes it on promotion. Appending the target above
an *uncommitted* joint entry is not allowed, since a majority of the incoming half alone could then
commit both.

The cluster view records the outcome afterwards and never decides it, so a view that fails to
publish costs routing accuracy and nothing in the quorum.

### Leadership transfer

Raft §3.10. `POST /cluster/transfer-leadership` moves office while the leader is alive and
reachable, so nothing waits out a contact timeout. Three steps, each load-bearing:

1. **Hold the writes.** The node's write barrier is taken for write, which drains what is in flight
   and blocks what arrives. Without it the target chases a tail that keeps moving.
2. **Catch the target up.** Every collection, to this leader's tail. A target short of it *loses*:
   log freshness is per collection, so any voter holding a frame the target lacks refuses it.
3. **Tell it to stand.** `POST /internal/timeout-now`. The election it runs skips the jitter, skips
   adopting the leader that is standing aside, and skips the pre-vote — which it would lose by
   asking, since every voter is inside its refusal window.

Step 3 is the one that needs a key. Leader leases mean a healthy cluster cannot elect anyone, and a
handover is precisely an election on a healthy cluster. The vote request carries the leader that
sent the candidate, and a voter ignores its own refusal only when that names the leader it is
currently following. Nothing else can spend a leader's authority, and the vote still has to clear
log freshness and membership behind it.

The transferring leader also **gives up its own read lease** for the length of the handover, and
records no new grants while it runs. A lease is the claim that no election can complete, and this
node has just arranged for one. Reads pay the confirmation round instead, and that round asks the
voters, which is what learns the new term.

Stepping down is not the transferring node's own move: the target raises the term, and the vote it
asks for is what demotes the old leader. So a handover that fails leaves a leader in office rather
than a group with none. Everything is bounded — the drain, the catch-up, and the wait for the target
to take office — and on any failure the writes resume and this node still leads.

Who may be handed to: voters of the configuration in force, this node excluded, and while joint, of
*both* halves. Among those the default is the readiest, measured by the collection it is furthest
behind on. A transfer is refused outright while a configuration entry has not committed yet.

### Read index and leader leases

`?read=quorum` is answered after the node establishes a **read index**, which is the guarantee
`?read=primary` is not: `read=primary` asks a node its own opinion of who leads, and a replaced
leader still holds that opinion. The order is Raft's and none of it is optional:

1. An entry of this leader's own term is committed for the collection — until then the commit index
   is a lower bound rather than the answer, because entries inherited from a previous leader may sit
   staged above it. An empty staging buffer says the same thing another way.
2. The commit index is sampled, *before* leadership is confirmed, so a write landing during the
   round cannot become part of what the read has to cover.
3. Leadership is confirmed with a majority of the configuration: a voter answering at a term at or
   below ours has not moved to a higher one, and any leader elected above ours would have needed a
   majority — two majorities intersect. A peer claiming leadership at our own term is a split brain,
   not a confirmation; a higher term deposes this node instead of answering the read.
4. The index is waited for locally, so it is readable and not merely known.

Every refusal is `503` with `Retry-After: 1`, because none of them means the read was wrong: not the
leader, leadership too new, could not confirm, and confirmed but not yet visible are all "not now".
On a query the guarantee covers the page's starting point, not the whole scan.

**Leases** make step 3 usually free. The leader asks for them on the probe it already sends every
replica several times a contact timeout: `?lease_ms=` is how long it asks a voter to go on refusing
votes, and the reply's `novote_ms` is what the voter commits to. It is kept by the vote handler — a
voter that owes a leader silence withholds its vote whatever term the candidate offers — and a
majority of live grants rules out an election exactly as the confirmation round does. The refusal
window sits below the contact timeout, so no failover ever waits on it.

The round is the leader's rather than the follower's, and that is what makes it exact. The leader
stamps the interval *before* the request leaves, and the voter answers with a duration it starts
counting when the request lands, so a slow link leaves the leader's deadline earlier than the
voter's instead of later. Nothing converts one clock to the other.

A grant is bounded on both sides. A voter grants no more than the rest of its own refusal window,
and refuses outright to a leader at a term it has already left. A leader accepts no more than its
own window, counts grants only from members of the current configuration, and counts only those
answering a probe it sent at the term it still holds. Grants are dropped on every leadership
transition, and one is retired when the wall clock shows the monotonic clock stalled under it. Boot
counts as owing silence: a restart forgets a grant the leader is still counting.

---

## 9. Replication

### Leader-driven

The leader tracks, per `(replica, collection)`:

- **matched** — the highest LSN the replica is known to hold. Quorum evidence, reset on a term
  change.
- **sent through** — an exclusive lower bound for the next send. A cursor, not evidence; it may be
  restored from `progress.meta`, which is flushed on a two-second interval so the write path never
  waits on a disk write for a value that is only ever a hint.

Two things drive progress. A write ships its frame immediately. Independently, a background driver
ticks every `drive_interval_ms`, compares each replica's cursor against each collection's tail, and
streams whatever is missing — so an idle cluster still heals and a failed send is retried.

A replica that makes no progress for two consecutive rounds is polled on a widening interval, up to
sixteen ticks. A write still triggers repair immediately regardless of backoff.

A third case is neither: there is nothing to send, and yet nothing records that the replica holds
the tail. That is the state a promotion leaves — evidence belongs to the term it was gathered in —
and the state a snapshot install leaves, since installing a log is not acknowledging one. A missing
cursor counts here too. The leader then re-sends the tail frame purely to be answered: the reply is
either `duplicate`, which the receiver only gives after checking the term at that LSN, or a gap,
which says where the replica really is. The periodic driver and the catch-up a leadership handover
runs both take this branch, so neither streams a collection from zero for want of a cursor. Without
it a leader can hold a tail a majority already has and never be able to prove it.

Before sending a frame the leader checks the cursor: if the replica is behind this frame's
predecessor, the backlog is streamed from the cursor first. Both send paths route through that
check.

### What a replica does with a frame

`POST /internal/replicate` carries one head frame plus an ordered batch of successors. The receiver:

1. Steps down if the request's term is higher than its own, and rejects with `409 stale_term` if it
   is lower. A leader is deposed *before* the "not a replica" check, so a deposed leader cannot keep
   accepting writes.
2. Refuses with `503` while a snapshot resync for that collection is installing.
3. Validates that the head frame's header agrees with the request fields and that successive frames
   form an increasing predecessor chain, then appends frame by frame until the first refusal.
4. Fsyncs the accepted new frames and records the durable prefix matched in this leader's term. A
   duplicate proves only its own LSN, not the replica's remaining tail.
5. Publishes up to the smaller of the leader's commit hint and that matched, durable prefix. A
   refused first frame advances nothing; a partially accepted batch may commit its accepted prefix.

Heartbeat commit hints use the same bound, under the collection's snapshot-install lock. Matching
evidence from an earlier term or process cannot publish pending entries. Tail confirmations and
replication rebuild it, so heartbeats can still commit an idle collection. Hints beyond the matched
prefix are not saved for later appends to consume. Reads and change events share this publication
gate.

Each frame is classified before it is written:

| Verdict | Condition | Reply |
|---|---|---|
| **Applied** | chains onto our tail, or replaces an uncommitted tail we may drop | `200 {"status":"applied","lsn":…}` |
| **Duplicate** | LSN at or below our tail, and either already published or staged at this term | skipped; `200 duplicate` if nothing in the batch was new |
| **Gap** | `prev_lsn` is ahead of our tail | `409 {"status":"gap","last_lsn","last_term"}` |
| **Divergent** | a conflict at or below our tail that truncation cannot resolve | `409 {"status":"divergent","last_lsn","last_term","applied"}` |

A retransmit is recognised first, ahead of any conflict check: a committed LSN is agreed and a
staged one matches at the same term, so an ordinary resend is never read as a conflicting log.

The batch is split as a head frame plus successors so that a peer which only understands one frame
per request stays correct — it applies the head, reports that LSN, and the leader resumes from there
instead of stepping over the frames it skipped.

### Truncating a divergent tail

A frame whose `prev_lsn` sits *below* our tail says the leader is replacing entries we hold. Raft
§5.3: the replica drops them rather than asking for a whole collection. Everything above the
predecessor leaves the pending map, later WALs are emptied before the cut file shrinks — the reverse
order leaves frames above a truncation point after a crash, and replay cannot tell those from a log
that continues — and the durable LSN is lowered to the cut, since durability this node no longer has
must not count toward a quorum. A frame at our own tail position but with a different `prev_term` is
a conflict with nothing above it to cut, and refuses outright.

Truncation stops at the applied watermark: a published entry was committed by somebody and is not
ours to drop. It also refuses when the named predecessor is not one we hold at that term, or when
nothing above the cut is uncommitted. In each of those cases the reply is `divergent`, and it
carries `applied` — the lowest point the leader can back up to — so the leader resumes there instead
of starting a transfer. A peer that reports no watermark gets a snapshot.

### Repair

A gap is normal, not a fault. The leader rewinds that replica's cursor to the reported tail and
starts a repair, at most one per `(replica, collection)`:

1. Read the frames after the replica's tail from the WALs, refusing a set with a hole in it.
2. Verify the frames form an unbroken `prev_lsn`/`prev_term` chain from that tail.
3. If the chain does not reach the collection's tail — because the replica is below the floor a
   compaction retained, or further behind than the retention budget covers — trigger a snapshot
   resync instead.
4. Otherwise ship the chain in batches bounded by frame count (64) **and** by size (one
   `MAX_RECORD_SIZE` frame's worth of raw bytes, so the encoded request fits `MAX_INTERNAL_BODY`):
   one round trip and one remote fsync per batch, releasing the inflight slot between chunks so a
   long backfill does not hold one for its whole duration. A frame over the size budget travels
   alone.

Repair runs up to eight passes, so writes arriving mid-stream are picked up, and re-checks the tail
before exiting. Divergence rewinds the replica's cursor to the watermark its refusal named and
repairs from there; only a divergence with no watermark, or one hit during a backfill that already
resumed from a point the replica named, falls back to a snapshot.

An acknowledgement counts toward write concern only when it means "this replica holds this frame",
and running out of passes short of that frame is not evidence. Coalescing behind another worker
depends on who is asking. A caller with no LSN to answer for — a gap, a divergence, the periodic
driver — leaves rather than waiting for an answer nobody reads. A caller waiting on a write concern
queues, and re-checks what the replica matched; if that repair read its target before this frame was
appended, the waiter keeps the lock and does the work itself.

### Write concern

`?w=` on any mutating request:

| Value | Acknowledgements required |
|---|---|
| `1` (default) | local durability only |
| `majority` | a majority of the configuration in force, counting the leader |
| `all` | every voting replica plus the leader |
| *N* | clamped to `1..=1 + voting replicas` |

Anything else is `400`.

`majority` is not a count while a configuration change is in flight: it is a majority of each half,
and any number of acknowledgements from one half alone is not one. The `required` figure a `202`
reports is then a floor.

`?wtimeout=` bounds the wait in milliseconds; the default is 5000. When the concern is met the
response is the normal `200`/`201`. When it is not, the write is still durable and the response is
`202 Accepted` carrying `acks`, `required` and a `warning`.

The local fsync and the replica round trips run concurrently, so a `w=majority` write costs the
slower of the two rather than their sum. Frames are replicated before the client is acknowledged.

#### The leadership fence

Every concern is fenced against the leadership term the append was made under. `append_term` samples
that term at the append rather than trusting the handler's check, which happens before the write
gate and the key locks; `kept_authority` rechecks it after the fsync and the replica calls, before
an outcome is built. A term lost at either point is `503 no longer leading`, and in the first case
nothing is appended at all.

`w=1` is what makes the recheck load-bearing. `w=majority` on a deposed node reports
`acks: 1 required: 2` and the shortfall is visible; `w=1` requires one acknowledgement and the local
node supplies it, so the quorum arithmetic can never notice that the entry is unreplicated at a
stale term. The refusal is `503` rather than `403` because [the router](#15-routing-and-failover)
retries a `503` across the group, and the node holding the current term is in it.

---

## 10. Snapshot transfer

When incremental repair cannot converge, the leader tells the replica to resync and the replica
pulls `GET /internal/snapshot?collection=…`.

A leader that has no such collection answers `404` rather than serving an empty snapshot. Serving is
not a way to create one, and an empty snapshot could not be told apart from one of a collection the
leader really holds nothing of. The replica reports that refusal separately from a failed transfer,
and nothing local is touched. A *dropped* collection is a tombstone that stays on disk, so it is
still served; the `404` means no directory for that name at all.

An accepted resync moves the leader's send cursor to its own tail. The snapshot carries the log, so
resuming below it would re-read the range that forced the snapshot and escalate again on the next
tick. The cursor is a hint and never evidence, so raising it optimistically is safe: if the install
did not land, the confirmation comes back a gap and the cursor drops to where the replica says it
is.

The sender holds the compaction boundary, fsyncs, and either rotates to a fresh WAL or — if the
active WAL is empty already — treats the existing set as frozen, so repeated requests against an
idle leader cost no new files. It then streams a self-describing format: an 8-byte magic, then one
entry per file (the index snapshot, the applied watermark, each frozen WAL) as 64 KiB chunks with a
length-and-CRC footer per entry, independent of HTTP framing. Files are produced on a blocking task
through a two-chunk channel, so a slow client backpressures the producer instead of buffering a
whole collection in memory.

The receiver validates as it goes: the magic, one normal path component per filename restricted to
the three expected shapes, no duplicates, a chunk-size ceiling, a file-count ceiling, the per-entry
length and CRC, the presence of every required file, and no trailing bytes. It then checks the
staged applied watermark twice over: it may not exceed the staged WAL tail, and it may not fall
below what this node has already published — a source deposed mid-transfer must not be able to
retract entries a quorum committed.

Installation swaps directories under the collection-map write lock: release the live handles onto a
tombstone file, write a `.install` marker into the staged directory, move the old directory aside,
move the staged one in, reopen. Every failure path restores and reopens the previous directory, and
incoming replication for that collection is serialised against installation.

The two renames are not one atomic step. Boot, before it adopts collection tails, reconciles any
live, `<name>.tmp` and `<name>.old` directories: a live directory is kept, a missing live directory
is filled from a marked or complete staged snapshot, and otherwise the `.old` backup is restored. A
later write cannot create an empty live directory beside a surviving backup. `.tmp` and `.old` are
excluded from collection names and listings.

Compaction and an install rewrite the same directory, so they are interlocked. Each holds the
collection's rewrite lock across the steps that change files on disk and checks, on entry and again
before the writer swap and the publish, whether the handle has already been released. A released
handle also refuses an append and refuses to serve a snapshot.

A replica also performs this sync for each existing collection at boot when it is configured with a
primary, and again for every collection when it discovers it is following a new leader.

---

## 11. Flow control

Two independent bounds, because they protect different things.

**Uncommitted backlog.** Staged frames only drain on commit, so a leader that has lost its quorum
would otherwise stage forever. A write is admitted only if its own frame count fits beside what the
collection already holds — staged frames plus the frames of writes admitted but not yet appended —
within `max_uncommitted_frames`. Otherwise it is refused with `503`, a `Retry-After: 1` header, and
a body naming the collection, the current backlog and the bound. The check happens before the
append, which is the last point where growth can still be refused. Rejections are counted in
metrics. Setting the bound to `0` restores unbounded behaviour.

Admission reserves the capacity it grants and releases it once the append stages, so the count a
request decides against already includes every other request in flight. A bulk write of *n*
documents reserves *n* frames and is admitted as a whole or not at all. A batch wider than the whole
bound is refused with `413 Payload Too Large` naming the batch size and the bound, not `503`:
draining cannot make room for it. Routers treat `413` as an authoritative answer and do not retry it
on the rest of the group.

Migration pushes go through the same admission, so keep `max_uncommitted_frames` above any source's
`data_movement.batch_size` — the two live on different nodes and nothing can validate the pair.

**Outbound concurrency.** A node-wide semaphore of `max_inflight_requests` permits caps concurrent
replication requests across every write and repair, rather than bounding one write's fan-out.
Available permits are exposed in metrics.

---

## 12. Cluster metadata

The cluster view is one versioned value carried by every node:

```json
{
  "version": 7,
  "updated_by": "n1",
  "seeded": false,
  "members": [
    { "url": "http://127.0.0.1:9501", "node_id": "n1", "role": "shard",
      "shard_role": "primary", "voting": true },
    { "url": "http://127.0.0.1:9504", "role": "shard", "shard_role": "replica",
      "voting": false, "follows": "http://127.0.0.1:9501" }
  ],
  "shards": [],
  "ring": { "vnodes": 128, "shards": [ { "node_url": "…", "replica_urls": ["…"] } ] },
  "migration": null
}
```

It is replaced wholesale rather than patched: a half-applied topology update is a routing hole.

### Convergence

Views are ordered by `(version, updated_by)`, which gives every node the same total order. A seed —
a view derived from one node's config — loses to everything and wins against nothing, because a
router seeds a ring while a shard seeds none, and letting a seed outrank a real view would route
every key nowhere. Two concurrent updates at the same version converge by deterministically
discarding one of them; that makes routing consistent rather than making concurrent control-plane
writes safe, which is why only a leader publishes.

Adoption validates before comparing, so one malformed push cannot outrank every correction that
follows. A view is persisted before it becomes visible.

Views spread three ways: a direct push to every node a change touches, an identity advertised in
every heartbeat that makes a follower pull the better view, and a router's probe loop pulling from
whichever peer is furthest ahead. What a heartbeat advertises is the whole thing the order is
computed from — `cluster_version`, `cluster_updated_by` and `cluster_seeded` — and both pullers apply
that same order rather than comparing versions, so two nodes that published concurrently converge on
the winner. The two fields after the version are absent from a node that predates them, which reads
as an unorderable equal version and falls back to strictly-greater, so a half-upgraded cluster
behaves as it did before.

The three-second catalogue round (see [Secondary indexes](#across-shard-groups-the-catalogue)) is
the fourth path, and the one that reaches between two shard leaders, which the other three do not.

### Membership

`POST /cluster/members` admits a node; `DELETE /cluster/members?url=…` removes one. Both must go to
a shard leader, which answers with the primary's address when it is not the leader itself.

An admitted node joins as a **learner**: it receives frames immediately and can serve reads, and is
counted in no quorum. Asking to join as voting is rejected with an explanation rather than quietly
downgraded, and a node that is already voting cannot be re-added as a learner, because that would
shrink the quorum it is already in.

Promotion and demotion are the log's business, not the view's:
`POST /cluster/configuration` moves the voting set through
[joint consensus](#configuration-changes), and the view records the outcome afterwards. So the order
for adding a voter is admit, catch up, promote; the order for removing one is demote, then
`DELETE /cluster/members`. Removing a voting member from the view directly is refused and names the
configuration endpoint: a voter the log still counts but the view cannot name is a voter nothing can
route to.

A learner names the primary it `follows`, defaulting to the leader that admitted it, so one
cluster-wide member list can serve several shard groups: a leader ships frames only to learners
naming it. On admission the leader starts that node's send cursor at 0 — a node it has never sent to
holds nothing, and starting at the tail would leave the driver seeing no gap.

A node that learns of its own admission by propagation behaves exactly as if it had been told
directly: it starts following the primary the view assigned it.

---

## 13. Ownership: ranges and the hash ring

Two models coexist. Wherever both are present, the ring decides.

**Ranges** (`shards`) assign explicit `[start_hash, end_hash)` intervals, wrapping when
`start > end`. Validation requires exact coverage of the 64-bit space with no overlap. They are kept
for clusters already running on them, and retained even under a ring so a rollback has something to
return to.

**The ring** (`ring`) stores membership and a vnode count; tokens are *derived*, never stored. Each
shard gets `vnodes` tokens at `xxh64(node_key#i)` — 128 by default, 4096 at most — and a key belongs
to the first token clockwise from `xxh64("collection:key")`. A ring holds at most 1024 shards and at
most 2^20 `shards x vnodes` tokens; the product is the binding one, since that is what deriving the
layout and diffing it against the current one costs. Over either ceiling is `422`, and the same
refusal applies wherever a ring arrives — the endpoint, a peer's view, `cluster.meta` at boot, and
the config file. Ties break by the same identity, so two nodes cannot disagree about a colliding
token. `node_key` is the lowercased `host:port`, so a trailing slash, a scheme change or host case
does not silently move a node to a different part of the ring, and a ring listing one host under two
spellings is refused as a duplicate. The token layout is built once per cluster version and cached,
not per request.

A ring entry belongs to a whole group, not one process: after a failover the node answering is a
replica URL, and it owns exactly what its entry owns.

### Ownership checks on the shard

Routing alone cannot make a topology change safe, because views converge rather than switching
together. So every shard classifies each key against its own view:

| Verdict | Response |
|---|---|
| Ours | serve normally |
| Elsewhere | `409` naming the `owner` |
| Moving, during the final phase of a handover | `503` with `Retry-After: 1` and `moving_to` |

Reads are refused only for keys owned elsewhere: a key mid-handover still reads correctly from its
current owner. A node outside an existing ring owns nothing and redirects everything, which is
exactly the state of a shard removed by a handover that still holds its data.

`POST` is a special case: the key is the server's to choose, so a shard that does not own the id it
drew simply draws another instead of refusing the write.

### Measuring a ring change

Keyspace movement between two rings is computed exactly, not sampled: every token from either ring
is a boundary, ownership is constant between adjacent boundaries, and the arc widths sum to the
fraction that changes hands. The per-pair transfers are reported alongside it.

### Ring changes without data movement are gated

`POST /cluster/ring` moves ownership and nothing else, so keys that change owner would read as
missing at their new owner. It therefore refuses unless the change provably moves nothing, or every
current owner confirms it holds no data. A failure to confirm — an unreachable owner, an unreadable
data directory, a missing answer — counts as "holds data" and blocks the change. The refusal points
at `POST /cluster/migrate`, which copies first. `?dry_run=true` always reports what the change would
cost without applying it. `allow_unsafe_ring_changes` forces it through, warns loudly at boot, and
is meant for development.

---

## 14. Online migration and rebalancing

`POST /cluster/migrate` takes the same target ring as `/cluster/ring` but copies the data before
ownership moves. If the target owns exactly the same keys, it publishes the ring.

The plan — and only the plan — lives in the cluster view, so every node agrees on which keys are
moving. Progress is per node: a half-copied shard is not a fact the cluster needs to agree on, and a
restart mid-copy re-plans from the view. A *finished* phase is written to `migration.meta` instead,
because a node that restarts after the flip still owes the cleanup and the plan is out of the view
by then.

**Copy.** Every source leader lists its keys, keeps those the current ring gives it and the target
ring gives away, groups them by destination, and pushes batches of `data_movement.batch_size`
documents with a configurable pause between them. That setting bounds the document *count*, so a
batch is split again by size to fit `MAX_INTERNAL_BODY`. Committed values are copied: an uncommitted
write may still be revoked. Destinations write batches through the normal write path, so moved keys
replicate inside the destination group. A destination that has not yet adopted the plan refuses the
batch, which is expected — pushes retry with a widening backoff for as long as the plan is in the
view.

**Finalize.** Once every source reports done, the coordinator publishes the finalizing phase.
Sources now hold a write barrier for their own push; destinations first reset any keys previously
received from that source, validated against the ring as genuinely moving; and the remaining keys
are pushed with no inter-batch delay. During this phase, and only during it, writes to keys that are
about to move get `503` with `Retry-After`.

**Flip.** The coordinator publishes the target ring. Ownership moves as each node adopts it.

**Cleanup.** The final view is handed to every node that owned the keyspace before or owns it now —
awaited, because a node still holding the plan correctly refuses to clean up — and each deletes the
keys it handed over. Deletions go through the write path as tombstones, so they are staged,
committed, applied and replicated like any other write, and every key is re-checked against the live
view first. Leftovers are unreachable while a node stays out of the ring, and put that node back
later and stale values would shadow current ones — so cleanup also reaches shards dropped from the
ring entirely.

The coordinator is whichever leader was asked to start the migration, and it moves no data itself.
If it dies mid-move nothing is lost: the plan is in the view, sources keep pushing, and coordination
resumes idempotently. Recovery hangs off every event that can leave a node holding the plan — a
promotion, a view adoption, a boot — so a leader that learns of the handover after winning office
picks it up too; the coordinator is then the elected leader of the lowest shard group. Calling it
often is safe because neither half restarts work already in flight: a push is skipped while its task
is alive, and the coordinator loop is single-flighted per plan. A progress record is not that
evidence, so a node re-elected inside a phase replans and pushes again rather than reading its own
abandoned record as a running copy. `DELETE /cluster/migrate` abandons a handover at any point
before the flip.

### Automatic rebalancing

Off by default, and shard-only. When enabled, the node in the lowest shard group derives the ring
that live membership implies — primaries sorted by endpoint, replicas placed by the primary they
follow — and, if it differs from the current ring and has stayed stable for `stabilization_secs`,
starts a migration through exactly the same copy-then-flip path. Membership churn is debounced into
one handover. `GET /cluster/rebalance` reports current versus desired placement, whether this node
is the coordinator, and what a reconciliation would move.

---

## 15. Routing and failover

A router holds no data. For each request it hashes the key, finds the owning group in its view, and
forwards.

**Writes.** Forwarded to the group's current primary. A `409` naming an owner means this router's
view is stale, so the write is retried once against the named owner. A shard's `4xx` is an answer,
not a failure — a `404` on `PATCH` means no such document — and is passed through. A transport
failure or a `5xx` triggers failover: one router-wide lock per shard, a re-check of the effective
primary in case a probe already moved it, then each replica in turn. Whichever replica answers
authoritatively is cached as that group's primary for 30 seconds.

The redirect belongs to every one of those attempts, not only the first: a ring that has moved and a
primary that has died are independent events and one write can meet both. A redirect is never what
promotes a node in the override cache — its whole content is that the key is not this node's, which
is no evidence about who leads the group.

**Reads.** With no `?read=`, the primary is tried and then the replicas, with no guarantee either
way. `?read=primary` and `?read=quorum` are forwarded, so the node that answers is the one enforcing
them. Replicas stay in the candidate list for both, so a promoted one is still found; every
candidate refusing is a `503` naming no reachable primary, which is distinct from the shard being
down. `?read=replica` prefers replicas and falls back to the primary, ranking candidates by observed
load: inflight requests times an EWMA of request latency, with unmeasured nodes tried after measured
ones and round-robin breaking ties. Load samples come from heartbeat probes, expire after ten
seconds, and are adjusted by the reads this router currently has in flight to each node. An unknown
value is a `400`. Reads follow the same `409` redirect as writes.

**Probing.** Every three seconds the router probes each group's primary, its replicas and the
current override. The highest-term node answering as primary wins, so a partitioned old primary
cannot reclaim traffic. The same pass collects load samples and pulls a better cluster view from
whichever peer is furthest ahead by the view order, tiebreak included.

**Fan-out.** `GET /collections` unions the collection lists of all groups, and refuses with `502`
naming a group none of whose nodes could answer: an empty list from an unavailable group would drop
its collections out of the union with nothing in the reply to say so.
`DELETE /collections/:name` and the maintenance endpoints fan out and report per node, answering
`207 Multi-Status` when any node failed; the drop's `?w=`/`?wtimeout=` are forwarded to each group.
Since those are forwarded, a group can answer `202` rather than fail, and the drop's aggregate keeps
that apart from success: `200` only when every group committed, `202` when every group answered and
at least one staged. The index fan-out grades the same three outcomes the same way, with `404` still
meaning "holds none of this collection" rather than a failure, and a create answers `201` when every
group that holds the collection created the definition. Compaction is sent to primaries only. A node
answering `404` is not a failure: a collection whose keys never hashed into that node's share does
not exist there. Every node answering `404` is the collection existing nowhere, and the router
answers `404` for the whole fan-out. `/query` merges across shards the same way — an absent shard
contributes no rows and no cursor position.

**Bulk writes** are split by owning group, sent in parallel, and reassembled in request order, so a
per-document result always lines up with the document that produced it. A slice refused with a `409`
naming an `owner` is retried once at that owner. Retrying the slice whole is safe because the shard
checks every key before writing anything: a target that does not own all of them refuses all of
them, so a slice spanning owners costs one wasted request rather than a half-applied batch. A group
that answered with an authoritative non-2xx sent no per-document results, so that status and body
are reported for every document in its slice — each item carries the shard's `code` and message.
When one such refusal answered for the whole batch, the router replies with the shard's status and
body instead of a `207`: no document was written, and a `413` in particular has to stay
distinguishable from a protocol fault because it is the one refusal a retry cannot satisfy.

---

## 16. REST API reference

All bodies are JSON. Errors are `{"error":"…"}`.

### Documents

| Method | Path | Notes |
|---|---|---|
| `POST` | `/collections/:name/docs` | Body `{"value":…}`. Server-generated UUID key. `201 {"id","status":"created"}`. |
| `PUT` | `/collections/:name/docs/:id` | Full replace. `201` when created, `200` when replaced. |
| `PATCH` | `/collections/:name/docs/:id` | JSON Merge Patch. `404` if absent; a `null` body is rejected. |
| `GET` | `/collections/:name/docs/:id` | The stored value, or `404` — for the document or for the collection. Accepts `?read=`. |
| `DELETE` | `/collections/:name/docs/:id` | `200 {"status":"deleted","existed":bool}`. |
| `POST` | `/collections/:name/docs/bulk` | Body `[{"id"?,"value"},…]`. One group commit and one replication round, per-document results. `201` when every item met its write concern, `207` when some did not, and the shard's own status when a refusal answered for the whole batch. |
| `GET` | `/collections/:name/docs` | An ownership-filtered query page (`items`, `next_cursor`), or `404` if there is no such collection. Takes the same `keys=true`/`keys=embed` shapes as `/query`. Default limit 100, maximum 10 000. Shard-only; a router answers `501` and points at `/query`. |
| `GET` | `/collections/:name/query` | See [Querying](#17-querying). |
| `GET` | `/collections/:name/aggregate` | Totals over the same filter, grouped on dotted fields, within a per-shard read budget. See [Aggregation](#17a-aggregation). |
| `GET` | `/collections/:name/changes` | A server-sent-events stream of committed changes, from one shard group or, on a router, from all of them. See [Change streams](#17b-change-streams). |
| `GET` | `/collections/:name/changes/ws` | The same stream as a WebSocket, with the same parameters and the same refusals. |

Mutating requests accept `?w=` and `?wtimeout=`. Merge-patch semantics are the standard ones: a
`null` value removes a field, nested objects merge, anything else replaces.

`?read=` on `GET /collections/:name/docs/:id`, `/query` and `/aggregate`:

| Value | Meaning |
|---|---|
| *absent* | Primary first, then replicas. No guarantee. |
| `primary` | The answering node must believe it leads, or it refuses with `503`. |
| `quorum` | Linearizable: the leader establishes a [read index](#read-index-and-leader-leases) first. Refusals are `503`. |
| `replica` | Prefer replicas, ranked by observed load; falls back to the primary. Possibly stale. |

Status codes worth knowing:

| Code | Meaning |
|---|---|
| `202` | Durable, but the write concern was not met; the body carries `acks` and `required`. |
| `400` | An unknown `read` preference, a filter, sort or metric the engine cannot evaluate, a cursor that does not belong to this query, an aggregation over more groups than the ceiling or past its read budget without `partial=true`, a `max_docs` above the ceiling, or a non-canonical collection name outside `a-z0-9._-`. |
| `403` | A direct write sent to a node that is not the leader, or a collection name beginning with `_`. |
| `404` | No such document, or no such collection. Reads never create one. On a router it means no shard holds it; a shard that holds none of a collection other shards do is not an error and does not reach the client. |
| `410` | A change-stream position older than the buffer still holds; the body names the `resume_floor` that works. |
| `413` | A request body over `MAX_PUBLIC_BODY`, or a bulk write of more documents than `flow_control.max_uncommitted_frames` allows in total. Not retriable. Through a router it stays `413`, and a per-item `code` says which slice it refused. |
| `422` | A ring that fails validation: overlapping or duplicated members, or a layout over the shard, vnode or token ceiling. |
| `429` | Every scan slot on the node is taken: aggregation, a sorted page, or a filtered one. Retriable, and per node: through a router it means every target in some group said so. |
| `409` | This shard does not own the key; the body names the `owner`. A router follows that name and retries once, for reads, single writes and bulk slices alike. Also an unsorted cursor issued against a different shard layout, from the shard as well as the router — a router passes that one through rather than trying the next replica, which holds the same view. |
| `503` | Replication backlog too large, the key is mid-handover, or a `primary`/`quorum` read this node cannot answer. `Retry-After` is set. Also a write whose [leadership term](#the-leadership-fence) went away while it was in flight — nothing was acknowledged, and the retry belongs on whichever node took the term. |

A bulk write checks ownership for every key before writing anything: the batch is accepted whole or
refused whole. [Admission](#11-flow-control) is whole-batch for the same reason.

### Collections

| Method | Path | Notes |
|---|---|---|
| `GET` | `/collections` | `{"collections":[…]}`. A router unions every group, and answers `502` naming a group that could not answer rather than a partial union. |
| `DELETE` | `/collections/:name` | Leader-only, and behind `auth.admin_keys` when set. A replicated log entry: accepts `?w=`/`?wtimeout=` and answers `202 {"status":"staged"}` when the quorum is short. Through a router, `202` when any group staged and `207` when any group failed. |
| `POST` | `/collections/:name/compact` | Leader-only. `404` if there is no such collection, `409` if a compaction or snapshot transfer is running. Reports bytes before and after. |
| `POST` | `/collections/:name/snapshot` | Writes an index snapshot. Allowed on any node. `404` if there is no such collection. |

### Secondary indexes

| Method | Path | Notes |
|---|---|---|
| `POST` | `/collections/:name/indexes` | Body `{"name","field"}`. Leader-only, and behind `auth.admin_keys` when set. A replicated log entry: accepts `?w=`/`?wtimeout=` and answers `202 {"status":"staged"}` when the quorum is short. `201` on success, `200 {"status":"exists"}` when the same definition is already there, `409` when the name is taken by another field or the collection is at its index ceiling, `400` on a malformed name or field path, `404` when the collection does not exist. Through a router, `201` when every group that holds the collection created it, `200` when one already had it, `202` when any group only staged it, `207` when any group failed, and `404` only when no group holds the collection. Also records the definition in the cluster index catalogue. |
| `GET` | `/collections/:name/indexes` | `{"collection","indexes":[{"name","field","state","documents","values"}]}`. `state` is `ready` or `building`; the planner uses an index once it is `ready`. A router unions the shard groups, summing the counts and reporting `building` if any group is still building it or has not got it yet. Not admin-gated. |
| `DELETE` | `/collections/:name/indexes/:index` | Leader-only, and behind `auth.admin_keys` when set. Also a replicated log entry with `?w=`. `200 {"existed":false}` when there is no such index, which writes nothing. Through a router, graded like the create. |

`201` says the definition is agreed, not that the index answers queries: the build runs in the
background afterwards, and `GET` is where that shows. Full semantics in
[Secondary indexes](#6a-secondary-indexes).

### Webhooks

Every route here is behind `auth.admin_keys` when that list is set, reads included: a listing names
the destinations this node posts documents to.

| Method | Path | Notes |
|---|---|---|
| `POST` | `/collections/:name/webhooks` | Body `{"id","url","secret"?,"filter"?,"ops"?}`. Leader-only. `201` after majority commit with the subscription and the peers it reached; `503` if unconfirmed (may still commit); `400` on a malformed id, URL, filter or `ops`; `404` when the collection does not exist; `409` at `webhooks.max_subscriptions`; `501` on a router or when `webhooks.enabled` is false. Re-posting an id replaces the destination and keeps the position. |
| `GET` | `/collections/:name/webhooks` | `{"collection","delivering","webhooks":[…]}`. `delivering` is whether this node currently leads. |
| `GET` | `/collections/:name/webhooks/:id` | One subscription and its delivery state, or `404`. |
| `DELETE` | `/collections/:name/webhooks/:id` | Leader-only. `200` after majority commit with the peers reached; `503` if unconfirmed (may still commit); `404` for an absent subscription on a single-node group. |

The secret is never in a response. Full semantics in [Webhook delivery](#17c-webhook-delivery).

### Cluster

Every route here is behind `auth.admin_keys` when that list is set, reads included; it falls back to
`auth.api_keys` when it is empty.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/cluster` | This node's view: version, model, members, ring, ranges, owners, migration. |
| `POST` | `/cluster/members` | Admit a node as a learner. Leader-only. |
| `DELETE` | `/cluster/members?url=` | Remove a non-voting member. Leader-only. |
| `GET` | `/cluster/configuration` | The voting set in force: `voters`, `outgoing`, `joint`, `uncommitted`. Any shard node. |
| `POST` | `/cluster/configuration` | Body `{"voters":[…]}`. Moves the set through joint consensus. Leader-only; `422` when refused, `503` when an entry is durable but short of a quorum, `409` with `primary` when the change removes this node and leadership was handed over. |
| `POST` | `/cluster/transfer-leadership` | Body `{"to":"<url>"}`, or empty for the readiest voter. Leader-only; `422` when refused, `503` when the handover was attempted and did not complete. |
| `POST` | `/cluster/ring` | Publish a ring. `?dry_run=true` to inspect. Gated when the cluster holds data. |
| `POST` | `/cluster/migrate` | Copy-then-flip handover to a target ring. `202` with `migration_id` and transfers. |
| `GET` | `/cluster/migrate` | Handover status and this node's local progress. |
| `DELETE` | `/cluster/migrate` | Abandon a handover before the flip. |
| `GET` | `/cluster/rebalance` | Current versus desired placement, and what reconciling would move. |

### Observability

| Method | Path | Notes |
|---|---|---|
| `GET` | `/health` | `200 ok`, or `503 degraded` with `reasons`. Always reachable without credentials. |
| `GET` | `/metrics` | JSON by default; `?format=prometheus` for text exposition. |

### Internal

Node-to-node only, behind the shared secret. `/internal/cluster` is served by every role; the rest
require a shard.

| Method | Path | Purpose |
|---|---|---|
| `GET`/`POST` | `/internal/cluster` | Serve or offer a cluster view. |
| `POST` | `/internal/replicate` | Append a batch of frames. |
| `GET` | `/internal/snapshot?collection=` | Stream a collection snapshot (leader only). `404` if this node has no such collection. |
| `GET` | `/internal/election-histories` | Complete per-collection tails and current term for election recovery. |
| `GET` | `/internal/election-snapshot?collection=` | Stream an existing collection's history from a shard, including a follower. `404` for an absent collection. |
| `POST` | `/internal/resync` | Ask a replica to pull a snapshot. |
| `POST` | `/internal/vote` | Request a vote. |
| `POST` | `/internal/pre-vote` | Would this vote be granted? Changes nothing on the voter. |
| `POST` | `/internal/timeout-now` | Stand for election now: the leader is handing over. Accepted only from the leader this node follows. |
| `GET` | `/internal/heartbeat` | Term, role, commit watermarks, cluster version, load. `?term=&lease_ms=` asks the answering voter for silence; the reply's `novote_ms` is what it grants. |
| `GET` | `/internal/data-summary` | Whether this node holds any data. |
| `POST` | `/internal/migrate` | Receive a handover batch. |
| `POST` | `/internal/migrate-reset` | Clear previously received keys before the final pass. |
| `POST` | `/internal/migrate-cleanup` | Delete keys handed over, once the flip is visible. Leader-only; `409` elsewhere. |
| `GET` | `/internal/migration-status` | This node's handover progress. |

---

## 17. Querying

```
GET /collections/:name/query
      ?filter={"age":{"$gte":30}}
      &sort=tier:asc,age:desc
      &fields=name,profile.city
      &start=u&end=v
      &limit=50
      &cursor=…
      &max_docs=100000
      &keys=true
      &read=replica
```

The response is `{"items":[…],"next_cursor":"…"|null}`. The cross-shard merge needs each row's key
to break ties and to build the next cursor, so a router always asks the shards for them; a client
may, in either of two shapes.

`keys` takes `true`, `false` or `embed`, and anything else is a `400`. Omitted is `false`.

| `keys` | Response |
|---|---|
| omitted, `false` | `{"items":[{"title":"one"}], "next_cursor":null}` |
| `true` | the same, plus `"keys":["a"]` — one entry per item, in the same order |
| `embed` | `{"items":[{"id":"a","value":{"title":"one"}}], "next_cursor":null}`, and no `keys` array |

`keys=true` is the compatibility form and is not deprecated: it is what a client that already pairs
the two arrays itself asks for, and what one node asks another for. `keys=embed` is the ergonomic
form, for a client that would only have written that pairing out by hand.

An embedded row is a projection over the response, not a change to the document: `value` is exactly
what was stored, so `GET /collections/:name/docs/:id` is unaffected, an array or a scalar embeds as
readily as an object, and a document with an `id` field of its own keeps it untouched inside `value`.
`fields=` projects within `value` and leaves the row's `id` alone.

### A collection that does not exist

`/query`, `/docs`, `/docs/:id` and `/aggregate` all answer `404 {"error":"collection 'x' does not
exist"}` for a name no collection answers to. Reading never creates one, and an empty page is not
substituted for the refusal: an empty page would make a mistyped collection name indistinguishable
from a collection that is genuinely empty, and the engine cannot tell which one the caller meant.

Collections are created by their first write, so an application's own collection is absent until
then. Translating that `404` into an empty result is an application decision, made at the call site
that knows the collection is the one it creates on first use — see
[Your first application](getting_started.md#a-missing-collection-is-not-an-empty-list). It is not a
rule about `404` in general: the same status from `/docs/:id` ordinarily means the document is gone.

Subscribing is the exception, and deliberately so: `/changes` opens on a collection that does not
exist yet and waits at position `0`. See [Change streams](#17b-change-streams).

### Filters

`filter` is a JSON object mapping field paths to conditions. Paths are dotted (`profile.city`). A
document missing the path does not match, so a condition on a field is also an assertion that the
field is there; `$exists: false` and `$not` are the two exceptions.

| Form | Meaning |
|---|---|
| `{"field": value}` | exact JSON equality; also spelled `{"$eq": value}` |
| `{"field": {"$ne": value}}` | not equal, exact JSON comparison |
| `{"field": {"$in": [a,b]}}` | membership, exact JSON comparison; `$nin` is its complement |
| `{"field": {"$gt": x}}` | greater-than within one JSON type; also `$gte`, `$lt`, `$lte` |
| `{"field": {"$exists": bool}}` | the document has, or does not have, the path |
| `{"field": {"$type": "number"}}` | one of `null`, `bool`, `number`, `string`, `array`, `object`, or an array of them |
| `{"field": {"$prefix": "ab"}}` | string starts with; also `$suffix` and `$contains` |
| `{"field": {"$all": [a,b]}}` | the array holds every listed value |
| `{"field": {"$size": n}}` | the array has exactly `n` elements |
| `{"field": {"$elemMatch": {…}}}` | one element satisfies the operators, or the sub-filter, inside |
| `{"field": {"$not": {…}}}` | the field's condition does not hold |
| `{"$and": [f,…]}` | every filter holds; also `$or` and `$nor` |

Several conditions on one field, and several fields, are ANDed, and an `$and`/`$or`/`$nor` beside
them is ANDed with them too. Filters nest 16 deep, counting `$and`/`$or`/`$nor` and field-level
`$not` and `$elemMatch` alike; past that the filter is `400`.

`$gt`/`$gte`/`$lt`/`$lte` take a number or a string and compare inside that type only: `$gt: 1`
never admits `"zebra"`, and `$gt: "b"` never admits a number. Values of different types are ordered
against each other for *sorting*, where a total order is required, but a comparison operator asks a
question only its own type can answer.

Numbers compare through `f64`, so two integers that differ only above 2^53 compare equal: a document
holding `9007199254740993` does not satisfy `$gt: 9007199254740992`, and `u64::MAX` ties with
`u64::MAX - 1`. Integers below 2^53 -- every ordinary count, price, timestamp in milliseconds or
identifier under nine quadrillion -- are exact. Above it, store the value as a string if you need to
range over it, since strings compare exactly and a zero-padded decimal sorts as its number does.

`$in`, `$nin` and `$all` compare whole values. `$in` on an array-valued field asks whether that array
is one of the listed values; `$all` and `$elemMatch` look inside it.

An unsupported operator, `$in` without an array, a comparison against something that is neither a
number nor a string, range bounds naming two types, an unknown `$type` name, an empty
`$and`/`$or`/`$nor`, and operators mixed with literal keys are all `400`.

### Index use

Where a condition names a field with a ready [secondary index](#6a-secondary-indexes), the candidate
keys can come from the index instead of a walk of the key range:

| Operator | Offers |
|---|---|
| equality, `$eq` | the one posting |
| `$in` | the postings of its members |
| `$gt`, `$gte`, `$lt`, `$lte` | the band between the bounds, inside the operand's type |
| `$prefix` | the strings from the prefix up to its successor |
| `$type` with one name | that type's whole band |
| `$exists: true` | every posting, which the selectivity gate then judges |
| everything else | nothing; the condition is left to the filter |

The narrowest eligible plan is chosen, the full filter is still applied to every candidate, and a
candidate set that is most of the collection falls back to the scan. Only conditions reachable
through `$and` are eligible. Where several operators sit in one condition, the narrowest is probed
and the rest are left to the filter.

An index changes only which keys are read. `start`, `end`, `limit` and the cursor all behave
identically either way, and a sorted query uses an index for its filter only.

Every shard snapshots the current ring for the whole scan and rejects keys its group does not own.
Migration copies therefore do not consume a limit, advance a cursor, enter a sorted candidate set,
or contribute to an aggregate. The current owner remains visible through finalization; the
destination becomes visible when the ring flips.

### Key ranges and pagination

`start` and `end` bound the key range inclusively, and ascending: `start` above `end` is `400`, on
`/query` and `/aggregate` alike. An equal pair is the one key. A `cursor` is opaque, so a page
resumed past a narrower `end` than the one it was issued under is an empty `200`, not a refusal.

Every page is resumed with an opaque `cursor`, and its shape follows the query. An unsorted cursor is
a position in the keyspace: a shard returns the last key it decided — the last row it emitted, or the
last candidate its read budget was spent on — and a router returns a map of per-shard positions so
every shard resumes where it left off. A sorted cursor is a position in the sort order — one position
for the whole cluster, which needs no per-shard state and survives a shard joining mid-scan.
`next_cursor` is `null` when the page is the last one.

All three shapes are base64 JSON in the URL-safe alphabet (`-_`), because a cursor exists to be put
in a query string and `+` there is a space. Cursors issued in the older standard alphabet are still
accepted.

`src/cluster/router.rs::router_query` retains positions for unsorted shards allocated no share of the
page limit. Absence from the queried shards cannot produce `404` while these positions remain: the
response is an empty `200` page with a continuation cursor. Once no positions remain, an all-absent
batch still returns `404`; a present collection with no matching rows returns `200`.

A cursor is rejected rather than ignored when it does not belong to the query it is handed to:
`400` for a sorted/unsorted mismatch in **either** direction, and `409` for an unsorted cursor issued
against a different shard layout — the positions were correct when issued, a key that changed owners
sits behind a position that never covered it, and the scan has to start again.

Both unsorted shapes carry that layout, and both are checked. The router compares
`ClusterMetadata::partition_fingerprint` against the `ShardCursor`'s; each shard stamps its own into
the `KeyCursor` it issues and compares it against the fingerprint of the `ScanOwnership` the next
page would scan under, read from the same `cluster` acquisition as the ring, so the verdict and the
fingerprint recording it cannot come from two views. The shard's check is the load-bearing one: a
router's view and a shard's view move independently, so a destination that has adopted a flip the
router has not would otherwise pass the router's check and answer as the new owner against a
position taken while it was not. A scan paged straight at a shard gets the same refusal. The
fingerprint is optional on the wire and an absent one resumes unpinned, so a page in flight across an
upgrade completes.

A sorted cursor is deliberately outside this: it is a position in the sort order, one every shard
reads the same way, so a key changing owners mid-scan is emitted once by whichever shard owns it when
the page reaches it. Replicas are outside the fingerprint for the same reason.

`limit` defaults to 100 and is capped at 10 000 — a page is preallocated from that number, so the cap
is a hard error rather than a slow query.

`limit` bounds the rows, `max_docs` bounds the reads, and a filter separates the two: a page that
matches nothing still reads what the plan offered. The budget is the aggregation one — 100 000
documents per shard per request, up to 1 000 000, refused rather than clamped above it — charged
after the ownership test and before the read. On an unsorted page, spending it ends the page and
issues a cursor rather than refusing, because the cursor passes every candidate the page read: a
rejected row is decided, so the next page cannot lose it. On a sorted page, which reads a whole range
before it can order any of it, exhaustion returns `400` without a partial page or cursor. A page
that carries a filter or a sort shares four scan slots per node with aggregation and returns `429`
after a two-second admission wait; an unfiltered listing reads exactly `limit` and is not admitted
through them. Routers propagate these refusals rather than merging an incomplete answer.

### Sorting and projection

`sort=field[:asc|desc][,field[:asc|desc]]…` sorts on up to eight dotted paths. A later key decides
where every earlier one ties, and the document key decides where they all do, so a sorted page
resumes at exactly one position. Values are ordered by a total order across JSON types (null, bool,
number, string, array, object); numbers are compared through `f64`, so integers that differ only
above 2^53 tie and the document key then separates them. That order is what lets a router merge
pages from several shards. A shard scans the requested range, filters, sorts, then truncates to
`limit`; a router asks each shard for the full limit and performs a k-way merge, because the top
rows may all live on one shard. A router returns a cursor when any shard had more rows or when the
merge cut some.

A sort cursor carries one position per sort key, so changing the sort keys — including their number —
invalidates it, and the mismatch is a `400`. A cursor issued by an earlier version, which carried a
single bare position, is read as a one-key position.

An empty sort key, a repeated one, an unknown direction and more than eight keys are all `400`.
Directions are case-insensitive but not free-form.

`fields=a,b.c` projects the named paths, rebuilding nested structure and omitting paths a document
does not have.

`read=primary|quorum|replica` decides which node in each group answers, and what the answer
guarantees; a `quorum` page has a linearizable starting point.

---

## 17a. Aggregation

```
GET /collections/:name/aggregate
      ?filter={"tier":"gold"}
      &group=region,plan
      &metrics=count,sum:amount,avg:amount,min:signed_up,max:signed_up
      &start=u&end=v
      &read=quorum
      &max_docs=100000&partial=false
```

`filter`, `start`, `end` and `read` mean exactly what they mean on `/query`, index use included.
There is no `limit` and no `cursor`: an aggregate is not resumable, because a partial grouping merged
across shards would be wrong rather than short.

The response is:

```json
{
  "groups": [
    {"key": {"region": "eu"}, "count": 3,
     "metrics": {"count": {"count": 3},
                 "sum:amount": {"count": 3, "sum": 60.0},
                 "avg:amount": {"count": 3, "sum": 60.0, "avg": 20.0}}}
  ],
  "matched": 3,
  "scanned": 3,
  "partial": false
}
```

### Groups

`group` is a comma-separated list of up to four dotted paths. The key is an object with one member
per path, omitting the ones a document does not have — so documents lacking a path group together,
and `{}` is the group of documents that had none of them. With no `group` there is one group and no
`key` at all.

`matched` is every document the filter matched, and each group's `count` is the documents in it.
Groups come back ordered by the canonical JSON text of their key, which is the same order on every
shard and at the router.

At most 10 000 distinct groups. Exceeding that is a `400` naming the ceiling, raised by whichever of
the shard or the router reaches it first.

### The read budget

`max_docs` is how many documents one shard may read for one request; absent, it is 100 000, and above
1 000 000 the request is refused rather than clamped. It bounds *reads*, which `matched` and the
group ceiling do not: a filter matching nothing reads everything the plan offered, and groups and
documents are the same number only when every document is its own group.

It is per shard, not divided across them. A `limit` bounds the answer, which is one cluster-wide
number; a budget bounds a walk, and each node runs one over its own share of the keyspace.

`scanned` is the documents read, across every shard, and is published whether or not the budget was
reached. `partial` is `true` when a walk stopped with keys it owns unread. A budget spent is a `400`
by default; `partial=true` accepts the totals over what was read instead, and one shard's `partial`
makes the merged answer partial, because the merge cannot tell which groups the unread keys belonged
to and so knows no group to be complete.

### Admission

A node runs at most four scans concurrently -- an aggregation, a sorted page or a filtered one, each
of which spends a read budget on a blocking thread. A request waits up to two seconds for a slot and
is then `429`: a scan holds a `spawn_blocking` thread for the length of its budget, and document
reads and group commits share that pool. An unfiltered page reads exactly `limit` and takes no slot.
Waiting holds no thread, so the wait is cheap and the refusal is for a queue that is not moving.

Through a router a `429` is per node, so the fan-out tries the group's next replica and answers `429`
only when every target it had refused. It is not a `503` — that is "no primary" — and not a `502`.

### Metrics

`metrics` is a comma-separated list; absent, it is `count`. Each entry is `count`, or
`sum|avg|min|max` followed by `:` and a dotted path. At most 16, and an entry may not be repeated —
the text of the entry is the key it comes back under.

| Metric | `count` counts | Also carries |
|---|---|---|
| `count` | every document in the group | — |
| `sum:f` | documents whose `f` is a number | `sum` |
| `avg:f` | documents whose `f` is a number | `sum`, `avg` |
| `min:f` | documents that have `f` | `min` |
| `max:f` | documents that have `f` | `max` |

`sum` and `avg` are numeric and skip a document whose value there is not a number: a missing field is
not a zero, and the metric's own `count` is how many documents actually went into it. `min` and `max`
compare any JSON value, using the same total order sorting uses.

Every metric publishes the totals behind it rather than only its answer, which is what makes the
cross-shard merge exact: a router sums each group's `sum` and `count` and divides once. Averaging
shard averages weights a shard holding one document the same as one holding a thousand.

An unknown metric, `count` given a field, `sum`/`avg`/`min`/`max` without one, a repeated entry, an
empty group field and a repeated group field are all `400`.

---

## 17b. Change streams

```
GET /collections/:name/changes
```

A [server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html) stream of one
collection's committed changes, served by any node in the shard group that holds it — or, on a
router, by every group at once.

Three transports sit on one feed. SSE is this route; WebSocket is the same frames on `/changes/ws`;
[webhooks](#17c-webhook-delivery) push to an endpoint instead of holding a connection. They share the
filtering, the positions and the end conditions — `src/cdc.rs` is the one place those are stated — so
anything below about what is published, resumed or refused holds for all three unless the transport's
own section says otherwise.

### What is published, and where from

Events are produced in `Collection::apply_committed` and nowhere else. A durable entry that has not
committed can still be truncated by a leader change, and a subscriber cannot un-see an event; a
committed entry never is. Because both the leader and its replicas apply the same committed frames, a
replica publishes the same events on the same positions, behind by its replication lag.

| `op` | Published when | Carries |
|---|---|---|
| `insert` | a `Put` commits for a key the index did not hold | `key`, `value` |
| `update` | a `Put` commits for a key it did | `key`, `value` |
| `delete` | a `Del` commits for a key the index held | `key` |
| `drop` | a collection `Drop` commits | neither |

Insert and update are told apart under the index write lock, immediately before the insert, because
that is the last moment they differ. A delete of a key that was not there changes nothing and
publishes nothing, though the frame still applies and still commits. A drop is one event rather than
one delete per key: nothing bounds how many keys it removed.

Barriers, configuration entries, handovers and index definitions are not data changes and are not
published. Neither is a timestamp: a delete has no document to take one from, and a wall clock only
half the events could report would be worse than none.

### The wire format

```
event: open
data: {"collection":"orders","position":412}
retry:2000

event: change
id: 413
data: {"lsn":413,"op":"insert","key":"o1","value":{"amount":35}}

event: error
data: {"error":"…","resume_floor":998}
```

`open` arrives first and names the position the stream starts from. Each `change` carries its LSN
both in the payload and as the SSE `id`. An `error` event is the last thing a stream sends before it
ends.

### Positions and resuming

| Parameter | Meaning |
|---|---|
| `after` | Deliver committed changes above this LSN. Absent is "from now on". |
| `filter` | A document filter, in [`/query`'s syntax](#filters). |
| `ops` | Comma-separated `insert`, `update`, `delete`, `drop`. Absent is all four. |
| `read` | `primary` refuses `503` on a node that does not lead, and ends the stream in-band if that node later stops leading; `replica` and absent take whichever node the request reached. `quorum` is a `400`: a read index makes one answer linearizable, and a stream is not one answer. On a router, `replica` is a `400` too. |

`Last-Event-ID` is read as `after` when `after` is absent, so a browser's `EventSource` resumes on
its own reconnect. An explicit `after` wins.

A collection that does not exist is not a refusal here, unlike every other read: the stream opens at
position `0` against a pending feed and delivers the collection's first write when it commits. A
subscription still never creates a collection — only a write does.

A node keeps the last `changefeed.buffer_events` events per collection and no more. The log cannot
stand behind them: compaction retires superseded frames, so it holds each key's latest value rather
than the sequence of values it held. `resume_floor` is that horizon as a number — the lowest `after`
the feed will accept — and it means *every event above this position that was ever published is still
buffered*. It rises when the buffer evicts, when a document cannot be resolved, and over any stretch
the feed did not record.

### Refusals

| Code | When |
|---|---|
| `400` | A filter, `ops` value or position that cannot be parsed, an `after` above this node's committed log, or `read=replica` on a router. |
| `409` | On a router: the position was issued against a different shard layout. |
| `410` | `after` is below `resume_floor`. The body carries `resume_floor`, and on a router the `shard` whose log it belongs to. |
| `502` | On a router: a shard group could not be reached at all. |
| `503` | `read=primary` on a node that does not lead, the collection is at `changefeed.max_subscribers`, or the handle was replaced mid-request. `Retry-After` is set. |

A subscriber that falls behind while connected is told the same thing in-band — an `error` event
carrying `resume_floor` — and the stream ends. It is never served a feed with a hole in it. The bound
is the one shared buffer, not a queue per subscriber, so a slow subscriber costs the node memory
nothing.

A `read=primary` stream re-checks leadership while it runs and ends the same way when the node stops
leading, with `position` rather than `resume_floor` — nothing was lost, so the position is exact and
another node in the group will honour it.

The credential is re-checked on the same footing, every two seconds, against the key set the node
holds at that moment — see [Rotation without a restart](#rotation-without-a-restart). A key removed
from `auth.api_keys` or `auth.admin_keys` ends every stream opened with it, in-band, with
`the credential this stream was opened with is no longer accepted; resubscribe` and no position.

### Filters

`filter` governs the events that carry a document, and nothing else. A delete carries none, so
evaluating the filter against it would be inventing a match — and dropping it would be worse: a
subscriber watching `{"status":"active"}` would never learn that an active document was deleted.
Deletes and drops are therefore always delivered. `ops` is how a client that wants only writes says
so, and that one does exclude a delete, because it was named.

### Cost, and when the feed is recording

Resolving a document larger than `read_cache.inline_max_value_bytes` is a WAL read, and
`apply_committed` runs ahead of the client's reply, so recording is not free. A feed therefore
records only while it has a subscriber, plus `changefeed.idle_retention_ms` after the last one
leaves, which is what lets a dropped connection reconnect and resume exactly. With no subscriber the
cost is one atomic load per commit.

A registration is the exception to "only while it has a subscriber". A webhook is a sender rather
than a connection, so every node holding one pins that collection's feed for as long as the
registration exists -- across restarts, on followers as well as the leader. Recording is then
continuous rather than bounded by someone watching.

If that collection holds documents above the inline ceiling, raise
`read_cache.inline_max_value_bytes` past your typical document size: it is what decides whether the
feed resolves a change from the index or from the WAL, so raising it removes the read instead of
moving it, at the cost of `read_cache.inline_budget_bytes` of resident memory. Measured on one node
with the feed pinned, the penalty is under 3 ms of p50 write latency and largest on *small*
documents, where the per-event publish cost lands against a cheaper write; large documents moved
less than the run-to-run spread. Those figures are with the frames still in page cache, which is the
best case -- a working set past RAM turns each unresolved change into a real seek on the apply path.

A feed that was quiet does not silently resume across the stretch it missed: `subscribe` compares the
collection's applied watermark against the feed's own position and raises `resume_floor` to the log,
so a stale position is refused. A snapshot install replaces the collection's directory, which ends
every stream on that handle with an `error`.

### Guarantees

- **Committed only.** Nothing is published that a leader change can take back.
- **Ordered by LSN**, within one collection on one node.
- **At-least-once across a reconnect.** Resuming from a position you have processed is exact;
  resuming from one you stored before processing replays the tail. `lsn` is a dedup key.
- **History reaches back as far as the buffer**, and positions are log positions, shared by every
  node in the group.

`/metrics` reports `subscribers`, `buffered`, `position`, `resume_floor`, `published` and `overruns`
per collection under `changefeed`, and the same as `dewdb_changefeed_*` in the Prometheus exposition.

### Relaying a stream to a browser

A browser normally reaches this feed through the application's own backend rather than directly: a
`WebSocket` cannot carry a credential header at all, `EventSource` cannot carry one either, and an
API key is per client rather than per end user. The backend holds the DewDB stream and re-serves it
same-origin. A worked example is in
[Your first application](getting_started.md#a-minimal-sse-proxy); the contract a relay has to keep
is four points.

- **Forward `Last-Event-ID` upstream.** The browser resends it on its own reconnect, and this
  endpoint reads it as `after`. A relay that drops it resubscribes from *now*, and the events
  committed during the gap are never delivered — silently, because nothing about the new stream says
  it skipped anything.
- **Forward the id downstream unparsed.** It is an LSN on a shard and an opaque cluster token on a
  router, and only the issuer's format is guaranteed.
- **Release the subscription when the browser disconnects.** Nothing upstream notices a reader that
  went away; a relay that keeps the upstream request open holds a subscriber against
  `changefeed.max_subscribers` (64 per collection) for the life of the relay process, and the ceiling
  answers `503` once it is reached. `subscribers` under `changefeed` in `/metrics` is where a leak
  shows.
- **Relay the bytes, not a re-rendering of them.** The stream is already well-formed SSE. Parsing it
  into objects and re-emitting them drops the `retry:` hint, the ids that make a resume work, and
  the keep-alive comments that hold idle connections open.

Two `EventSource` behaviours on the browser side follow from the standard rather than from DewDB,
and both fail quietly. `onmessage` handles only events with no name, and every event here is named,
so it never fires; and `open` and `error` are `EventSource`'s own event names as well as this feed's,
so a listener on either sees the browser's connection events too. Those carry no `data`, which is
what tells them apart.

A refusal is an ordinary JSON response, not a stream — a relay should pass the status through rather
than opening an SSE response and reporting the failure inside it.

### Cluster-wide streams

On a router the same route holds one upstream subscription per shard group and merges them. Nothing
about the events changes; what changes is that there are several logs behind them.

```
event: open
data: {"collection":"orders","shards":["http://s1:8081","http://s2:8082"],"position":"<token>"}

event: change
id: <token>
data: {"lsn":413,"op":"insert","key":"o1","value":{…},"shard":"http://s1:8081"}

event: topology
data: {"ring":8842…,"shards":[…],"added":[…],"removed":[…],"position":"<token>"}

event: error
data: {"error":"…","shard":"http://s1:8081","resume_floor":998}
```

**`shard`** names the group that published the event. It is not decoration: `lsn` is a position in
that group's log and in no other, so the two are only meaningful together.

**The position is a token**, not a number: one LSN per group plus a fingerprint of the shard layout
they were taken against, base64url-encoded. It is the SSE `id` of every event, so `?after=<token>`
and a browser's `Last-Event-ID` both resume each group where it stopped. Treat it as opaque.

**Each group is subscribed through its leader.** Not for freshness: every node in a group numbers the
same log, and a replica's feed trails it, so a leader's position resumed against a replica is "above
this node's committed log". Asking for the leader makes a failover a reconnect at the same number —
the router walks the group's candidates the way a forwarded write does, finds the promoted replica,
and the subscriber sees nothing. `read=replica` is refused rather than downgraded.

**A shard layout change is answered twice over.** Under a live stream the router re-plans — a group
that entered the ring gets a subscription, one that left has its subscription dropped — and emits
`topology` with the new fingerprint and a position stamped with it. Nothing is lost at that seam: a
group that was not an owner in any view accepted no client write, since the shard-side ownership
check refuses those, so everything it holds arrived by handover. A position presented *after* a
change is `409` instead: the subscriber was away across the seam.

**Handover writes are silent.** Migration puts and deletes carry `migration: true` in their WAL
payload. Commit application updates documents and indexes but omits their CDC events, including on
replicas and after recovery. Client writes still publish; topology events are unchanged. Old WAL
entries without the marker remain ordinary writes. Upgrade all shard nodes before relying on
suppression: older binaries ignore the new field and still publish movement events.

**A subscription can precede the collection.** SSE and WebSocket open at zero on a group that holds
none of the collection, using an in-memory feed that the first write adopts. The subscription creates
no collection directory or listing entry. This also applies when every group is empty: the stream
opens and waits. Subscriber limits apply before creation. Routers retain a retry path for older
nodes.

| Guarantee | Cluster-wide |
|---|---|
| Ordering | Per group. Two groups' logs are independent, so no total order is offered or invented. |
| Delivery | At-least-once, per group, the same as a single feed. `(shard, lsn)` is the dedup key. |
| Resume | Exact per group while the shard layout stands; refused `409` once it has moved. |
| Failover | Transparent. The position is the group's, not the node's. |
| Migration | Continues, and reports the movement. |

A cluster-wide stream can miss up to a second of a collection's first changes on a shard group that
held none of it when the stream opened.

### WebSocket delivery

```
GET /collections/:name/changes/ws
```

The same feed, for a client that cannot use SSE. `after`, `filter`, `ops` and `read` mean exactly
what they mean above, on a shard and on a router alike, and the router merges the groups the same
way.

Every frame is one JSON text message. There is no event envelope the way SSE has one, so the name and
the resume position ride inside the object:

```json
{"type":"open","collection":"orders","position":412}
{"type":"change","position":"413","lsn":413,"op":"insert","key":"o1","value":{"amount":35}}
{"type":"error","error":"…","resume_floor":998}
```

`position` is a string in every frame that carries one — an LSN on a shard, a cluster token on a
router — so a client passes it back to `?after=` unchanged.

**Refusals happen before the upgrade.** A position the buffer no longer holds is a `410` on the
handshake, not a socket that opens and closes; the whole table under [Refusals](#refusals) applies as
written. After the upgrade the only ending is an `error` frame followed by a close.

The server pings every 15 seconds, since a client that went away without closing is only discovered
by writing to it and a quiet collection gives nothing to write. Nothing the client sends is read as
input — the stream is server-push — and a close, or a disconnect that reads as one, drops the
subscription immediately.

The handshake is authorized like any other request, from `x-api-key` or `Authorization`. A browser
cannot set headers on a `WebSocket`, so a browser client reaches this feed through SSE and a
same-origin proxy, or with the credential terminated in front of the node.

The credential the handshake carried is re-checked for the life of the socket, exactly as on the SSE
stream, and a revoked one ends it with the same frame followed by a close.

---

## 17c. Webhook delivery

```
POST   /collections/:name/webhooks
GET    /collections/:name/webhooks
GET    /collections/:name/webhooks/:id
DELETE /collections/:name/webhooks/:id
```

A webhook is a CDC consumer that pushes instead of being pulled: the node holds the subscription and
posts batches to an endpoint, so a consumer need not keep a connection open or be running when a
change happens.

### Registering one

```bash
curl -X POST localhost:8081/collections/orders/webhooks \
  -H 'content-type: application/json' \
  -d '{"id":"billing","url":"https://billing.internal/dew","secret":"…","ops":"insert,update"}'
```

`id` is 1–64 of `[A-Za-z0-9_-]` and is unique per collection. `url` must be an absolute `http(s)`
URL. `filter` and `ops` are the change endpoint's own, checked here rather than at the first delivery:
a filter the engine cannot evaluate is the operator's error, and finding out about it from a
subscription that never fires is worse.

A new subscription starts *from now on*, at the collection's committed watermark. Re-posting an
existing id replaces the destination, the secret and the filter, keeps the position, and clears
whatever had stopped it — which is how a secret is rotated and how a disabled subscription resumes.

### What a delivery looks like

```
POST /dew HTTP/1.1
content-type: application/json
X-Dew-Subscription: billing
X-Dew-Delivery: 6f1c…            one id per batch, unchanged across every retry of it
X-Dew-Timestamp: 1757116800
X-Dew-Signature: sha256=9ab3…    absent when the subscription has no secret

{"subscription":"billing","collection":"orders","delivery":"6f1c…","node":"shard-1",
 "position":417,"events":[{"lsn":416,"op":"insert","key":"o1","value":{…}},…]}
```

Events are batched up to `webhooks.batch_max_events`, or until `webhooks.batch_window_ms` passes with
nothing more to add — whichever comes first, so a quiet collection is not held back waiting for a
batch to fill.

**The signature** is `HMAC-SHA256(secret, "<X-Dew-Timestamp>.<raw body>")`, hex, prefixed `sha256=`.
The timestamp is inside the signed string rather than beside it so a delivery captured off the wire
cannot be replayed later under its own signature; an endpoint should reject a timestamp far from its
own clock as well as a signature that does not match. The secret is write-only as far as the API is
concerned: it is never in a response, and an operator who lost it re-registers with a new one. It
sits in `webhooks.meta` as plaintext and travels to the group's peers in an ordinary request body, so
it is as private as the data directory and the link between nodes.

### Retries, backoff and what stops

Any non-`2xx`, and any transport failure, is retried with the same batch and the same
`X-Dew-Delivery`. The delay doubles from `webhooks.initial_backoff_ms` to `webhooks.max_backoff_ms`
and carries a per-subscription spread, so two subscriptions that started failing together do not
retry in lockstep for the rest of an outage.

There is no attempt ceiling. A bounded retry drops events, and dropping them silently is worse than a
subscription that is visibly behind — `GET /collections/:name/webhooks/:id` is where that shows. The
one exception is `410 Gone`: that is the endpoint saying to stop rather than that it is busy, so the
subscription is disabled with the reason recorded, and registering it again is how it resumes.

The other thing that stops one is the credential it was registered with no longer being accepted. A
registration is durable, so a restart does not end it the way a restart ends a connection. The node
keeps a SHA-256 digest of that key — never the key — and asks before each open and each attempt
whether one it still holds hashes to it and still clears `POST /collections/:name/webhooks`. When
none does, the subscription is disabled the same way a `410` disables it.

### Delivery state

```json
{"id":"billing","collection":"orders","url":"https://billing.internal/dew","filter":null,
 "ops":"insert,update","signed":true,
 "delivery":{"position":417,"delivered":112,"attempts":115,"failures":0,"gaps":0,
             "last_error":null}}
```

| Field | Meaning |
|---|---|
| `position` | LSN of the last event acknowledged with a `2xx` and then committed to the shard-group majority. |
| `delivered` | Events acknowledged since the subscription was created. |
| `attempts` | Requests made, retries included. |
| `failures` | The current run of consecutive failures, which is what the backoff is computed from. Cleared by an acknowledgement. |
| `gaps` | Times the feed moved past this subscription before it could read. |
| `last_error` | Why the most recent attempt failed, or why a gap was taken. |
| `disabled` | Present once an endpoint has answered `410`. |

### Guarantees and bounds

- **At-least-once across failover.** After a `2xx`, `_webhooks` commits the position to a majority
  before the local cursor advances. A crash before that commit may redeliver the batch; a promoted
  replica resumes after the committed cursor. `lsn` and `X-Dew-Delivery` remain the dedup keys.
- **The leader delivers.** The subscription is opened `read=primary`, so a step-down ends it in place
  rather than leaving two nodes pushing the same events. `delivering` in the listing says whether
  this node is the one.
- **Registered replicas hold the feed open.** A sender is not a connection, and the node that leads
  next needs retained events before it is promoted. Each node that holds a deliverable registration
  pins the same bounded collection feed; only the leader sends.
- **A backlog is bounded by the change buffer, not by memory.** An endpoint that stays down does not
  make the node grow: the feed keeps `changefeed.buffer_events`, and once delivery falls further
  behind than that, the subscription resumes at the floor, `gaps` goes up and `last_error` says so.
  Size `changefeed.buffer_events` against how long an endpoint may plausibly be down.
- **Registrations and removals commit to a majority.** The `registrations` key in `_webhooks` holds
  the complete catalogue, including an empty catalogue after the last removal. Log catch-up and
  snapshot recovery repair offline replicas and new members. Pushes accelerate reconciliation;
  `replicated_to` and `unreachable` describe those wakeups, not the commit decision. An unsettled
  write or unconfirmed quorum returns `503`; a pending write may still commit.
- **Boot recovery.** A failed catalogue read or decode leaves the supervisor running. It retries
  reconciliation every 500 ms and starts no senders from a failed round. Once the catalogue recovers,
  normal reconciliation and delivery resume without a restart.
- **Local counters are coalesced.** A blocking worker flushes dirty `webhooks.meta` state about every
  500 ms, outside the subscription lock. Failed writes remain dirty for retry. A crash can lose
  recent counters; acknowledged positions still pass through the majority-committed log.
- **A failover resumes at the last acknowledgement the promoted node can see.** Promotion waits for
  inherited staged `_webhooks` entries to settle, then opens the sender at the greater of that group
  cursor and any legacy local cursor. Budget for a redelivery around any failover: at-least-once and
  the two dedup keys are the guarantee, not the cursor.
- **Shard-local.** A router answers `501` and names the groups: a webhook is delivered by the group
  that holds the keys, and on a sharded collection each group needs its own registration.

Upgrade note: until the first catalogue change, existing local registrations remain usable. The first
successful administration request imports the current leader's legacy registrations into the
catalogue. Re-register any legacy destination that leader itself never received. Upgrade every group
member before relying on catalogue reconciliation.

---

## 18. Security

Three independent credentials, all optional and all off by default:

- **`auth.api_keys`** guards the public API. Present a key as `x-api-key: <key>` or
  `Authorization: Bearer <key>`; the explicit header wins. Any configured key is accepted, so keys
  rotate by appending one and removing the old one later. While the list is empty the public API is
  open, and a warning says so at boot.
- **`auth.admin_keys`** guards the admin surface: every `/cluster/*` route, read and write,
  `DELETE /collections/:name`, `POST`/`DELETE` on `/collections/:name/indexes`, and every method on
  `/collections/:name/webhooks` — a webhook listing names the destinations this node posts documents
  to, and its registration carries a secret, so reads are gated there too. While it is empty those
  routes fall back to `api_keys`, which is what they did before the tier existed. Same headers as
  `api_keys`, and the same rotation.
- **`auth.internal_secret`** guards `/internal/*` through `x-dew-internal-secret`. While it is unset
  those routes are open, and a warning says so at boot.

An admin key also opens the public API — the tier is a superset, so an operator does not need a
second credential to read what it is about to drop. The internal secret substitutes for neither.
Comparisons are constant-time, so a wrong value does not leak a prefix. `auth.upstream_api_key` is
the key this node presents when it calls another node's public API; on a router it must be accepted
by the shards' `admin_keys`, because a routed `DELETE /collections/:name` and a routed index
definition are both forwarded on the public path. Credentials must be non-empty printable ASCII — a
value that could not go in a header fails the boot instead of silently disabling a check.

The admin surface is topology, destruction and schema, not maintenance:
`POST /collections/:name/compact` and `/snapshot` stay on the public tier, and so do
`DELETE /collections/:name/docs/:id` and `GET /collections/:name/indexes` — listing the definitions
is a read of the collection. Route segments are counted before percent-decoding, the way axum routes
them, so an encoded slash in a collection name cannot make a drop look like a document delete.

Every outbound internal request also carries `x-dew-node: <own url>`, which names the sender. It is
identification, not authentication — the internal secret is what authorises the call — and it is what
the test-only link-fault table keys on.

`/health` stays reachable without credentials so probes work. `/metrics` does not, because it exposes
topology.

### Rotation without a restart

`auth.api_keys` and `auth.admin_keys` are re-read from the config file while the node runs. Edit the
file and remove a key; within about five seconds the node stops accepting it, and it also stops being
accepted by the subjects that were *already* admitted under it. A file that cannot be parsed, or that
carries a credential no header could hold, is refused and the set in force is kept.

`auth.internal_secret` and `auth.upstream_api_key` are baked into this node's outbound clients at
boot, so taking a new one live would leave the node presenting a credential its peers no longer
expect; changing either needs a restart, and the node warns when it sees one change under it.

Authorization is per request, and every other endpoint is a request. Three subjects outlive the
request that created them, and all three are judged again rather than only at the start:

- An **SSE change stream** and a **WebSocket** one re-check the credential the connection carries
  every two seconds, and end in-band with the same `error` frame an overrun uses:
  `the credential this stream was opened with is no longer accepted; resubscribe`.
- A **webhook registration** is durable, so a restart does not end one the way it ends a connection.
  The sender keeps a SHA-256 digest of the key the registration was created with — never the key
  itself — and asks before each open and each attempt whether a key the node still holds hashes to it
  *and* still clears `POST /collections/:name/webhooks`. When it does not, the subscription is
  disabled with `delivery.disabled` saying so. The digest is not readable back through the API. A
  registration created while the public API was open carries no digest, and stops delivering once
  keys are configured.

Leadership is the other guarantee of that shape that is re-checked, since a `read=primary` stream
that stops leading stops being able to answer at all.

Note also that a browser's `EventSource` cannot set headers, so a locked public API is reachable from
`fetch` and from ordinary clients but not from `EventSource`; the key deliberately has no
query-parameter form, which would put it in every access log along the way.

Handled outside the process: TLS, per-key authorization scopes finer than the three tiers above,
at-rest encryption, and audit logging. Terminate TLS in front of the node and restrict network
reachability of `/internal/*`; internal traffic authenticates with a shared secret sent in the clear,
so `/internal/*` reachability is the control that matters.

---

## 19. Operations and observability

### Logging

Structured `tracing` output, either human-readable text or one JSON object per line, both stamped
with `node_id` and a subsystem target: `boot`, `config`, `wal`, `storage`, `db`, `compaction`,
`maintenance`, `replication`, `replicate`, `repair`, `replica_sync`, `resync`, `election`, `vote`,
`heartbeat`, `checkquorum`, `failover`, `demote`, `read_index`, `cluster`, `membership`, `migration`,
`rebalance`, `ring`, `router`, `router_probe`, `changefeed`, `changestream`, `admin`, `auth`.
`RUST_LOG` overrides `logging.level`.

### Health

`GET /health` answers `503 degraded` with the reasons, which is what a load balancer wants:

- a shard with no open database,
- a replica with no known primary,
- a replica whose last primary heartbeat is older than `heartbeat_timeout_secs`, or that has never
  reached one,
- a router with no shards in its cluster view.

The body also carries role, leadership, term, cluster version, uptime and collection count.

### Metrics

`GET /metrics` returns JSON: per-collection documents, WAL bytes, live and dead bytes, dead ratio,
last and applied LSN, pending applies, cached documents and cache bytes, whether a compaction is
running, whether the collection is a drop tombstone, and a `changefeed` block (subscribers, buffered
events, position, resume floor, events published, overruns); a replication block; a cluster summary;
router state; and per-route request counts with errors, average and p50/p95/p99 latency.

The replication block differs by role. A leader reports term, durable LSN, commit index and the
committed watermark per collection, replica count split into voting replicas and learners, worst
replica lag, per-replica matched LSNs, repair counters (gaps, divergences, resyncs triggered),
batching counters (batches and frames sent, widest batch, frames per batch) and flow-control counters
(writes rejected, inflight slots available). A follower reports its primary, that primary's commit
index, its own lag, and seconds since the last replication.

`GET /metrics?format=prometheus` exposes the same data as Prometheus text, including `dewdb_up`,
`dewdb_uptime_seconds`, `dewdb_leader`, `dewdb_term`, `dewdb_cluster_version`, `dewdb_commit_index`,
`dewdb_durable_lsn`, `dewdb_collection_documents`, `dewdb_wal_bytes`, `dewdb_wal_dead_bytes`,
`dewdb_replication_lag_lsn`, `dewdb_replica_lag_lsn`, `dewdb_replication_gaps_total`,
`dewdb_replication_divergences_total`, `dewdb_replication_resyncs_total`, `dewdb_inflight_requests`,
`dewdb_request_latency_ewma_ms`, `dewdb_changefeed_subscribers`, `dewdb_changefeed_events_total`,
`dewdb_changefeed_overruns_total`, and a `dewdb_request_duration_ms` histogram with
`dewdb_requests_total` and `dewdb_request_errors_total` — all labelled by `node_id`, plus
`collection`, `replica`, `method` and `route` where they apply.

`/metrics` and `/collections/:name/changes` are both excluded from request metrics. A change stream
lives as long as its subscriber, and timing it would fold minutes into the latency EWMA that
load-aware replica routing reads.

### Routine operations

**Adding a replica to a group.** Start it with `membership_mode: "learner"` and `primary_addr` set,
or admit it at runtime with `POST /cluster/members` naming the primary it follows. It receives frames
immediately and is counted in no quorum.

**Promoting a learner to a voter.** Let it catch up first — `matched` per replica in `/metrics`
against the leader's tail — then send the whole set you want to `POST /cluster/configuration`, e.g.
`{"voters":[n1,n2,n3,n4]}`. Read the current set from `GET /cluster/configuration`. The change is two
log entries; a `503` means one is in force and retrying the same body finishes it.

**Removing a voter.** Demote it with `POST /cluster/configuration` naming the set without it, then
`DELETE /cluster/members?url=…`.

**Removing the leader.** The same request. A change whose target set does not name this node hands
leadership to a voter the change keeps and answers `409` with a `primary` field naming it; send the
same body there and it applies. Nothing is appended on the old leader, so a failed handover leaves it
leading and the request is safe to retry.

**Moving leadership without changing the voting set.** `POST /cluster/transfer-leadership`, with
`{"to": "<url>"}` to choose the successor or an empty body to take the readiest voter — draining a
node before maintenance, or moving office off a host that is going away.

**Adding or removing a shard group.** Publish the target ring through `POST /cluster/migrate` so the
data moves before ownership does. Use `POST /cluster/ring?dry_run=true` first to see what fraction of
the keyspace moves and between which nodes.

**Reclaiming disk.** Compaction runs automatically once a collection's dead-byte ratio and size cross
their thresholds. `POST /collections/:name/compact` forces it on the leader.

**Speeding up restarts.** Index snapshots bound replay work; `POST /collections/:name/snapshot`
writes one on demand.

**Backups.** Copy a node's `data_dir` while it is stopped, or take an index snapshot first and copy
the collection directory — a WAL plus its snapshot is self-describing, and recovery truncates a torn
tail. Bringing a shard node back into a running cluster is usually best done by letting it resync
from its leader.

**Re-seeding topology.** Config is a bootstrap seed. To make a config change to `shard_map`, `ring`
or membership take effect on a node that has already booted, stop it, delete `cluster.meta`, and
start it again — or, preferably, publish the change through `/cluster/ring`, `/cluster/migrate`,
`/cluster/members` or `/cluster/configuration`. The voting set is a separate matter: it lives in the
`_config` log, and config seeds it only until a configuration entry exists.

### Troubleshooting

| Symptom | Where to look |
|---|---|
| Write returned `202` | `acks` and `required` in the body; replica lag and `max_replica_lag` in metrics. |
| Write returned `503` with a backlog | The quorum is not acknowledging; check replica reachability and `writes_rejected`. |
| Write returned `403` | It was sent to a follower. Use the router, or the primary named in the reply. |
| Read returned `409` | The caller's view is stale; the body names the owner. |
| `read=quorum` returned `503` | The error names which step failed: no leadership, a term too new to answer at, no confirmation from a majority, or an index not yet visible. All are retryable. |
| A configuration change returned `503` | `GET /cluster/configuration`: `joint: true` means the group is deciding with both halves. Retry the same body against the leader. |
| No leader | `/health` on each shard, `dewdb_term` across nodes, and the `election`/`vote` logs. A leader that stepped down without losing its term is in the `checkquorum` logs. |
| A replica never catches up | The `repairs` counters; a rising `resyncs_triggered` means chains keep breaking, typically through compaction. |
| Nodes disagree about topology | `version` from `GET /cluster` on each node. |
| A ring change was refused | The refusal names which owner holds data; use `/cluster/migrate`. |

---

## 20. Repository layout

```
src/
├── main.rs               boot: config, storage, shared state, role tasks
├── config.rs             config schema, validation, misconfiguration warnings
├── state.rs              shared handle: storage, replication state, caches, ownership
├── model.rs              client-facing wire types
├── query.rs              filter expressions, sort keys, cross-shard merge, cursors
├── aggregate.rs          metric accumulation on a shard and the merge across them
├── changefeed.rs         the committed change feed: its ring buffer, subscribers and positions
├── cdc.rs                the change consumer every transport shares: filtering, frames, endings
├── webhook.rs            webhook registrations, durable positions, signing, retries and backoff
├── json.rs               JSON paths, ordering, projection, merge patch
├── ring.rs               hash ranges, token ring, keyspace movement
├── auth.rs               credentials and the authorization decision
├── metrics.rs            latency histograms, repair and batching counters
├── logging.rs            tracing setup and the node-stamped formatter
├── maintenance.rs        background compaction and snapshot scheduling
├── util.rs               endpoints, atomic writes, retrying file operations, base64, URL escaping
├── api/
│   ├── mod.rs            the route table
│   ├── middleware.rs     auth, the collection-name gate, latency, test-only link faults
│   ├── docs.rs           document endpoints
│   ├── changes.rs        the change-stream endpoint and its SSE delivery
│   ├── ws.rs             the same stream over a WebSocket
│   ├── webhooks.rs       webhook subscription administration
│   ├── write.rs          the local write pipeline
│   ├── collections.rs    collection administration
│   ├── indexes.rs        secondary index administration
│   ├── internal.rs       node-to-node endpoints
│   ├── members.rs        runtime membership changes and the voting set
│   ├── ring.rs           publishing a ring, previewing its cost
│   ├── migrate.rs        starting, watching and aborting a handover
│   └── observe.rs        /health and /metrics
├── storage/
│   ├── frame.rs          frame format and the replica apply verdict
│   ├── wal.rs            append, replicated apply, replay, frame read-back
│   ├── index.rs          index entries, snapshots, watermark files
│   ├── collection.rs     index, key locks, group commit, read path
│   ├── secondary.rs      secondary index definitions, postings, and the filter planner
│   ├── compaction.rs     relocate live keys, retire frozen WALs
│   └── database.rs       collections under one data directory, shared LSN counters
├── consensus/
│   ├── state.rs          durable term and vote, demotion, relinquishing leadership
│   ├── config.rs         quorum arithmetic over a configuration, and the log it travels in
│   ├── election.rs       pre-vote and vote rounds, eligibility, assuming leadership
│   ├── recovery.rs       recover newer per-collection histories before retrying pre-vote
│   ├── failover.rs       follower watchdog, leader discovery, contact quorum, stepping down
│   ├── lease.rs          what a voter grants a probing leader, and the leases a leader holds
│   ├── read_index.rs     what a leader establishes before answering a quorum read
│   ├── reconfigure.rs    membership changes as a joint entry and then its target
│   ├── transfer.rs       handing office to a voter that can take it
│   └── progress.rs       per-replica progress, contact stamps, the committed watermark
├── replication/
│   ├── protocol.rs       RPC types and refusal decoding
│   ├── stream.rs         outbound replication and gap repair
│   ├── snapshot.rs       bounded-memory collection transfer
│   └── write_concern.rs  acknowledgement requirements
└── cluster/
    ├── catalog.rs        the cluster index catalogue: gossip and reconciliation
    ├── metadata.rs       versioned topology, convergence, membership planning
    ├── ownership.rs      who may hold a key
    ├── router.rs         forwarding, failover, fan-out
    ├── probe.rs          background primary and view probing
    ├── migration.rs      source-driven key handover
    └── rebalance.rs      reconciling ownership with membership
```

`src/test_support.rs` (the live-cluster harness), `src/chaos.rs` (directional link faults),
`src/bench.rs` (benchmarks) and `src/soak.rs` (long-running durability scenarios) are compiled only
under `cfg(test)`.

---

## 21. Glossary

**Applied LSN** — how far the index reflects the log; the boundary of what reads can see.

**Collection** — a namespace of documents, created on first use.

**Commit index** — the highest LSN a quorum holds, tracked per collection.

**Cluster view** — the versioned topology (members, ownership, migration plan) every node carries.

**Configuration** — the voting set, carried as a log entry in `_config`. In force from the moment it
is appended, not when it commits.

**Delivery position** — the LSN of the last event a webhook endpoint acknowledged and the shard group
then majority-committed. It survives both process restart and leader failover.

**Divergence** — a replica holding a frame at the same log position but from a different history.

**Durable LSN** — the highest LSN this node has fsynced.

**Feed pin** — a hold that keeps a collection's feed recording for a consumer that is not a
connection, so a webhook loses nothing between one subscription and the next.

**Frame** — one WAL record: a 40-byte header plus a JSON payload.

**Gap** — frames missing between a replica's tail and the frame being sent to it.

**Group commit** — one fsync serving many queued writers.

**Joint configuration** — the transitional entry naming both the outgoing and incoming voting sets;
while it is in force every decision needs a majority of each.

**Learner** — a member that receives frames but is counted in no quorum.

**Lease** — a majority of live grants not to vote, collected on the leader's own probe, which lets it
answer a quorum read without a confirmation round.

**LSN** — log sequence number: allocated database-wide, chained per collection.

**Pre-vote** — a round asking whether a candidacy could win, held before any term is raised.

**Read index** — the log position a linearizable read is answered at, once leadership is confirmed
and that position is applied locally.

**Resume floor** — the lowest change-stream position a feed will still accept: every event above it
that was ever published is still buffered.

**Ring** — consistent-hash ownership: membership plus a vnode count, from which tokens are derived.

**Term** — a monotonically increasing leadership epoch.

**Voter** — a member counted in election and commit quorums.

**Write concern** — how many acknowledgements a write waits for before the client is told it
succeeded.
