# DewDB Features

A capability-by-capability account of what DewDB does, with the guarantee each one gives and the
bound that goes with it.

DewDB is a distributed JSON document database written in Rust: one binary, one JSON config file per
node, local files for storage, HTTP/REST as the native protocol. There is no external coordinator
and no separate metadata service.

- For a hands-on walkthrough, see [getting_started.md](getting_started.md).
- For the reference manual — configuration fields, endpoint tables, on-disk formats — see
  [documentation.md](documentation.md).

---

## At a glance

| Area | What you get |
|---|---|
| Data model | Schemaless JSON documents in auto-created collections, string keys |
| API | REST over HTTP: CRUD, bulk writes, query, aggregation, collection admin, cluster control |
| Query | Equality, comparison, string, existence, type and array predicates with `$and`/`$or`/`$nor`, dotted paths, multi-field sort, projection, cursor paging sorted or not |
| Aggregation | Shard-side `count`, `sum`, `avg`, `min`, `max`, grouped on dotted fields, merged across shards through the totals rather than the answers |
| Indexes | Per-collection secondary indexes on dotted fields, replicated as log entries and carried across shard groups by a cluster-wide catalogue, used for equality and range filters |
| Change streams | A per-collection feed of committed inserts, updates, deletes and drops, filtered like a query, resumable from the position each event carries, and cluster-wide through a router with one position per shard group |
| CDC delivery | The same feed over SSE, WebSocket, or webhooks that push signed batches to an endpoint with retries, capped backoff and a group-durable delivery position |
| Durability | Append-only WAL, CRC per frame, group commit fsync, torn-tail recovery |
| Consistency | Quorum commit index per collection; reads see committed state only, and `read=quorum` is linearizable |
| Replication | Leader-driven, pipelined, self-healing, with tail truncation, gap repair and snapshot fallback |
| Availability | Pre-vote elections, automatic failover, leader leases, a leader that steps down when it cannot reach a quorum |
| Write safety | `w=1｜majority｜all｜N` with `wtimeout`, and an explicit `202` when the concern is unmet |
| Scale-out | Consistent-hash ring or explicit ranges, online migration, optional auto-rebalancing |
| Membership | Runtime join as a learner, and voting-set changes through joint consensus |
| Operations | Structured logs, JSON and Prometheus metrics, health checks, background maintenance |
| Security | API keys or bearer tokens on the public API, an admin tier over topology, drops and index definitions, shared secret on internal routes |

---

## 1. Documents and collections

- **Schemaless JSON.** Any JSON value can be stored under a string key. Objects are the usual case;
  arrays, numbers, strings and booleans are stored as given and returned identically in meaning.
- **Collections created on first use.** No create step, no schema, no migration to declare a new
  namespace.
- **Server or client keys.** `POST` mints a UUIDv4; `PUT` takes the key you supply. On a sharded
  cluster a `POST` that lands on a shard which does not own the drawn id draws another, rather than
  refusing a write whose key was the server's to choose.
- **Full replace and merge patch.** `PUT` replaces a document; `PATCH` applies JSON Merge Patch, so
  a `null` field removes it, nested objects merge, and anything else replaces.
- **Deletes are log entries.** A delete appends a tombstone, so it replicates, commits and recovers
  exactly like a write, and reports whether the document existed.
- **Independent key spread.** Routing hashes `collection:key`, so two collections distribute across
  a cluster independently of each other.
- **Drops are log entries too.** Dropping a collection commits by quorum like a write, so it
  survives a leader change and reaches a replica that was down for it, instead of being fanned out
  beside the log.
- **Reserved names.** Names beginning with `_` belong to system logs — `_config` carries the voting
  set — and are refused on the public API and hidden from collection listings.
- **Bounded collection names.** A name is 1–128 characters of `a-z0-9._-`, not `.`-prefixed, not
  ending in `.`, `.tmp` or `.old`. Uppercase letters, trailing dots and Windows device names are
  rejected to enforce a portable canonical identity free of filesystem aliases. The name is checked
  after percent-decoding, so the check and the handler judge the same string, and again before it
  becomes a directory under the data root. A name that fails is `400`.
- **Reads do not create collections.** A write to a collection that does not exist creates it; a
  read, query, compact or snapshot of one answers `404`. A typo in a read cannot leave behind a
  directory, a WAL and a commit task that nothing evicts. A collection that exists but will not open
  is a `500`, not a `404`. The same holds on the internal tier: serving a peer's snapshot request is
  not a way to create the collection it named.
- **A collection may be narrower than the ring.** With few enough keys, some shard's share never
  takes one, so it holds no such collection at all. The router counts that as absence rather than as
  a failed shard: a query merges what the other shards returned, maintenance reports the node as
  having nothing to do, and only every shard saying so is the `404` the client sees.
- **Nothing unrecognised is reinterpreted.** An unknown `?w=`, `?read=`, or a cursor that does not
  belong to the query it is handed to, is a `400` rather than a silent default. A client that asked
  for durability and was answered `200` got the durability it asked for.
- **One node is one node.** Host comparison is case-insensitive, and everything keyed by, ordered by
  or hashed from a node uses the same rule — ring tokens included — so a config or a published view
  that spells one node two ways cannot give it two votes, two ring positions, or a placement that
  changes when only the spelling did. A hostname and an address are still two names, and matching
  them stays an operator constraint.
- **Keys survive routing.** The router percent-encodes each path segment it forwards, so a key
  containing `/`, `?` or `#` reaches the shard as the key the ring hashed.

**Guarantee.** A single document write is atomic: it becomes durable as one frame or not at all.
**Scope.** Writes are per document — there are no multi-document transactions, and no per-document
version or compare-and-set token.

---

## 2. REST API

- **HTTP is the protocol.** curl, browsers and any HTTP client are first-class; there is no driver
  to install and no binary wire format to implement.
- **Document endpoints** for create, replace, merge-patch, read, delete, and a paged
  whole-collection listing on a shard.
- **Bulk writes** accept an array of documents, optionally with ids, and land as one group commit.
  They answer `201` when every item met its write concern, and `207` when some did not. Ownership is
  checked for every key before anything is written, so a batch is accepted whole or refused whole; a
  router splits a batch by owning group, sends the parts in parallel, and reassembles results in
  request order so each result lines up with the document that produced it. A group that refused
  rather than answered carries its status and message through the merge: each of its items names the
  shard's `code`, and a refusal that answered for the whole batch becomes the router's own status
  instead of a `207`. A slice refused for ownership is retried once at the owner the shard names —
  the same redirect a single write follows — so a stale ring costs a batch nothing; the retry cannot
  half-apply it, because the target re-checks every key before writing any of them.
- **Query and aggregation** as two GETs on a collection: `/query` returns documents a page at a
  time, `/aggregate` returns totals over the same filter.
- **Collection administration** at runtime: list (a union across shard groups that refuses rather
  than hiding a group it could not reach), drop (a replicated log entry, so it takes `?w=` and
  answers `202` when the quorum is short, through a router as well as on a shard), force compaction,
  and write an index snapshot. None of these need a restart.
- **Secondary index administration** at runtime: create, list and drop per-collection indexes on
  dotted fields. A definition is a replicated log entry like a drop, so it takes `?w=`, answers
  `202` on a short quorum, and reaches a replica that was down for it — and a cluster-wide catalogue
  carries it to a shard group that gains the collection later. `POST` and `DELETE` sit in the admin
  credential tier; listing does not.
- **Webhook administration** at runtime: register, list, inspect and remove per-collection delivery
  endpoints. Every method sits in the admin credential tier, listing included, because a listing
  names where this node posts documents and a registration carries a signing secret.
- **Cluster control** for membership, the voting set, ring publication, migration and rebalancing
  status.
- **Status codes that say what happened.** `202` when a write is durable but its write concern was
  not met, with `acks` and `required` in the body. `403` for a direct write to a follower. `409`
  naming the real owner when a shard does not own the key. `503` with `Retry-After` for a
  replication backlog, a key mid-handover, a guaranteed read this node cannot answer right now, or a
  write whose leadership term went away underneath it. `207` when a fan-out partially failed, or
  when a bulk write's items did not all meet their write concern — the status is the part a client
  acts on. Within one shard group a batch meets its concern together or not at all, since one
  replication round decides the whole run. A bulk write no shard accepted answers with the shard's
  status, not `207`: `413` for a batch wider than the uncommitted bound stays the `413` that says a
  retry is pointless.
- **Losing leadership mid-write is refused, at every concern.** Leadership is checked in the
  handler, before the write gate and the key locks; the append is fenced again when it happens and
  the acknowledgement is fenced against the term it was made under. A node deposed while a handler
  waited answers `503` and appends nothing, and one deposed after its append answers `503` rather
  than acknowledging an entry the next leader will truncate. `w=1` is why the recheck matters: the
  local node is the whole quorum there, so no arithmetic on acks can notice.
- **Size limits that agree with each other.** A 2 MiB ceiling on any public request body, a 10 MiB
  ceiling per stored record, and an internal replicate limit sized to carry a full record
  base64-encoded. The chain is derived from one constant and asserted at compile time, so a document
  the write path accepts is always one the replication path can carry.

**Scope.** JSON bodies, no streaming request bodies, and a 2 MiB ceiling per request body.

---

## 3. Query engine

```
GET /collections/:name/query?filter={"age":{"$gte":30}}&sort=age:desc&fields=name,city&limit=50
```

- **Filters** on dotted paths, in five families: equality (`$eq`, `$ne`, `$in`, `$nin`), ordered
  comparison (`$gt`, `$gte`, `$lt`, `$lte`) over numbers or strings, shape (`$exists`, `$type`),
  strings (`$prefix`, `$suffix`, `$contains`), and arrays (`$all`, `$size`, `$elemMatch`).
  Conditions on one field and across fields are ANDed; `$and`, `$or` and `$nor` nest, and `$not`
  negates one field's condition. A document missing the path does not match — `$exists: false` and
  `$not` are the two ways to select for absence.
- **Ordered comparison stays inside one JSON type.** `$gt: 1` never admits `"zebra"` and `$gt: "b"`
  never admits a number: they are different kinds of thing, not different points on one line.
  Integer comparisons retain their full signed or unsigned 64-bit precision, including above 2^53. A
  condition whose bounds name two types is refused rather than answered empty.
- **Secondary indexes accelerate equality, range, prefix, type and existence filters.** Where a
  condition names an indexed field with `=`, `$in`, an ordered comparison, `$prefix`, a single
  `$type`, or `$exists: true`, the planner takes the candidate keys from the index instead of
  walking the key range, then applies the whole filter to each one. It narrows which keys are
  *read*, never which rows are returned, so an index makes a query faster and the suite asserts the
  two answers agree. Conditions reachable through `$and` are eligible. See section 3a.
- **Key ranges.** `start` and `end` bound the scan inclusively and ascending over the key order; a
  reversed pair is a `400` rather than an empty page. A cursor carried past a narrower `end` is an
  empty page, because a client cannot read the key inside one.
- **Cursor pagination, sorted or not.** Every query returns an opaque `next_cursor`, and its shape
  follows the query. An unsorted cursor is a keyspace position: the last key a shard decided about —
  emitted, or read and rejected — or a per-shard position map from a router so every shard resumes
  exactly where it stopped. A sorted cursor is a position in the sort order — one position for the
  whole cluster, needing no per-shard state and surviving a shard joining mid-scan. `null` means the
  page was the last one, and a cursor handed to a query it does not belong to is rejected rather
  than ignored. Unsorted router pages can be empty with a continuation cursor when the limit leaves
  some shards unqueried, and so can a shard's own page once it has spent its read budget.
- **An unsorted scan is pinned to the layout it began on.** Both unsorted cursors carry the
  partitioning they were taken against — the router's position map and each shard's own position —
  and a page asked for under another one is a `409` telling the client to restart the scan, never a
  key returned twice or silently dropped. The shard checks its own, so the refusal holds for a scan
  paged straight at a shard and for one whose layout moved before the router saw it. Replicas are
  deliberately outside the fingerprint: one joining or leaving moves no key.
- **Bounded in reads as well as rows.** `limit` bounds the answer and `max_docs` bounds the walk
  behind it — 100 000 documents per shard per request by default, 1 000 000 at most — because a
  filter matching nothing still reads what the plan offered. An unsorted page spends it and stops,
  handing back a cursor past everything it read; a sorted page, which reads a whole range before it
  can order any of it, is refused instead.
- **Sorting** on up to eight dotted paths, each ascending or descending, using a total order across
  JSON types. Later keys decide where earlier ones tie, and the document key decides where they all
  do. Numeric order compares integers exactly rather than rounding them through a float. That total
  order is what lets a router k-way-merge pages from several shards into one correctly ordered
  result — and why a router asks each shard for the full limit rather than a share of it, since the
  top rows may all live on one shard.
- **Nothing is reinterpreted on the way in.** An unknown sort direction, an empty sort key, a
  repeated one, an unknown operator, a `$type` naming no such type, a mismatched cursor: each is a
  `400`.
- **Projection.** `fields=a,b.c` returns only the named paths, rebuilding nested structure and
  omitting paths a document does not have.
- **Read preference.** No preference tries the primary and then replicas; `read=primary` refuses
  unless the answering node believes it leads; `read=quorum` establishes a read index first, so the
  answer cannot come from a replaced leader; `read=replica` spreads reads across replicas and
  tolerates staleness. An unknown value is an error, not a silent fallback.
- **Keys on request.** `keys=true` returns each row's key alongside it — the cross-shard merge needs
  them, and a client that pages by key can have them too.
- **Owner-only scans.** During migration, a destination's committed copy stays invisible until the
  ring gives it ownership. Query limits, cursors, sorting and aggregation all skip that copy, so a
  router sees exactly one row and one metric contribution while both groups hold the key.
- **Bounded pages.** `limit` defaults to 100 and is capped at 10 000, so an oversized page is a
  clear error rather than a slow query.

**Scope.** Sorting reads and orders what the filter selected; `$contains`, `$suffix`, array
predicates and negations are evaluated per document. A `read=quorum` page has a linearizable
starting point — the guarantee covers where the page starts, not the whole walk.

---

## 3a. Secondary indexes

```
POST   /collections/:name/indexes        {"name": "by_age", "field": "age"}
GET    /collections/:name/indexes
DELETE /collections/:name/indexes/:index
```

- **The definition is a log entry.** It travels the ordinary replication path, so a definition
  commits under a quorum, survives a leader change, reaches a replica that was down for it, and
  rides `applied.meta` past the compaction that retires its frame — the same treatment a drop and a
  configuration get, for the same reason.
- **The postings are derived.** Nothing about them is written to disk. A node that opens a
  collection rebuilds every index it holds a definition for, which is why no fsync ordering can
  leave an index disagreeing with the documents it indexes.
- **Definitions take effect where they are appended; builds take effect where they commit.** Every
  write above a definition stages the values that index asks for, so committing it costs no read.
  The build walks what is committed below it. Between the two, no key is missed and none is indexed
  twice.
- **A build runs in the background** and the index is used once it finishes, so a query is never
  answered from a half-filled index. Writes during the build maintain it, and the write always
  wins: what the walk holds for a key it touched is the value that write replaced.
- **Maintained on every path that changes a key** — create, replace, merge-patch, delete, bulk
  write, replicated apply, replay, collection drop — because all of them meet at one place, the
  commit that publishes the key.
- **Dotted fields, indexed by value.** A document without the path is not indexed, which is sound: a
  filter condition cannot match a field a document does not have. An explicit `null` is a value and
  is indexed.
- **The planner picks the narrowest of the eligible indexes** and falls back to the scan when the
  candidate set is most of the collection — materialising every key to save no reads is slower than
  the walk it replaces. Equality, `$in`, an ordered comparison, `$prefix`, one `$type` and
  `$exists: true` each name a span of the postings; `$ne`, `$nin`, `$contains`, `$suffix`, the array
  predicates and `$not` name none, because a complement is the whole index and array elements are
  filed as one value. A condition that names no span is left to the filter, which every candidate
  goes through anyway.
- **Definitions survive a change of owner.** A cluster-wide catalogue records what indexes each
  *collection* has, separately from which shard group holds its keys today. A group that gains the
  collection afterwards — a new shard, a handover, a rebalance, a restart — appends the definitions
  it is missing through the ordinary quorum path and builds them before the planner may select
  them. Creating and dropping an index updates the catalogue and the groups that own the collection
  now; the catalogue is what reaches the ones that own it later.
- **A missing or rebuilding index falls back to the scan, never to partial results.** A group still
  catching up answers the same rows, more slowly, and `GET /collections/:name/indexes` reports the
  index as `building` until every group can use it.
- **Index listings require every group to answer.** The router falls back from the effective primary
  to the original primary and replicas. An unanswered group yields `502`, never partial counts or
  readiness. A group's `404` means absent; replica catalogue reads can lag.
- **Bounded.** Eight indexes per collection, 64 bytes of index name, 256 bytes and 16 segments of
  field path, and 4096 collections in the catalogue.
- **One unusable entry costs the entry.** A catalogue entry that fails the current name, index or
  field-path rules is dropped with a warning wherever it is read, rather than making the cluster view
  carrying it unreadable. Those rules tighten between releases, and a rolling upgrade keeps routing.

**Scope.** Indexes are single-field and by value, with arrays filed as one value. One index answers
one query: candidates from two are not intersected, and an `$or` is answered from the scan. The
catalogue converges deterministically rather than by agreement, so two definitions of the same index
made at once on different nodes settle on one of the two — the same property the cluster view has.

---

## 3b. Aggregation

```
GET /collections/:name/aggregate?filter={"tier":"gold"}&group=region&metrics=count,sum:amount,avg:amount
```

- **Evaluated on the shard.** Each group answers over the documents it holds and returns partial
  totals; nothing streams whole documents to a client to be folded there.
- **Merged through the totals, not through the answers.** Every metric travels with the count behind
  it, so a router sums `sum` and `count` across the groups and divides once. An average of shard
  averages is not an average, and this is the shape that makes that unrepresentable.
- **`count`, `sum`, `avg`, `min` and `max`,** the last two ordered by the same total order sorting
  uses, so they work on any JSON value. `sum` and `avg` are numeric and count the documents holding
  a number there — a missing field is not a zero.
- **Grouped on up to four dotted paths.** The key is an object with one member per field, omitting
  the ones a document does not have, so documents lacking a path group together instead of
  vanishing. No `group` is one group over everything the filter matched.
- **The same filter, index use and read preference as a query.** An indexed filter narrows what the
  aggregation reads exactly as it narrows what a page reads.
- **Bounded, and explicit about it.** At most 10 000 distinct groups and 16 metrics. Exceeding the
  group ceiling is a `400`, on the shard and at the router alike: an aggregate is not resumable, so
  a truncated grouping merged across shards would be wrong rather than merely short.
- **Bounded in reads, not only in answers.** `max_docs` is how many documents one shard may read for
  one request — 100 000 by default, 1 000 000 at most — because groups and documents are the same
  number only when every document is its own group, and a filter matching nothing still reads
  everything the plan offered. It is per shard, since a budget bounds a walk where a `limit` bounds
  an answer. Every response carries `scanned` beside `matched`, so the cost of a request is in the
  answer rather than something to guess at.
- **A spent budget refuses; `partial=true` opts in.** The default is a `400` naming the remedies.
  `partial=true` answers with the totals over what was read and `"partial": true` to say so, and one
  shard stopping short marks the merged answer — the merge cannot tell which groups the unread keys
  belonged to, so it knows no group to be complete.
- **Admitted, four at a time per node.** A scan holds a blocking thread for as long as its budget
  lasts, and document reads and group commits share that pool. A fifth request waits two seconds for
  a slot and is then `429`; through a router that means every target in some group was busy, which
  is neither `503`'s "no primary" nor a `502`.

**Scope.** The metric set is `count`, `sum`, `avg`, `min` and `max` over up to four grouping fields.
An aggregation walks its range each time rather than reading a cached rollup, and, like a query, it
is not a snapshot. A range larger than the budget is narrowed, given a larger `max_docs`, or
answered as a labelled partial.

---

## 3c. Change streams and CDC delivery

```
GET  /collections/:name/changes?after=412&filter={"status":"active"}&ops=insert,update
GET  /collections/:name/changes?after=<cluster position>   # through a router
GET  /collections/:name/changes/ws                          # the same frames over a WebSocket
POST /collections/:name/webhooks                            # pushed to an endpoint instead
```

- **Subscribe before creation.** SSE and WebSocket can wait for an absent collection, including
  through a router, and capture its first writes without creating storage or a listing entry.
- **Committed changes only.** Events are published where entries are applied, never where they are
  appended. A durable-but-uncommitted entry can still be truncated by a leader change, and a
  subscriber cannot un-see an event.
- **One event per committed entry that changed something**: `insert`, `update`, `delete` or `drop`.
  Inserts and updates carry the document; a delete carries the key; a drop is one event, not one
  delete per key.
- **Three transports, one feed.** SSE, WebSocket and webhooks share the filtering, the positions and
  the end conditions, because they consume the same change-capture interface rather than the feed
  directly. A WebSocket carries the identical frames as JSON text messages, with the name and the
  resume position inside each object; its refusals are answered on the handshake, so an unusable
  position is a `410` rather than a socket that opens and shuts.
- **Every event is a resume position.** The event's `lsn` is also its SSE `id`, so `?after=<lsn>`
  and a browser's own `Last-Event-ID` reconnect both pick up exactly where the stream stopped.
- **Bounded by one buffer per collection, not a queue per subscriber.** A subscriber that falls
  behind it is ended in-band and refused `410` with the `resume_floor` that still works — never
  served a feed with a hole in it and told nothing.
- **Filtered like a query,** with one stated rule: the filter governs the events that carry a
  document. A delete has none, so it is always delivered, and a subscriber is never left believing a
  match it was watching is still there. `?ops=` narrows by kind instead.
- **Leadership is a choice you can make.** `?read=primary` is answered by the leader or refused —
  and a stream that asked for the leader ends in-band when that node stops leading, rather than
  going quiet on a log nothing is adding to.
- **Cluster-wide through a router.** One subscription, one upstream stream per shard group, merged.
  Every event names the `shard` that published it, and the resume position is one per group stamped
  with the shard layout it was taken against.
- **Ordered per group.** Two groups' logs are independent, so the order offered is each group's own;
  a total order would mean holding events back to sort them.
- **Continues across a failover and across a rebalance.** A group is subscribed through its leader,
  so a promoted replica resumes at the same number the old one issued. A shard layout that changes
  under a live stream is announced with a `topology` event and a fresh position; one presented after
  the fact is refused `409`, because nobody was watching the seam it crossed.

**Webhook delivery** is the same feed for a consumer that is not running when the change happens:

- **Signed batches.** Each delivery carries `HMAC-SHA256(secret, "<timestamp>.<body>")` — the
  timestamp inside the signed string, so a delivery captured off the wire cannot be replayed later
  under its own signature. The secret is write-only and never appears in a response.
- **Retried until acknowledged**, with the same batch and the same delivery id, on a delay that
  doubles to a cap and carries a per-subscription spread so two failing subscriptions do not retry
  in lockstep. There is no attempt ceiling: a bounded retry drops events, and dropping them silently
  is worse than a subscription that is visibly behind. `410` is the exception — that is the endpoint
  saying to stop, so it is disabled with the reason recorded.
- **A group-durable position, and its state beside it.** After a `2xx`, the position advances
  through a majority-committed `_webhooks` entry before the local cursor moves, so a restart or
  promoted replica resumes where the endpoint got to; `delivered`, `attempts`, `failures`, `gaps`
  and `last_error` say what has been happening since.
- **Reconciled registrations.** Registrations and removals commit to a majority (`503` if
  unconfirmed, possibly still committing), and replicas recover them through catch-up. Local
  counters flush on a blocking worker about every 500 ms.
- **Boot recovery.** Webhook supervision survives a failed boot catalogue reconciliation. It retries
  every 500 ms and starts senders once reconciliation succeeds.
- **Bounded by the change buffer, not by memory.** An endpoint that stays down does not make the
  node grow. Once delivery falls further behind than `changefeed.buffer_events`, the subscription
  resumes at the floor and `gaps` records that it happened.
- **The leader delivers.** Every registered replica pins the bounded feed, so the shared cursor
  still names retained events when one of those replicas becomes leader.

**Scope.** History reaches back as far as the buffer — the log is compacted by key, so it holds each
key's latest value rather than the sequence of values it held. Delivery is at-least-once, and events
carry no timestamp. Keys moved between groups by a handover are excluded from every CDC transport.
Webhook registrations and removals are majority-committed in the group log and recovered by replicas
through catch-up; a sharded collection takes one registration per group. A browser authenticates an
SSE stream through a same-origin proxy, since it cannot set headers on a WebSocket handshake.

---

## 4. Storage and durability

- **Append-only write-ahead log** per collection, rotated at 50 MiB. Every frame carries its length,
  a CRC32 of the payload, the leadership term, its LSN, and the LSN and term of its predecessor *in
  that collection*.
- **CRC on every frame, checked on every read.** A frame read back from the WAL is validated against
  both the length the index recorded and its own CRC before its bytes are used.
- **Group commit.** Concurrent writers share one fsync. The commit task wakes on the first waiter,
  on a batch of 32, or on a 5 ms tick, so a lone write pays one fsync of latency while a busy
  collection amortises one fsync across many. The fsync reports exactly the log tail it covered,
  never a fresher read that would include appends it missed. Each waiter batch is detached before
  syncing; later waiters stay queued for the next sync and cannot inherit the earlier result.
- **Parallel writes.** Key locks are striped 64 ways, so writes to distinct keys do not queue behind
  each other. A bulk write sorts and dedupes its stripes before locking, so it cannot deadlock
  against itself.
- **In-memory index with an inline cache.** Keys map to frame locations in a sorted map, giving
  ordered scans and range queries for free. Values at or below 512 bytes (configurable, with a
  64 MiB budget) are cached inline in the index entry, so small-document reads and full scans do not
  become one random read per key. The budget covers uncommitted writes too: a staged frame's inline
  copy reserves against it from the append and gives the bytes back when the frame commits or is
  truncated.
- **A torn write does not poison the offsets after it.** A frame whose write fails part-way leaves
  bytes the size counter is not past; the WAL rotates so the tear is the last thing in its file,
  where replay takes it and nothing above it, and the writer continues on a clean one.
- **A stale collection handle refuses reads.** A handle whose directory a snapshot install replaced
  errors instead of reading its cleared index and reporting the collection empty.
- **Crash recovery by design, not by repair tool.** Boot loads an index snapshot if one exists and
  replays from its resume point, or replays everything otherwise. Replay stops at the first frame
  with an implausible length, a short payload or a bad CRC and truncates there: a torn tail is the
  expected shape of a crash.
- **Index snapshots** bound replay work. They are written atomically through a temp file plus
  rename, so a crash mid-write leaves the previous snapshot intact, and they are taken on any node,
  leader or not.
- **Tail replacement survives restart.** Truncation reconciles saved tail metadata before cutting
  the WAL; recovery prefers surviving replayed frames even when their LSN is lower.
- **A failed truncation is fenced, not forgotten.** The cut is recorded in `wal.cut` before any entry
  or WAL byte moves, and re-applying it is idempotent. If the truncation fails part-way the
  collection refuses appends and snapshot streaming until the recorded cut completes, and boot
  finishes a cut the previous process did not, rather than replaying frames the cut deleted.
- **Snapshot install survives a crash between directory renames.** Boot reconciles the live
  collection directory with `<name>.tmp` and `<name>.old` before serving; a missing live directory is
  restored from the backup or the staged snapshot rather than listed as empty.
- **Non-blocking compaction.** Live keys are rewritten into a fresh WAL while writes continue in a
  new active file — nothing is paused. The index is remapped only for entries that still point at
  the copied locations, so a concurrent overwrite wins, and the new index snapshot is the pivot
  after which a failed unlink is wasted disk rather than a resurrected key. Compaction and a
  snapshot install rewrite the same directory, so they are interlocked: each checks on entry, before
  the writer swap and before the publish, whether the other has already replaced the collection
  under it.
- **Compaction keeps the frames a replica still needs.** A run is given a floor at the lowest
  position any replication target has acknowledged, and the frames above it are copied into the
  compacted output instead of dropped — so a replica a few frames behind repairs from frames rather
  than paying a full-collection snapshot. The tail is bounded by `maintenance.wal_retention_bytes`;
  a target below what fits resyncs from a snapshot. The output comes out in LSN order, which is what
  lets the next run find its boundary in a single walk, and a run that would not reclaim enough to
  pay for its own rewrite is refused before it rotates.
- **A leader can always establish where a replica is.** With a backlog it streams one; with none and
  no acknowledgement on record — after a promotion, or after a snapshot install, neither of which
  produces one — it re-sends its tail frame to be answered. A collection the leader holds no cursor
  for at all is answered the same way: the refusal names the position the next tick streams from.
  The answer is one round trip away, so a commit index can trail its quorum by a driver tick.
- **Entries that are not keyed survive compaction.** A promotion barrier, a drop and a configuration
  are never in the index, so compaction retires their frames like any other superseded frame; the
  drop tombstone and the newest committed configuration are recorded next to the applied watermark,
  where replay finds them again.
- **Initial watermark failure rejects the append.** Local and replicated writes wait for the first
  `applied.meta` write to finish before changing the WAL. A failed write can be retried; it cannot
  leave uncommitted frames that recovery would publish as legacy data.
- **The applied watermark is durable before the client is told the write succeeded.** It is also the
  floor below which a log truncation is refused, so a watermark lagging a crash cannot return
  published entries to the pending set and let the next leader delete them.
- **Commit persistence errors reach the caller.** Writes, bulk writes and drops return `500` when
  their commit recovery state cannot be saved. Reapplying the same committed position retries the
  save even when its entries have already become visible in memory.
- **Corrupt applied positions fail recovery.** Bad magic, bad checksums and nonzero malformed slots
  are damage; only complete zero-filled slots are unwritten. Recovery accepts one intact slot in the
  1024-byte file, and refuses damage with no valid slot or an unexpected file size.
- **And it is cheap, because the position and the facts beside it are stored apart.** The drop
  tombstone, configuration and handover record go in a file written atomically through a staging
  file, since once compaction retires their frames it is the only copy of them — a filesystem
  metadata transaction rather than a data flush. The position moves on every commit and goes to its
  own pre-allocated two-slot file updated in place, at roughly a third of the cost. Alternating
  slots with a CRC each replace the staging file: a torn write damages one slot and the other still
  holds the previous position. Either file present but damaged fails the open rather than being read
  as "no consensus history", which would publish the whole log. Concurrent commits still share one
  fsync.
- **Background maintenance.** Compaction fires when a collection's dead-byte ratio and total size
  both cross their thresholds; index snapshots are written on an interval and skipped when nothing
  was appended. Both are configurable, and the whole scheduler can be turned off.
- **Clean shutdown.** `Ctrl-C` fsyncs pending group-commit waiters and persists the durable LSN
  before exit.
- **Concurrent reads.** Reads and scans run on a blocking pool, and every WAL file is opened four
  times so readers do not serialise on one file handle.

**Guarantee.** A write acknowledged with `w=1` is on disk, survives a crash of that node, and was
appended by a node that still held the leadership term at the moment it answered.
**Scope.** Values are read back from the log rather than from a page-cached B-tree, so a large-value
random read costs one seek unless it fits the inline cache.

---

## 5. Consensus and failover

- **Durable term and vote.** Term, leadership and vote are persisted as one value, because a term
  without its vote would allow the same term to be voted twice after a restart. A node never acts on
  a term or a vote before it is durable: a candidacy that cannot be persisted is abandoned, and a
  vote that cannot be persisted is denied rather than promised. Out-of-order saves cannot erase or
  change a persisted vote within its term; leadership changes can still be saved with the same vote.
- **Vote-based election with a majority.** A follower that loses leader contact waits a per-node
  jitter — derived from its id and the clock, so a shared timeout does not split the vote every
  round — then looks for a live leader at a term no lower than its own and follows it if there is
  one.
- **Pre-vote before the term moves.** Otherwise it asks the voters whether it *could* win, against a
  read lock and without touching a term, a recorded vote or the disk. A candidate short of a quorum
  keeps its term and vote: an isolated node does not inflate its term every cycle, and a partitioned
  or removed one does not depose a healthy leader on its way past. A peer that does not know the
  endpoint is counted as willing, so a half-upgraded cluster elects as before.
- **Per-collection log freshness.** A vote is granted to a candidate whose log is at least as fresh
  as the voter's *in every collection*. One leader serves all collections, so a candidate behind on
  a single collection could otherwise lose that collection's committed entries.
- **Recovery of complementary histories.** After a failed pre-vote, a candidate asks its voting
  group for collection tails and pulls snapshots for histories newer than its own. It can collect X
  from one survivor and Y from another before retrying the unchanged voting rule. Recovery preserves
  local commits, leaves uncommitted entries staged, and includes compacted collections and the
  configuration log. A recovered configuration takes effect before another campaign.
- **Automatic step-down.** Any higher term deposes a leader, whether it arrives in a heartbeat
  reply, a replication rejection, a vote request or a repair response. Stepping down clears
  leadership, forgets the vote, discards quorum evidence gathered in the old term, restarts the
  follower watchdog, and resyncs from the new leader. A leader deposed by *granting* a vote has no
  new leader to be told about — the candidate has not won yet — so the watchdog looks for one on
  every tick while it has nobody to poll.
- **A leader that cannot reach a quorum stands down.** Every other signal is inbound, and an
  asymmetric partition leaves all of it healthy while nothing the leader sends arrives. So a leader
  probes its replicas on a quarter of the contact window and relinquishes leadership if it cannot
  show a reply from a majority within the contact timeout — inside its own term, keeping its own
  vote, since a node that forgot voting for itself could seat a second leader in that term.
- **Quorum-driven commit index, per collection.** An acknowledgement for one collection says nothing
  about another, so each collection has its own watermark. It is the majority position over the
  leader and its voting replicas — the lower of both halves' majorities while a membership change is
  in flight — and it never moves backwards.
- **Current-term commit rule.** The watermark will not advance below the first LSN this leader
  appended in its own term. A majority holding a prior-term entry is not proof a later leader keeps
  it; such entries commit indirectly once a current-term entry above them commits.
- **Committed means visible; uncommitted means revocable.** Durable-but-uncommitted frames are
  staged, not published, so a leader that dies before reaching a quorum leaves entries no client
  ever read. The rule survives restarts: boot replays the whole tail but publishes only up to the
  applied watermark.
- **Promotion keeps what was already published.** A new leader seeds its commit watermark from what
  it has already applied, since anything published must have been committed by a previous leader.
- **Single-node progress.** A leader with no replicas commits on its own durability, so a standalone
  node is a working database rather than a cluster waiting for peers.
- **Learners never distort a quorum.** A non-voting member receives frames and can serve reads, and
  its acknowledgement is recorded and never counted; it neither campaigns nor votes.
- **Membership changes through joint consensus.** The voting set is a log entry, and a change is two
  of them: a joint entry naming both halves, then the target. While the joint entry is in force —
  from the moment it is *appended* — every decision needs a majority of each half separately, so
  there is no instant at which the leaving and joining halves can each decide alone. An entry that is
  durable but short of a quorum is reported as such and finished by retrying the same change; a
  leader that inherits a committed joint entry completes it on promotion.
- **A majority is a majority of physical nodes.** Quorum arithmetic counts one entry per node per
  half, by endpoint, so a voter list naming one node twice — or under two spellings that resolve to
  the same `host:port` — cannot let that node satisfy the threshold its own duplicate raised. The
  membership API refuses such a list outright; an entry already in a log is read back with each half
  collapsed, so a change a pre-fix leader left joint can still be finished.
- **Serialized configuration changes.** Requests and automatic resume share one gate from validation
  through both commits. A waiting request validates against the preceding operation's result;
  retries do not duplicate completed entries. Client cancellation does not release an in-flight
  transition, and resume rechecks committed and pending configuration state after waiting.
- **Quorum reads.** `read=quorum` is answered after the node commits an entry of its own term,
  samples its commit index, confirms with a majority that it still leads, and applies that index
  locally. Every refusal is a retryable `503`, because none of them means the read was wrong.
- **Leader leases make that confirmation free.** The leader asks for silence on the probe it already
  sends every replica, and a voter answers with how long it will go on refusing votes; the vote
  handler keeps that, so a majority of live grants rules out an election exactly as a confirmation
  round would. The leader dates the window from before it sent the probe, so a slow link shortens
  the lease rather than overrunning it, and nothing converts one node's clock to another's. Grants
  are capped by the voter's own window and by the leader's, dropped on any leadership transition,
  retired when the wall clock shows the monotonic one stalled, and owed from boot.
- **Leadership transfer**, so office moves while the leader is alive rather than after it stops
  answering. The leader holds its writes still, brings the target up to its own tail, and tells it
  to stand for election immediately — skipping the pre-vote, and past the refusal window every voter
  of a healthy cluster is inside, which only the leader those voters are following can ask for. It
  gives up its own read lease for the length of the handover. A transfer that fails leaves this node
  leading; nothing is appended either way.
- **A leader can remove itself from the voting set.** The change hands office to a voter the change
  keeps and answers with that node's address, which is the same answer a change sent to a follower
  has always got. No operator-driven failover in the middle.

**Guarantee.** A write acknowledged with `w=majority` is held by a majority of voters and will
survive the loss of any minority, including the leader. A read answered with `read=quorum` reflects
every write committed before it. **Scope.** Leadership moves on request, through
`POST /cluster/transfer-leadership`.

---

## 6. Replication and repair

- **Leader-driven, not fire-and-forget.** The leader tracks, per replica and collection, both what
  the replica is known to hold (quorum evidence) and where to resume sending (a cursor). Cursors
  survive a leader restart as a hint that may only lower where sending resumes, never raise it.
- **Progress is a standing job.** A write ships its frame immediately, and independently a driver
  ticks a few times a second, compares every replica against every collection tail — whether or not
  it holds a cursor for that pair — and streams whatever is missing. An idle cluster still heals, and
  a failed send is retried instead of forgotten.
- **Adaptive polling.** A replica that makes no progress for two consecutive rounds is polled on a
  widening interval, so a node that is simply down does not cost a scan and a connect every tick. A
  write still triggers repair immediately regardless.
- **Backlog before newest.** If a replica is behind the predecessor of the frame being sent, the
  leader streams from the cursor instead of sending one frame that could only be rejected.
- **Pipelined batches, bounded two ways.** Repair ships up to 64 frames per request — one round trip
  and one remote fsync per batch instead of per frame — and releases its concurrency slot between
  chunks so a long backfill does not hold one for its whole duration. The receiver appends the run
  and fsyncs once. A batch is bounded by size as well as count, and a frame over that budget travels
  alone.
- **Precise refusals.** A replica classifies each frame as applied, duplicate, gap or divergent, and
  answers with its own tail so the leader knows exactly where to resume. A retransmit is recognised
  first — a committed LSN is agreed, a staged one matches at the same term — so an ordinary resend
  is never read as a conflicting history.
- **Commit hints require log matching.** Replication and heartbeat hints publish only through a
  durable prefix matched in the current leader's term. A duplicate confirms its own position;
  rejected frames cannot expose a conflicting tail through reads or change streams.
- **A conflicting tail is truncated, not transferred.** A frame naming a predecessor below the
  replica's tail says the leader is replacing entries the replica holds uncommitted, so the replica
  drops them and applies the leader's entry. Later WAL files are emptied before the cut file shrinks,
  so a crash mid-truncation cannot leave frames above the cut, and the durable LSN is lowered to the
  cut so durability this node no longer has cannot count toward a quorum. Truncation stops at the
  applied watermark, and the refusal reports that watermark so the leader resumes from the highest
  point they are known to agree on.
- **Chain verification.** Repair ships frames that form an unbroken predecessor chain from the
  replica's reported tail, and refuses to work from a frame set with a hole in it.
- **Snapshot fallback.** When the chain cannot reach the tail — because the replica is below the
  floor compaction retained, or further behind than the retention budget covers — or when a
  divergence is below what the replica has already published, the leader triggers a full snapshot
  resync instead of guessing. A replica that refuses a batch outright escalates the same way. The
  exception is the replica reporting a snapshot already installing, which is left to finish.
- **One repairer per replica and collection.** A second would re-read the same range. A caller with
  no acknowledgement to report — a gap, a divergence, the periodic driver — leaves the running
  repair to it; a caller waiting on a write concern queues, re-checks what the replica matched, and
  does the work itself if the running repair had already read its target before this frame existed.
- **Acknowledgements mean what they say.** Only "this replica holds this frame" counts toward a
  write concern; running out of repair passes short of it does not.

### Write concern

| `?w=` | Waits for |
|---|---|
| `1` (default) | local durability |
| `majority` | a majority of the configuration in force — of *each* half while a membership change is in flight |
| `all` | every voting replica plus the leader |
| *N* | N acknowledgements, clamped to the group size |

`?wtimeout=` bounds the wait (5 s by default). Replication happens *before* the client is
acknowledged, and the local fsync runs concurrently with the replica round trips, so a `w=majority`
write costs the slower of the two rather than their sum. When the concern is not met the write is
still durable and the response says so explicitly with `acks` and `required`.

Every concern is fenced against the leadership term the append was made under, `w=1` included. A
write that lost the term at any point up to its acknowledgement is refused with `503` instead of
counted, because the local node alone satisfies `w=1` and would otherwise report success for an
entry the next leader truncates.

---

## 7. Snapshot transfer and recovery

- **Bounded memory in both directions.** A collection is streamed as 64 KiB chunks through a
  two-chunk channel, so a slow receiver backpressures the producer instead of buffering a whole
  collection into memory.
- **Self-validating format.** A magic header, then one entry per file with a length-and-CRC footer
  independent of HTTP framing. The receiver enforces the magic, safe single-component filenames
  restricted to the expected shapes, no duplicates, chunk and file-count ceilings, per-entry length
  and CRC, the presence of every required file, and no trailing bytes — then checks the staged
  applied watermark from both sides: it may not exceed the staged log tail, and it may not fall
  below what this node has already published, so a source deposed mid-transfer cannot retract
  entries a quorum committed.
- **Cheap on an idle leader.** The sender rotates to a fresh WAL to freeze the set it streams, or
  skips the rotation entirely when the active WAL is already empty, so repeated requests cost no new
  files.
- **Atomic installation with rollback.** The staged directory is swapped in under a lock after a
  durable `.install` marker is written into it; every failure path restores and reopens the previous
  directory, and incoming replication for that collection is serialised against installation so an
  acknowledged frame is never lost to a swap. A crash between the two directory renames is repaired
  at boot from the marker, the staged snapshot, or the `.old` backup. An install and a compaction
  cannot finish onto each other's files, and a handle whose collection has been replaced refuses
  appends and refuses to serve a snapshot rather than acting as if the collection were empty.
- **Automatic use.** A replica syncs at boot when configured with a primary, on discovering a new
  leader, and whenever a leader tells it that incremental repair cannot converge.
- **A node serves only what it has.** Asked for a collection it holds no directory for, a leader
  answers `404` rather than creating one and streaming it back empty. The replica reports that apart
  from a failed transfer and leaves its local copy alone. A dropped collection is still served: the
  tombstone holds the log the drop lives in.

---

## 8. Sharding and topology

- **One versioned cluster view, everywhere.** Members, ownership and any in-flight migration plan
  live in a single value that every node — routers included — carries, persists and serves.
- **Deterministic convergence.** Views are totally ordered by version and author, so every node
  converges on the same one. A config-derived seed loses to every real view, and a malformed view is
  rejected before it is compared, so one bad push cannot outrank the correction that follows.
- **Durable before visible.** A view is persisted before it is served, so a node never serves a
  topology it would forget on restart.
- **Four propagation paths.** A direct push to every node a change touches, the view's ordering
  identity advertised in every heartbeat that makes a follower pull, a router probe that pulls from
  whichever peer is furthest ahead, and the three-second catalogue round every node runs against
  every peer it can name. Every puller applies the same total order it would apply to the view
  itself, so two nodes that published concurrently at one version converge on the tiebreak winner.
  The catalogue round is the one that reaches between two shard leaders.
- **Config is a seed, not an authority.** After the first boot the durable view decides, and a later
  config edit is logged as ignored rather than half-applied.
- **Two ownership models.** Explicit hash ranges, validated for exact coverage with no overlap; and
  a consistent-hash ring that stores membership plus a vnode count and *derives* tokens, so two
  nodes cannot disagree about a token list they never exchanged. The ring wins wherever both are
  present, and ranges are retained underneath so a rollback has something to return to.
- **Group-level ownership.** A ring entry belongs to a whole shard group, so after a failover the
  replica now answering owns exactly what its entry owns.
- **Bounded ring layout.** At most 1024 shards, at most 4096 vnodes, and at most 2^20
  `shards x vnodes` tokens — the product, because that is what deriving and diffing a ring costs.
  Refused with `422` before anything is built, on the dry run as much as the real publish. Deriving
  and costing a ring runs off the request task.
- **Shard-side ownership enforcement.** Because views converge rather than switching in lockstep, a
  shard refuses keys it does not own and names the real owner. A stale router is told where to go
  instead of writing a second copy, and it retries there transparently.
- **Exact change costing.** The fraction of keyspace a ring change would move, and the per-pair
  transfers, are computed over arcs rather than sampled — an exact answer for the decision an
  operator is making. `?dry_run=true` reports it without applying anything.
- **Unsafe changes are refused, not warned about.** Publishing a ring moves ownership without moving
  data, so it is blocked unless the change provably moves nothing or every current owner confirms it
  is empty. An unreachable owner or an unreadable data directory counts as "holds data". The refusal
  names the safe path instead.

### Runtime membership

- **Join and leave without a restart**, addressed to a shard leader, which redirects to the primary
  if it is not the leader itself.
- **Admitted nodes are learners.** They replicate immediately and can serve reads, and are counted
  in no quorum. Asking to join as voting is refused with an explanation rather than quietly
  downgraded, and a node already voting cannot be re-added as a learner, because that would shrink
  the quorum it is already in.
- **Promotion and demotion belong to the log.** The voting set moves through joint consensus, and
  the view records the outcome afterwards. So a voter is added by admitting it, letting it catch up
  and then publishing the set that includes it; and removed by publishing the set without it before
  dropping it from the view.
- **Learners name the primary they follow**, so one cluster-wide member list serves several shard
  groups: a leader ships frames only to learners naming it.
- **Immediate catch-up.** A newly admitted node gets a send cursor at zero, so the driver picks it up
  on its next tick instead of waiting for the next write. A node that learns of its own admission by
  propagation behaves exactly as if it had been told directly.

---

## 9. Online migration and rebalancing

Copy first, flip once, then clean up:

1. **Copy.** Every source leader works out which of its keys the target ring gives away, groups them
   by destination, and pushes batches through the destination's normal write path — so moved keys
   replicate inside the destination group. Committed values are copied: an uncommitted write may
   still be revoked, and handing it over would make it durable elsewhere. Batch size and inter-batch
   pause are configurable, so a handover can be made to yield to live traffic; the configured size
   bounds the document count, and a batch is split again by size so a handover of large documents is
   not a request the destination refuses.
2. **Finalize.** Once every source is done, a short barrier phase: destinations reset keys received
   earlier from that source, the remainder is pushed with no delay, and the keys actually about to
   move are briefly refused with `Retry-After`. Everything else keeps serving.
3. **Flip.** The target ring is published; ownership moves as each node adopts it.
4. **Clean up.** The final view is handed to every node that owned the keyspace before or owns it
   now, and each deletes what it handed over — as tombstones through the write path, and only after
   re-checking each key against the live view. Cleanup deliberately reaches shards dropped from the
   ring entirely, since leftovers there would shadow current values if that node ever returned.

- **Crash-tolerant coordination.** The plan lives in the cluster view, not in the coordinator's
  memory, so if the coordinator dies the sources keep pushing and coordination resumes idempotently.
  Resumption hangs off every event that can hand a node the plan — a promotion, a view adoption, a
  boot — and a source whose push stopped when leadership moved is replanned by whoever holds office
  next, including the same node winning it back.
- **The handover record is replicated.** What a group moved is a log entry in its `_config` log, not
  a node-local file, so a group that elects someone else between the ring flip and cleanup still
  knows to drop its copies. The record names the plan and the ring it moved keys *for*, and cleanup
  asks that ring which of the keys it holds are no longer its own.
- **Cleanup goes to the group's current leader**, resolved rather than read off the ring, and a node
  that does not lead refuses it. Only a leader can commit the tombstones.
- **Abortable.** A handover can be abandoned any time before the flip: ownership never moved, so the
  copies already pushed are unreferenced.
- **Optional automatic rebalancing.** Off by default. When enabled, one designated node derives the
  ring that live membership implies, debounces churn over a stabilization window, and runs exactly
  the same copy-then-flip path — so automatic and manual rebalancing share one safety boundary. A
  status endpoint reports current versus desired placement and what reconciling would move.

---

## 10. Routing and client-facing availability

- **Stateless routers.** A router holds no data: it hashes the key, finds the owning group in its
  view, and forwards.
- **Write failover.** A transport failure or a `5xx` triggers a single-flight failover per shard: a
  re-check of the effective primary in case a probe already moved it, then each replica in turn. The
  replica that answers authoritatively is cached as that group's primary for 30 seconds — unless its
  answer was a redirect, which names the key's owner and says nothing about who leads the group. A
  shard's `4xx` is treated as an answer, not a failure, so a `404` on a patch is passed through
  rather than retried around the group.
- **Background primary tracking.** Every few seconds the router probes each group's primary,
  replicas and current override. The highest-term node answering as primary wins, so a partitioned
  old primary cannot reclaim traffic — reads follow leadership changes without waiting for a write
  to fail first.
- **Stale-view redirects.** Reads, single writes and bulk writes all follow a shard's `409` to the
  named owner and retry there once, because reading a moved key from its old owner would return a
  wrong answer rather than a visible failure. Every node a request is tried at is followed this way,
  the failover replicas included, so a write that meets a dead primary and a moved key at the same
  time still lands. A bulk slice is retried whole: the shard names the owner of the first key it
  disowns, and the target re-checks every key, so a slice spanning owners is refused rather than
  half applied.
- **Load-aware replica reads.** With `read=replica`, candidates are ranked by inflight requests times
  an EWMA of latency, with unmeasured nodes tried after measured ones and round-robin breaking ties.
  Samples come from heartbeat probes, expire after ten seconds, and are adjusted by the reads this
  router itself has in flight.
- **Guarantees are enforced where they are answered.** `read=primary` and `read=quorum` are
  forwarded rather than resolved from the router's own view, which can be stale, and replicas stay
  in the candidate list so a promoted one is still found. Every candidate refusing is a `503` naming
  no reachable primary, which is a different thing from the shard being down.
- **Fan-out with per-node reporting.** Listing collections unions every group and refuses with `502`
  when one of them cannot answer, since a partial union is indistinguishable from a smaller cluster;
  drop, compact and snapshot fan out and report per node, answering `207` when any node failed. A
  drop answers `200` only when every group committed and `202` when every group answered and one
  staged, so the aggregate does not upgrade a short quorum into success; the index fan-out grades
  the same three outcomes the same way, and a create answers `201` where every group that holds the
  collection created the definition. Compaction goes to primaries only, since replicas refuse it by
  design.

**Scope.** `read=replica` is stale-tolerant: a replica may not yet have applied the newest committed
write. A router reports a read refusal as "no reachable primary" — the status class and the remedy
are right, and a direct request to the shard gives the precise message.

---

## 11. Flow control and backpressure

- **Bounded uncommitted backlog.** Staged frames only drain on commit, so a leader that has lost its
  quorum would otherwise stage forever. Writes are refused with `503`, `Retry-After` and a body
  naming the collection, the backlog and the bound — checked before the append, which is the last
  point where growth can still be refused. Rejections are counted in metrics, and the bound can be
  disabled.
- **Admission reserves what it admits.** A write holds capacity for its own frame count from the
  check until the append stages it, so a bulk request is admitted as a whole or not at all and
  concurrent writes cannot each pass the same pre-append count. A batch wider than the whole bound is
  refused with `413` rather than retriable backpressure — no amount of draining admits it.
- **Bounded outbound concurrency.** A node-wide semaphore caps concurrent replication requests across
  every write and repair, rather than bounding one write's fan-out and nothing else. Available
  permits are exported.
- **Backpressure end to end.** Snapshot streaming, the migration batch pacing and the group-commit
  queue all push back rather than buffer without limit.

---

## 12. Security

- **API keys on the public API**, as `x-api-key` or `Authorization: Bearer`. Several keys can be
  configured at once, so rotation is append-then-remove.
- **Rotation without a restart.** `api_keys` and `admin_keys` are re-read from the config file while
  the node runs, so removing a compromised key takes effect within about five seconds. A file that
  will not parse or that names an unusable credential is refused and the set in force is kept.
  `internal_secret` and `upstream_api_key` are wired into the node's outbound clients at boot, so
  changing either takes a restart.
- **An admin tier over topology, destruction and schema.** `admin_keys` guards every `/cluster/*`
  route, `DELETE /collections/:name`, index definitions and every webhook route; leaving it empty
  falls back to `api_keys`, so an existing deployment is unchanged. An admin key opens the public API
  as well, so one credential covers reading a collection and dropping it.
- **Shared secret on internal routes**, so node-to-node endpoints are not open to clients.
- **No cross-unlocking.** An internal secret does not open the public API and an API key does not
  open internal routes.
- **Authorization that outlives no subject.** The three things that answer past the request that
  created them are judged again rather than once: an SSE change stream and a WebSocket one re-check
  their credential every two seconds and end in-band when it stops being accepted, and a webhook
  subscription re-checks before every open and every delivery attempt, disabling itself when the key
  that registered it is gone. What a registration keeps on disk is a SHA-256 digest of that key, not
  the key.
- **Constant-time comparison**, so a wrong value does not leak a prefix.
- **Outbound credentials.** A node can be given the key it should present when calling another
  node's public API.
- **Fail-loud credential validation.** A credential that could not go in an HTTP header fails the
  boot instead of silently disabling a check, and an unset credential is warned about at startup.
- **Probe-friendly.** `/health` stays open; `/metrics` does not, because it exposes topology.

**Scope.** TLS is terminated in front of the node, and the credential tiers are client, admin and
internal. At-rest encryption and audit logging live in the surrounding infrastructure. Compact and
snapshot sit on the client tier.

---

## 13. Observability and operations

- **Structured logging** in text or JSON, stamped with the node id and a subsystem target, with the
  level overridable by environment.
- **Actionable health.** `/health` returns `503` with reasons — no open database, no known primary,
  a stale primary heartbeat, a router with no shards in its view — which is what a load balancer
  needs to act on.
- **Metrics in two formats.** JSON for humans and scripts, Prometheus text for scrapers, from the
  same data.
- **Storage visibility.** Per collection: documents, WAL bytes, live and dead bytes, dead ratio,
  last and applied LSN, pending applies, inline-cache occupancy committed and staged, whether a
  compaction is running, and whether the collection is a drop tombstone.
- **Replication visibility.** On a leader: term, durable LSN, commit index overall and per
  collection, voting replicas versus learners, worst replica lag, per-replica matched LSNs, repair
  counters (gaps, divergences, resyncs), batching counters (batches, frames, widest batch, frames
  per batch) and flow-control counters. On a follower: its primary, that primary's commit index, its
  own lag, and seconds since last replication.
- **Request visibility.** Per-route counts, errors, average and p50/p95/p99 latency, and a latency
  histogram for scrapers.
- **Cluster visibility.** The view version, which ownership model is actually deciding, member and
  shard counts, any migration with local progress, and — on a router — the effective primary per
  group with a failover flag and its load estimates.
- **Restart-free administration.** Drop, compact and snapshot a collection; join and remove members;
  read and change the voting set; publish a ring; start, watch and abort a migration.
- **Windows-aware file handling.** Deletes, renames and handle draining retry and park open handles
  where the platform will not unlink an open file, so compaction and snapshot installation behave the
  same on Windows as elsewhere.
- **Tested against real nodes.** The suite runs live multi-node clusters on loopback with temporary
  data directories, covering replication, election, failover, migration and snapshot paths end to
  end, alongside unit tests of the storage and consensus internals. A cluster reaches a test
  converged rather than after a fixed sleep, and runs on a slack contact timeout unless the test asks
  otherwise, so load cannot depose the leader a test was handed.
- **Fault injection.** A test-only, directional link-fault table — compiled out of a release build —
  cuts, delays or isolates traffic between nodes, keyed on the header every node already stamps on
  internal requests. A cut hangs at the receiving end, so the sender fails the way a partition makes
  it fail rather than learning the peer is up and refusing. Asymmetric partitions are the point:
  every other signal a node has is inbound.
- **Soak scenarios.** Repeated crashes with writes in flight, cluster churn under concurrent
  `w=majority` writes, compaction interleaved with crashes, and a WAL cut off mid-record — the
  machine-crash case an in-process kill cannot reach. Each asserts against a ledger of what the
  client was actually told: a `200`/`201` is a promise, a `202` is not, and an answer that never
  arrived is evidence of neither. Fixed seeds, overridable, so a failure replays.
- **Benchmarks** are kept out of the default run and report medians over several samples, including
  quorum width over 3, 5 and 7 voters, routed writes over 1 to 8 shards, and cross-shard query with
  and without the sorted merge.

---

## 14. Configuration surface

One JSON file per node. Everything below has a default except identity and role:

identity and role · group membership (`primary_addr`, `replicas`, `peers`, `membership_mode`) ·
ownership (`shard_map` or `ring`) · data directory · election and heartbeat timing · flow control ·
maintenance thresholds · rebalancing · migration batch pacing · read cache · change-stream buffer,
retention and subscriber ceiling · webhook batching, timeout and backoff · logging · auth.

- **Validation is fatal where a mistake would be silent** — a key no field matches (the config tree
  refuses unknown keys, so a typo does not quietly take a default), an unknown role, contradictory
  ownership models, a learner declared primary, an invalid ring, an unusable credential, an
  unreadable durable view.
- **Warnings name the fix, not just the symptom** — a node listing itself as its own peer, a replica
  absent from the voting set, a shard with no replicas to fail over to, a lone voter that will elect
  itself.

Full field-by-field reference in [documentation.md](documentation.md#4-configuration).

---

## 15. Scope at a glance

The shape of what DewDB does today, in one place:

| Area | Where it stands |
|---|---|
| Writes | Atomic per document. Bulk writes are one group commit and one replication round, accepted or refused whole. |
| Indexes | Single-field, by value, non-unique, up to eight per collection. A filter with no eligible index scans a key range; a condition inside an `$or` is answered by the scan. |
| Aggregation | `count`, `sum`, `avg`, `min`, `max` over up to four grouping fields, bounded at 10 000 groups, computed fresh on each request. |
| Index reach | An index is defined per shard group; a group that gains the collection later picks it up from the cluster catalogue in the background and scans until it has. |
| Leadership | Moves on request, through `POST /cluster/transfer-leadership`. |
| Quorum reads | `read=quorum` is linearizable at the page's starting point. |
| Replica recovery | A replica within `maintenance.wal_retention_bytes` repairs from frames; one further behind takes a full collection snapshot. |
| Ownership models | The ring is established before data is loaded; `/cluster/migrate` handles every change after that. Shard-side ownership refusals apply when a ring is in force. |
| Sizes | 10 MiB per stored record, 2 MiB per public request body, 10 000 documents per query page. |
| Change history | As far back as `changefeed.buffer_events` per collection. A cluster-wide stream can miss up to a second of a collection's first changes on a shard group that held none of it when the stream opened. |
| Webhooks | Registered per shard group. Registrations, removals and acknowledged positions are majority-committed; administration needs a reachable quorum. |
| WebSocket auth | The handshake carries a credential header, so a browser reaches an authenticated feed through SSE and a same-origin proxy. |
| TLS and audit | Terminated and collected in front of the node. |
| Cross-shard work | Per-key or fan-out; joins are done in the client. |

---

## 16. Planned

Two capabilities are designed and not yet shipped. They are listed here so the scope above is read
as it stands today, not as a permanent boundary.

- **TLS in the process.** The public and internal listeners terminate TLS from a certificate and key
  named in the config, and a node dials its peers the same way, so replication does not carry its
  shared secret in the clear and a deployment needs no proxy in front of it to be reachable safely.
  Plaintext stays available, because a single-node trial should not need a certificate.

- **Conditional writes.** `If-Match` on `PUT`, `PATCH` and `DELETE`, carrying the version a read
  returned and answered `412` when the document moved underneath it. Single-document writes are
  already atomic, but atomicity settles one write against another and never a write against the read
  it was based on; the precondition is what lets two clients that read, decide and write notice that
  one of them lost.
