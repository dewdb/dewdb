# Getting Started with DewDB

**DewDB is a distributed JSON document database in a single binary.**

Replication, sharding and failover run inside the process. One binary, one JSON config file per
node, local files for storage, and HTTP as the protocol — no external coordinator, no separate
metadata service, no driver to install. Written in Rust, Apache 2.0.

This guide takes you from a single node to a replicated, sharded cluster. Every command below runs
as written.

- For the full capability catalogue, see [features.md](features.md).
- For the reference manual — every config field, endpoint and on-disk format — see
  [documentation.md](documentation.md).

---

## Contents

1. [Install and build](#1-install-and-build)
2. [Your first node](#2-your-first-node)
3. [Documents: create, read, update, delete](#3-documents-create-read-update-delete)
4. [Bulk writes](#4-bulk-writes)
5. [Querying](#5-querying)
6. [Aggregating](#6-aggregating)
7. [Secondary indexes](#7-secondary-indexes)
8. [Watching for changes](#8-watching-for-changes)
9. [Getting changes pushed to you](#9-getting-changes-pushed-to-you)
10. [Write concerns and durability](#10-write-concerns-and-durability)
11. [Collection administration](#11-collection-administration)
12. [Add replicas and watch a failover](#12-add-replicas-and-watch-a-failover)
13. [Add a node at runtime](#13-add-a-node-at-runtime)
14. [Shard with a router](#14-shard-with-a-router)
15. [Add a shard without downtime](#15-add-a-shard-without-downtime)
16. [Turn on authentication](#16-turn-on-authentication)
17. [Monitoring](#17-monitoring)
18. [Backups and restarts](#18-backups-and-restarts)
19. [Handling responses in a client](#19-handling-responses-in-a-client)
20. [Client snippets](#20-client-snippets)
21. [Troubleshooting](#21-troubleshooting)

---

## 1. Install and build

You need a Rust toolchain (edition 2024).

```bash
cargo build --release
```

The binary is `./target/release/dewdb`. Run the tests in your environment:

```bash
cargo test
```

Benchmarks are excluded from the default run:

```bash
cargo test --release -- --ignored --nocapture bench
```

So are the long-running durability scenarios — repeated crashes, cluster churn under load,
compaction interleaved with crashes, and a WAL cut off mid-record. These take minutes:

```bash
cargo test --release -- --ignored --nocapture soak
```

Set `DEWDB_SOAK_SEED=0x…` to run a different schedule than the fixed seed each scenario ships with.

---

## 2. Your first node

Create `dew.json`:

```json
{
  "node_id": "n1",
  "role": "shard",
  "shard_role": "primary",
  "listen_addr": "127.0.0.1:8081",
  "data_dir": "./data"
}
```

Start it:

```bash
./target/release/dewdb --config dew.json
```

That is the whole setup. This node leads itself, commits on its own fsync, and is ready
immediately. Confirm:

```bash
curl -s localhost:8081/health
```

```json
{"status":"ok","node_id":"n1","role":"shard","leader":true,"term":0,
 "cluster_version":1,"uptime_secs":3,"collections":0,"reasons":[]}
```

`Ctrl-C` shuts it down cleanly: pending writes are fsynced and the durable position is recorded
before the process exits.

> Running several nodes on one machine? Give each its own `data_dir` and port.

---

## 3. Documents: create, read, update, delete

Collections are created on first use. No schema, no create step.

### Create with a server-generated key

```bash
curl -s -X POST localhost:8081/collections/users/docs \
  -H 'content-type: application/json' \
  -d '{"value":{"name":"ada","age":36,"profile":{"city":"london"}}}'
```

```json
{"id":"7c1f…","status":"created"}
```

Note the shape: the document goes inside `value`.

### Create or replace a known key

```bash
curl -s -X PUT localhost:8081/collections/users/docs/u1 \
  -H 'content-type: application/json' \
  -d '{"value":{"name":"ada","age":36,"profile":{"city":"london"}}}'
```

`201` the first time, `200` when it replaced an existing document.

### Read

```bash
curl -s localhost:8081/collections/users/docs/u1
```

```json
{"name":"ada","age":36,"profile":{"city":"london"}}
```

The stored document comes back as-is — no envelope, no injected fields. What you `PUT` is what you
`GET`.

A missing document is `404 {"error":"not found"}`, and a name no collection answers to is
`404 {"error":"collection 'x' does not exist"}`. Reading never creates a collection, so a typo is
an error rather than a new empty one.

### Merge patch

`PATCH` applies JSON Merge Patch: nested objects merge, `null` removes a field, anything else
replaces.

```bash
curl -s -X PATCH localhost:8081/collections/users/docs/u1 \
  -H 'content-type: application/json' \
  -d '{"value":{"age":37,"profile":{"country":"uk"},"nickname":null}}'
```

`u1` is now `{"name":"ada","age":37,"profile":{"city":"london","country":"uk"}}`. Patching a
document that does not exist is `404` — use `PUT` to create.

### Delete

```bash
curl -s -X DELETE localhost:8081/collections/users/docs/u1
```

```json
{"status":"deleted","existed":true}
```

### List a collection

```bash
curl -s localhost:8081/collections/users/docs
```

You get a page — `{"items":[…],"next_cursor":…}` — with a default limit of 100 and a maximum of
10 000. Follow `next_cursor` until it comes back `null`. On a sharded cluster, use `/query` instead;
a router answers this route with `501` and says so.

---

## 4. Bulk writes

Send an array; `id` is optional per document. The whole batch lands as one group commit and one
replication round, which is far cheaper than N single writes — about 0.55 ms per document at
`w=majority` on three nodes, against 25 ms for the same documents written singly.

```bash
curl -s -X POST localhost:8081/collections/users/docs/bulk \
  -H 'content-type: application/json' \
  -d '[
        {"id":"u1","value":{"name":"ada","age":36,"profile":{"city":"london"}}},
        {"id":"u2","value":{"name":"lin","age":29,"profile":{"city":"berlin"}}},
        {"value":{"name":"kai","age":41,"profile":{"city":"lisbon"}}}
      ]'
```

```json
{"results":[{"id":"u1","status":"created"},
            {"id":"u2","status":"created"},
            {"id":"e3b0…","status":"created"}]}
```

Results come back in request order, including on a router that split the batch across shard groups.

A few things the batch does for you:

- **Ownership is checked first.** On a sharded cluster the batch is accepted whole or refused
  whole, never half applied. A shard that refuses names the owner, and the router retries that
  slice there once, so a batch sent through a ring that has not caught up with a handover still
  lands.
- **One write concern for the run.** The log is a chain, so a replica that acknowledges the last
  document holds all of them. Every item in a group's results carries the same `acks` and
  `required`.
- **Admitted whole.** The batch reserves one frame per document against
  `flow_control.max_uncommitted_frames`. A group whose replication is stalled answers `503` rather
  than staging past the bound. A batch with more documents than the whole bound gets `413` — that
  one will not clear on retry, so split it.

A refusal keeps its status through the router. When a shard's `400`, `404`, `409`, `413` or `422`
answered for the whole batch, the router replies with the shard's own status and body rather than a
`207` — nothing was written, so there is no partial success to report. A `207` therefore means what
it says: some documents landed, and the ones that did not name their own `code`.

---

## 5. Querying

```
GET /collections/:name/query
```

Let curl encode the JSON filter for you:

```bash
curl -s --get localhost:8081/collections/users/query \
  --data-urlencode 'filter={"age":{"$gte":30}}' \
  --data-urlencode 'sort=age:desc' \
  --data-urlencode 'fields=name,profile.city' \
  --data-urlencode 'limit=10'
```

```json
{"items":[{"name":"kai","profile":{"city":"lisbon"}},
          {"name":"ada","profile":{"city":"london"}}],
 "next_cursor":null}
```

### Filters

| Want | Filter |
|---|---|
| exact match | `{"name":"ada"}` |
| nested field | `{"profile.city":"london"}` |
| numeric range | `{"age":{"$gte":30,"$lt":40}}` |
| string range | `{"name":{"$gte":"a","$lt":"n"}}` |
| not equal | `{"status":{"$ne":"archived"}}` |
| one of | `{"profile.city":{"$in":["london","berlin"]}}` |
| none of | `{"profile.city":{"$nin":["london"]}}` |
| field is absent | `{"deleted_at":{"$exists":false}}` |
| value is a number | `{"age":{"$type":"number"}}` |
| starts with | `{"name":{"$prefix":"ad"}}` |
| ends with, contains | `{"email":{"$suffix":"@x.com"}}`, `{"bio":{"$contains":"rust"}}` |
| array holds all of | `{"tags":{"$all":["rust","db"]}}` |
| array length | `{"tags":{"$size":3}}` |
| an element matches | `{"scores":{"$elemMatch":{"$gte":90}}}` |
| an object element matches | `{"items":{"$elemMatch":{"sku":"x","qty":{"$gte":5}}}}` |
| two conditions (AND) | `{"age":{"$gte":30},"profile.city":"london"}` |
| either (OR) | `{"$or":[{"age":{"$lt":18}},{"age":{"$gte":65}}]}` |
| neither (NOR) | `{"$nor":[{"status":"archived"},{"status":"draft"}]}` |
| negation | `{"age":{"$not":{"$gte":30}}}` |

Paths are dotted. A document that does not have the path does not match, so a condition on a field
is also a check that the field is there. `$exists:false` and `$not` are the two ways to ask about
absence.

`$gt`/`$gte`/`$lt`/`$lte` take a number *or* a string, and compare inside that type only:
`{"age":{"$gt":1}}` will not return a document whose `age` is `"zebra"`, and the reverse holds too.
Numbers compare through `f64`: integers below 2^53 are exact, and two that differ only above that
compare equal, so range filters cannot separate them. Store such a value as a zero-padded string if
you need to range over it.
`$ne`, `$in`, `$nin` and `$all` compare whole JSON values.

Anything the engine cannot evaluate — an operator it does not know, `$in` without an array, range
bounds of two different types — is a `400`. It is never quietly dropped, so a query answered `200`
is the query you asked for.

### Sort and projection

```bash
curl -s --get localhost:8081/collections/users/query \
  --data-urlencode 'sort=profile.city:asc,age:desc' \
  --data-urlencode 'fields=name,age'
```

Sort takes up to eight dotted paths, each with `:asc` (default) or `:desc`. A later key decides
where the earlier ones tie, and it works across shards — the router merges the shards' ordered
pages. Numeric sort keys compare integers exactly rather than rounding them through a float.
Projection rebuilds nested structure and skips paths a document does not have.

An unknown direction (`:descc`), an empty key and a repeated one are all `400`. Change the sort keys
and you start the scan again: a cursor is a position under the keys it was issued for, and handing
it to a different sort is a `400`.

### Paging

Every query pages with an opaque cursor:

```bash
# first page
curl -s 'localhost:8081/collections/users/query?limit=2'
# → {"items":[…],"next_cursor":"u2"}

# next page: pass it straight back
curl -s --get localhost:8081/collections/users/query \
  --data-urlencode 'limit=2' --data-urlencode 'cursor=u2'
```

Keep passing `next_cursor` until it comes back `null`, and treat it as opaque. Do not parse one or
construct one yourself, and do not carry one between differently shaped queries: a sorted cursor
handed to an unsorted query (or the reverse) is a `400`, and an unsorted cursor issued before a
ring change is a `409` telling you to restart the scan.

An unsorted cursor is a position in the keyspace — a single shard returns the last key it decided
about, and a router returns an encoded position per shard so each one resumes exactly where it
stopped. An unsorted router page can come back `200` with empty `items` and a non-null
`next_cursor` when `limit` leaves shards unqueried; follow that cursor to reach the rest.

A sorted query pages the same way, but its cursor is a position in the *sort order* — one position
for the whole cluster, so it keeps working when a shard joins mid-scan:

```bash
CURSOR=$(curl -s --get localhost:8080/collections/users/query \
  --data-urlencode 'sort=age:desc' --data-urlencode 'limit=100' \
  | python -c 'import json,sys; print(json.load(sys.stdin)["next_cursor"] or "")')

curl -s --get localhost:8080/collections/users/query \
  --data-urlencode 'sort=age:desc' --data-urlencode 'limit=100' \
  --data-urlencode "cursor=$CURSOR"
```

`limit` defaults to 100 and is capped at 10 000; ask for more and you get a `400` telling you to
page. Add `keys=true` if you want each row's key alongside it.

`max_docs` bounds the other cost: documents *read* per shard per request, 100 000 by default and
1 000 000 at most. A filter is what makes the two differ, since a page that matches nothing still
reads what it had to look at. Spend the budget and an unsorted page comes back short — sometimes
empty — with a cursor; keep following it and you still see every match. A sorted page reads a whole
range before it can order any of it, so there the same exhaustion is a `400`.

### Key ranges

Keys are stored in sorted order, so a key prefix scheme gives you cheap range scans:

```bash
curl -s 'localhost:8081/collections/events/query?start=2026-08-01&end=2026-08-31&limit=500'
```

`start` and `end` are inclusive and ascending. `start` above `end` is a `400`, not an empty page, so
a reversed pair tells you rather than looking like no data. The same holds on `/aggregate`.

### Choosing what a read guarantees

| `?read=` | Answered by | Guarantee |
|---|---|---|
| *omitted* | the primary, then replicas | whatever answers first |
| `primary` | a node that believes it leads | fresher than a replica, though a replaced leader still believes it leads |
| `quorum` | a leader that has just proved it still leads | linearizable: reflects every write committed before the read |
| `replica` | a replica, ranked by observed load | stale-tolerant |

Spread read load across the group:

```bash
curl -s 'localhost:8080/collections/users/query?read=replica&limit=50'
curl -s 'localhost:8080/collections/users/docs/u1?read=replica'
```

The router ranks replicas by observed load and falls back to the primary. These reads are
stale-tolerant: a replica may not have applied the newest committed write yet.

When you need a read you can reason about — read-after-write across a failover, or a value you are
about to act on — ask for a quorum read:

```bash
curl -s 'localhost:8080/collections/users/docs/u1?read=quorum'
```

The answering leader commits an entry of its own term, samples its commit index, confirms with a
majority of voters that it still leads, and waits for that index to be visible locally. Usually the
confirmation is free: followers hand the leader a short promise not to vote with every poll, and a
majority of live promises rules out an election just as a round of heartbeats would.

Any of those steps failing is a `503` with `Retry-After`, never a wrong answer — leadership too new,
no confirmation from a majority, or an index not yet applied all mean "not now", so retry. The
guarantee covers where a page *starts*, not the whole scan.

An unknown value (`read=primaryy`) is a `400` rather than a silent fallback.

---

## 6. Aggregating

```
GET /collections/:name/aggregate
```

When you want totals rather than documents, ask the shards for them instead of pulling the rows
back and folding them in your client:

```bash
curl -s --get localhost:8080/collections/orders/aggregate \
  --data-urlencode 'filter={"status":"paid"}' \
  --data-urlencode 'group=region' \
  --data-urlencode 'metrics=count,sum:amount,avg:amount'
```

```json
{"groups":[
   {"key":{"region":"eu"},"count":2,
    "metrics":{"count":{"count":2},
               "sum:amount":{"count":2,"sum":75.0},
               "avg:amount":{"count":2,"sum":75.0,"avg":37.5}}},
   {"key":{"region":"us"},"count":1,
    "metrics":{"count":{"count":1},
               "sum:amount":{"count":1,"sum":40.0},
               "avg:amount":{"count":1,"sum":40.0,"avg":40.0}}}],
 "matched":3}
```

`filter`, `start`, `end` and `read` mean exactly what they mean on `/query`, and an index behind the
filter narrows what the aggregation reads the same way. There is no `limit` and no `cursor`: an
aggregate is not resumable, so you get the whole answer or an error.

`scanned` is how many documents were read to produce that answer, which `matched` is not — a filter
that selects three rows out of a thousand still reads the thousand. It is there so the cost of an
aggregation is something you can see rather than guess.

### Metrics

| Entry | Gives you |
|---|---|
| `count` | how many documents are in the group |
| `sum:field` | the total of that field, over the documents where it is a number |
| `avg:field` | the mean of that field, likewise |
| `min:field`, `max:field` | the smallest and largest value, of any JSON type |

Each metric comes back as an object rather than a bare number, because it carries the count behind
it. That is what makes the cross-shard answer exact: the router adds up each group's `sum` and
`count` and divides once, instead of averaging the shards' averages — which would weight a shard
holding one document the same as one holding a thousand. Read `avg` and ignore the rest, or use the
totals to roll several answers up yourself.

`sum` and `avg` skip a document whose value there is not a number, and every metric skips one that
does not have the field at all. That is why each metric reports its own `count` alongside the
group's: `{"count":40}` on the group and `{"count":31,…}` on `avg:amount` says nine documents had no
usable amount.

### Groups

`group` is up to four dotted paths. The key is an object with one member per path, and a path a
document does not have is left out — so those documents group together under `{}` rather than
disappearing. Leave `group` out and you get one group with no key at all, over everything the filter
matched.

```bash
# one number for the whole collection
curl -s 'localhost:8080/collections/orders/aggregate?metrics=sum:amount'
```

One aggregation may produce up to 10 000 groups. Past that you get a `400` telling you to narrow it
— a grouping truncated on one shard and merged with another would be wrong, not just short.

### How much it is allowed to read

Each shard reads at most 100 000 documents for one request by default. Run out and you get a `400`:

```bash
curl -s 'localhost:8080/collections/orders/aggregate?metrics=count&max_docs=1000'
# {"error":"aggregation read its budget of 1000 documents on one shard without finishing; ..."}
```

Three ways forward:

- **Read less.** `filter` on an indexed field, or `start`/`end`, narrows what the walk touches
  rather than only what it counts.
- **Raise the budget.** `max_docs` goes up to 1 000 000. It is per shard, not per cluster: it bounds
  one node's walk, and a five-shard cluster does five of them at once.
- **Accept what it read.** `partial=true` answers `200` with `"partial": true` and `scanned` saying
  how far it got. Those totals cover the documents read, not the range, and one shard stopping short
  makes the whole merged answer partial — check the flag before you use the numbers.

A node runs up to four scans at a time -- an aggregation, or a `/query` page that filters or sorts.
A fifth waits a couple of seconds for a slot and is then `429`, which means retry: a scan holds a
thread for as long as its budget lasts, and reads and writes share that pool. A plain listing with
no filter reads exactly `limit` and never waits for a slot.

---

## 7. Secondary indexes

A filter with no index behind it walks the collection's key range and reads every document. An index
on the field turns that into reading only the documents that might match.

```bash
# define one — leader only; a replicated log entry, so it takes ?w= like a write
curl -s -X POST localhost:8081/collections/users/indexes \
  -H 'content-type: application/json' \
  -d '{"name":"by_age","field":"age"}'

# what this collection has, and whether it is answering queries yet
curl -s localhost:8081/collections/users/indexes

# drop one
curl -s -X DELETE localhost:8081/collections/users/indexes/by_age
```

```json
{"collection":"users",
 "indexes":[{"name":"by_age","field":"age","state":"ready","documents":1204,"values":58}]}
```

Nothing else changes. You do not name an index in a query and there is no hint syntax: the same
`filter=` you were already sending starts using it.

```bash
# reads 58 documents instead of 1204, and returns exactly what it returned before
curl -s --get localhost:8081/collections/users/query \
  --data-urlencode 'filter={"age":30}'
```

### What it accelerates

| Condition on an indexed field | Uses the index |
|---|---|
| `{"age":30}` — any exact value, including nested objects | yes |
| `{"age":{"$in":[30,40]}}` | yes |
| `{"age":{"$gte":30,"$lt":40}}` — numbers or strings | yes |
| `{"name":{"$prefix":"ad"}}` | yes |
| `{"age":{"$type":"number"}}` — one type named | yes |
| `{"deleted_at":{"$exists":true}}` | yes, when few documents have the field |
| `{"age":{"$ne":30}}`, `$nin`, `$not` | no — "everything except" is the scan it would replace |
| `{"bio":{"$contains":"x"}}`, `$suffix` | no — neither names a span of the order |
| `$all`, `$size`, `$elemMatch` | no — an array is indexed as one value |
| a condition inside an `$or` or `$nor` | no — see below |

A filter naming several indexed fields uses the narrowest one, then applies the whole filter to each
candidate. So an index changes which keys are *read*, never which rows come back: the answer is the
answer the scan would have given, and the test suite asserts that by running each filter against a
second, unindexed collection and comparing.

A condition inside an `$or` is not used, even on an indexed field, because it does not hold for
every matching document. A condition sitting *beside* the `$or` is — `{"age":30,"$or":[…]}` uses the
`age` index.

A sorted query uses an index for its filter. Aggregation uses one exactly as a query does.

### `state`, and why it matters

A new index is `building` until a background walk has filed every existing document. While it is
building it is maintained by every write and not yet used by queries, so it can never answer a query
with a partial picture. `201` from the `POST` means the definition is agreed; check
`GET .../indexes` for `"state":"ready"`.

Building is quick on a small collection and proportional to size on a large one. Nothing blocks
meanwhile: writes and reads carry on and queries use the scan.

### What is stored, and what is derived

The **definition** is a log entry, exactly like a collection drop. It commits by quorum, survives a
leader change, reaches a replica that was down for it, and comes back after a restart — so it takes
`?w=` and answers `202 {"status":"staged"}` when the quorum is short.

The **postings** are rebuilt when a node opens the collection. That is deliberate: an index derived
from the documents cannot end up disagreeing with them, whatever a crash interrupted. The cost is a
background build after a restart, and `state` is where you see it.

### Sharded clusters: define an index once

An index belongs to the collection, not to whichever shard group holds its keys today. Creating one
records it in a cluster-wide catalogue as well as on the groups that own the collection now, and a
group that gains the collection later — you added a shard, a migration moved keys, a node came back
from a restart — reads the catalogue, defines the index in its own log, and builds it. Dropping one
works the same way in reverse, including for a group that was unreachable when you asked.

You do not create an index again after adding a shard, and you do not wait for a collection to
spread across the ring before indexing it.

Reconciliation is a background loop, so there is a gap of a few seconds between a group taking its
first key and that group's index being usable. A group without the index answers the same rows from
a scan, and `GET .../indexes` on the router reports the index as `building` until every group that
holds the collection can use it.

```
$ curl -s localhost:8080/collections/users/indexes    # through the router
{"collection":"users",
 "indexes":[{"name":"by_age","field":"age","state":"building","documents":812,"values":41}]}
```

### Bounds

- One field per index, up to eight per collection. Values are indexed by value, and an array counts
  as one value.
- Index name: 1–64 bytes of `A-Za-z0-9_-`. Field path: up to 256 bytes and 16 dot-separated
  segments.
- Defining and dropping sit behind `auth.admin_keys` when you set that tier — see
  [section 16](#16-turn-on-authentication). Listing does not.

---

## 8. Watching for changes

```
GET /collections/:name/changes
```

A long-lived [server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
stream of everything that commits to a collection. Open one in a second terminal:

```bash
curl -N localhost:8080/collections/orders/changes
```

`-N` matters — without it curl buffers and you see nothing until the stream ends. The first thing
back tells you where you are:

```
event: open
data: {"collection":"orders","position":412}
retry:2000
```

Now write from the first terminal:

```bash
curl -X PUT localhost:8080/collections/orders/docs/o1 -H 'Content-Type: application/json' \
  -d '{"value":{"region":"eu","amount":35,"status":"paid"}}'
curl -X PUT localhost:8080/collections/orders/docs/o1 -H 'Content-Type: application/json' \
  -d '{"value":{"region":"eu","amount":35,"status":"refunded"}}'
curl -X DELETE localhost:8080/collections/orders/docs/o1
```

and the watcher prints:

```
event: change
id: 413
data: {"lsn":413,"op":"insert","key":"o1","value":{"region":"eu","amount":35,"status":"paid"}}

event: change
id: 414
data: {"lsn":414,"op":"update","key":"o1","value":{"region":"eu","amount":35,"status":"refunded"}}

event: change
id: 415
data: {"lsn":415,"op":"delete","key":"o1"}
```

Four kinds of event: `insert`, `update`, `delete` and `drop`. Inserts and updates carry the whole
document. A delete carries the key it removed. A `drop` is the whole collection going away, in one
event rather than one delete per key.

You see changes that **committed**. A write that is durable but has not reached a quorum is one a
leader change can still take back, so it is not published until it can no longer be undone.
Deleting a key that was not there publishes nothing: it changed nothing.

A stream may be opened before the collection exists, on a shard or a router. It waits at position
zero and captures the first writes without creating a directory or a collection-list entry.

### Picking up where you left off

Every event's `lsn` is a position, and it is also the SSE `id`. Pass it back as `after`:

```bash
curl -N 'localhost:8080/collections/orders/changes?after=414'
```

and you get 415 onward. A browser's `EventSource` does this for you — it resends the last `id` as
`Last-Event-ID`, which this endpoint reads the same way.

The history has a horizon. A node keeps the last `changefeed.buffer_events` events per collection
(1024 by default). The log itself does not stand in for them, because compaction keeps each key's
latest value rather than the sequence of values it held. Ask for a position older than that and you
are refused rather than quietly resumed in the middle:

```json
{"error":"that position is older than the change buffer still holds","resume_floor":998}
```

That is a `410`, and `resume_floor` is a position the server *will* accept. The recovery is to
re-read what you care about with `/query` and resubscribe from there. A subscriber that falls behind
while connected gets the same news in-band and the stream ends:

```
event: error
data: {"error":"this subscriber fell behind the change buffer; resubscribe from `resume_floor`","resume_floor":998}
```

### Filtering

`filter` is the same filter `/query` takes:

```bash
curl -N --get localhost:8080/collections/orders/changes \
  --data-urlencode 'filter={"status":"paid"}'
```

with one rule worth stating: **the filter applies to events that carry a document.** A delete
carries none, so deletes come through regardless. That is deliberate — if they were filtered out,
you would watch `{"status":"paid"}`, see a document arrive, never hear that it was deleted, and go
on believing it is there.

If you want only writes, say so by kind:

```bash
curl -N 'localhost:8080/collections/orders/changes?ops=insert,update'
```

`ops` takes any of `insert`, `update`, `delete`, `drop`.

### What to know before you build on it

- **Any node in the group can serve it.** A replica's stream lags the way its reads do. Add
  `?read=primary` if you need the leader's, and it refuses with `503` rather than answering short.
  It keeps refusing, too: if that node later stops leading, the stream ends with an `error` event
  carrying the `position` to resume from. `?read=quorum` is a `400`: a read index makes one answer
  linearizable, and a stream is not one answer.
- **The key you opened with keeps being checked.** Remove it from `auth.api_keys` while the stream
  runs and the stream ends with an `error` frame saying so, within a couple of seconds. There is no
  position on that one: what a resume needs is a credential, not a place to start from.
- **Delivery is at-least-once.** A reconnect from a position you already processed is exact; if you
  reconnect from a position you *stored* before processing, you will see the tail again. Make your
  handler idempotent — the `lsn` is a natural dedup key.
- **A node counts its subscribers.** `changefeed.max_subscribers` (64) is per collection, and past
  it you get `503` with `Retry-After`.
- **It costs something while you watch.** Resolving a document large enough not to be cached in
  memory is a disk read on the write path. A feed with no subscriber costs one atomic load per
  commit, and stops recording `changefeed.idle_retention_ms` after the last subscriber leaves.

`/metrics` reports `subscribers`, `buffered`, `position`, `resume_floor`, `published` and `overruns`
per collection under `changefeed`.

### Over a WebSocket instead

If SSE does not suit — a proxy that buffers it, a client library without one — the same feed is on
`/changes/ws`, with the same `after`, `filter`, `ops` and `read`:

```bash
websocat 'ws://localhost:8080/collections/orders/changes/ws?ops=insert,update'
```

Every frame is one JSON text message. There is no event envelope, so the name and the resume
position are inside the object:

```json
{"type":"open","collection":"orders","position":412}
{"type":"change","position":"413","lsn":413,"op":"insert","key":"o1","value":{"amount":35}}
```

`position` is always a string — an LSN on a shard, a cluster token on a router — so you pass it back
to `?after=` unchanged. Refusals happen on the handshake rather than after it: a position the buffer
no longer holds is a `410` on the upgrade, so you never get a socket that opens and immediately
closes and leaves you guessing why.

One thing to plan for: while `auth.api_keys` is set, this route needs a credential header, and a
browser cannot set one on a `WebSocket`. Terminate the key in front of the node, or use SSE through
a same-origin proxy.

### Watching a sharded collection

Ask a router the same question and you get the whole collection, not one group's share of it:

```bash
curl -N localhost:8080/collections/orders/changes
```

```
event: open
data: {"collection":"orders","shards":["http://s1:8081","http://s2:8082"],"position":"eyJyaW5nIjo..."}

event: change
id: eyJyaW5nIjo...
data: {"lsn":413,"op":"insert","key":"o1","value":{"amount":35},"shard":"http://s1:8081"}
```

Two differences from a single group, and both come from the same fact: **each shard group numbers
its own log**, so LSN 413 on one group has nothing to do with LSN 413 on another.

- Every event says which `shard` published it. Events from one group arrive in that group's order;
  across groups the logs are independent, so no order is invented.
- The position is an opaque token holding one LSN per group, and it is still the SSE `id`, so
  `?after=<token>` and a browser's `Last-Event-ID` both resume every group where it stopped. Treat
  it as opaque.

`?read=replica` is refused here. A group's position is honourable by a node whose log the group
agrees on, so the router subscribes to each group's leader; when one fails over it reconnects to the
promoted replica at the same position, and you see nothing.

**When the shard layout changes.** Adding a shard, or rebalancing across the ones you have, moves
keys between groups. If your stream is open while that happens, you get:

```
event: topology
data: {"ring":8842116...,"shards":[...],"added":["http://s3:8083"],"removed":[],"position":"eyJ..."}
```

and the stream carries on with the new groups. If you were disconnected while it happened, the
position you saved names a layout that no longer maps keys the same way, and you get a `409` —
resubscribe with no position and re-read what you care about with `/query`.

Keys moving between groups during a handover do not emit document events; client writes during a
handover still emit their ordinary changes, and topology events stay visible.

---

## 9. Getting changes pushed to you

```
POST   /collections/:name/webhooks
GET    /collections/:name/webhooks
GET    /collections/:name/webhooks/:id
DELETE /collections/:name/webhooks/:id
```

Holding a stream open means being up when the change happens. A webhook is the same feed the other
way round: the node keeps the subscription and posts batches to you.

```bash
curl -s -X POST localhost:8081/collections/orders/webhooks \
  -H 'content-type: application/json' \
  -d '{"id":"billing","url":"https://billing.internal/dew","secret":"keep-this","ops":"insert,update"}'
```

```json
{"id":"billing","collection":"orders","url":"https://billing.internal/dew",
 "filter":null,"ops":"insert,update","signed":true,
 "delivery":{"position":412,"delivered":0,"attempts":0,"failures":0,"gaps":0},
 "replicated_to":["http://127.0.0.1:8083"]}
```

Register on the group's **leader** — a replica refuses with `403`, the way it refuses any other
write. The registration is majority-committed, so whichever node leads next holds the same
destination; a `503` means it was not confirmed and may still commit. `replicated_to` is the peers
the leader also woke directly, and `unreachable` the ones it could not — a peer in that list catches
up on its own rather than needing the request re-run.

A new subscription starts *from now on*, at the collection's current position. Re-posting the same
`id` replaces the URL, secret and filter and **keeps the position** — that is how you rotate a
secret, and how you restart one that stopped.

### What arrives at your endpoint

```
POST /dew HTTP/1.1
content-type: application/json
X-Dew-Subscription: billing
X-Dew-Delivery: 6f1c9e2a-…
X-Dew-Timestamp: 1757116800
X-Dew-Signature: sha256=9ab3…

{"subscription":"billing","collection":"orders","delivery":"6f1c9e2a-…","node":"shard-1",
 "position":417,"events":[{"lsn":416,"op":"insert","key":"o1","value":{"amount":35}}]}
```

Verify it before you trust it. The signature is HMAC-SHA256 over `"<X-Dew-Timestamp>.<raw body>"`
with your secret, hex-encoded:

```python
import hashlib, hmac, time

def verify(secret, timestamp, body, signature):
    expected = "sha256=" + hmac.new(
        secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature) and abs(time.time() - int(timestamp)) < 300
```

Sign over the raw bytes, not a re-serialized object. The timestamp is inside the signed string so a
delivery someone captured cannot be replayed at you later — check it against your own clock too.

### Failures, retries and back pressure

Answer `2xx` and the position moves. Answer anything else, or nothing at all, and the *same* batch
comes back with the *same* `X-Dew-Delivery`, on a delay that doubles from `initial_backoff_ms` to
`max_backoff_ms` with a per-subscription spread so two failing subscriptions do not retry in
lockstep. There is no attempt ceiling: a bounded retry would drop events quietly, which is worse
than a subscription you can see is behind.

After the `2xx`, DewDB commits the position to a majority in the shard group before advancing the
local cursor, so a promoted replica resumes after the last acknowledged event. Registrations and
removals also commit to a majority; offline replicas and new members recover them through log
catch-up. An unconfirmed administration request returns `503` and may still commit, so inspect its
state before retrying.

The way to make it stop is `410 Gone`. That disables the subscription with the reason recorded, and
re-registering the id turns it back on.

Watch it from the outside:

```bash
curl -s localhost:8081/collections/orders/webhooks/billing
```

```json
{"id":"billing", "…":"…",
 "delivery":{"position":417,"delivered":112,"attempts":115,"failures":0,"gaps":0,
             "last_error":null}}
```

`failures` is the current run of consecutive failures, `attempts` counts retries, and `gaps` is the
number of times your endpoint fell so far behind that the node could no longer hold the events for
it. That last one is the bound worth planning around: the backlog lives in the same
`changefeed.buffer_events` ring the streams use (1024 by default), so an endpoint down longer than
that many writes resumes at the floor and says so in `gaps` and `last_error`. Size
`changefeed.buffer_events` against how long your endpoint may plausibly be down.

Three more things to know:

- **Delivery is at-least-once.** The position moves after your `2xx`, so a node that crashes between
  your answer and its own write will send that batch again. Dedup on `lsn`, or on `X-Dew-Delivery`
  if you handle a batch as a unit. Budget for a redelivery around a failover.
- **The leader delivers.** Each node keeps its own position and the delivering one advances it.
- **Register on the group, not on the router.** A router answers `501` and names the groups; on a
  sharded collection each group needs its own registration, and each delivers its own keys.

---

## 10. Write concerns and durability

Every mutating request takes `?w=` and `?wtimeout=`.

```bash
# durable on the leader (default)
curl -s -X PUT 'localhost:8081/collections/users/docs/u1?w=1' \
  -H 'content-type: application/json' -d '{"value":{"name":"ada"}}'

# durable on a majority of voters, wait up to 2s
curl -s -X PUT 'localhost:8081/collections/users/docs/u1?w=majority&wtimeout=2000' \
  -H 'content-type: application/json' -d '{"value":{"name":"ada"}}'

# every voting replica plus the leader
curl -s -X DELETE 'localhost:8081/collections/users/docs/u1?w=all'
```

| `w` | Waits for |
|---|---|
| `1` (default) | the leader's own fsync |
| `majority` | a majority of the leader plus its voting replicas |
| `all` | all of them |
| a number | that many acknowledgements, clamped to the group size |

Concurrent writes share a group commit. A write waits for a sync covering its own append; joining
while an earlier batch is syncing puts it in the next batch.

If the concern is not met in time you get `202` — the write **is** durable on the leader, it did not
reach as many nodes as you asked for:

```json
{"id":"u1","status":"replaced","warning":"write concern not met","acks":1,"required":2}
```

That is a signal about the cluster, not a failed write: do not blindly retry it. Check replica lag
in `/metrics`.

Replication starts before you are acknowledged, and the local fsync runs alongside the replica round
trips, so `w=majority` costs the slower of the two rather than their sum.

While a voting-set change is in flight, `w=majority` means a majority of *each* set — the one being
left and the one being joined — so acknowledgements from one side alone will not satisfy it. The
`required` number in a `202` is then a floor rather than an exact count.

### A write that loses the leadership term is refused

Every concern, `w=1` included, is fenced against the leadership term the append was made under. The
node checks that it leads when the handler starts, again when it appends, and once more before it
answers. Lose the term at any of those points and the write comes back `503`:

```json
{"error":"no longer leading users; retry against the current primary"}
```

The handler's first check happens before the write gate and the key locks, so a request that queues
behind a data movement or another write to the same key can be several seconds old by the time it
appends. An append made after the term moved is unreplicated at a term no quorum will commit, and
the next leader truncates it.

`w=1` is the case this exists for. `w=majority` reports `acks: 1 required: 2` and a client can see
it fell short; `w=1` needs one acknowledgement and the local node is it. Retry: the router already
fails a `503` over to the rest of the group, which is where the node that took the term is.

Document size does not affect any of this. A body over 2 MiB is refused with `413`, and anything the
write path accepts can be replicated, so a large document meets `w=majority` the same way a small
one does.

---

## 11. Collection administration

All of these work at runtime; nothing needs a restart.

```bash
# what collections exist (a router unions every shard group, or answers 502 if one is down)
curl -s localhost:8081/collections

# drop a collection — leader only; a replicated log entry, so it takes ?w= like a write
curl -s -X DELETE 'localhost:8081/collections/users?w=majority'

# reclaim space now instead of waiting for the scheduler — leader only
curl -s -X POST localhost:8081/collections/users/compact

# write an index snapshot to shorten the next restart's replay
curl -s -X POST localhost:8081/collections/users/snapshot
```

A drop is a log entry, not a file deletion: it commits by quorum, survives a leader change, and
reaches a replica that was down for it. So it answers like a write — `202 {"status":"staged"}` when
the quorum is short — and the collection stays a tombstone until something is written to the name
again. Through a router the pending answer carries: a top-level `202` when every group answered and
any of them staged, so the aggregate never promises a removal no group committed.

Compaction reports what it reclaimed:

```json
{"collection":"users","status":"compacted","wal_id":7,"documents":1204,
 "bytes_before":41943040,"bytes_after":9437184,"dead_ratio_before":0.77}
```

You normally do not call compaction or snapshot by hand: the background scheduler compacts once a
collection's dead-byte ratio and size cross their thresholds, and snapshots on an interval. Tune or
disable it under `maintenance` in the config.

`409` from compaction means a compaction or a snapshot transfer is already running — retry later.
Compaction runs on the leader, because a replica's log must stay streamable for repair.

Compaction keeps the frames its replicas still need rather than dropping them, up to
`maintenance.wal_retention_bytes` (64 MiB by default), so ordinary lag does not turn into a full
snapshot. A replica further behind than that budget resyncs from a snapshot. On the scheduled path a
run that would not reclaim at least `maintenance.compaction_min_wal_bytes` is skipped, because the
retained tail is itself dead bytes and would otherwise be rewritten every interval; calling
`/compact` by hand runs it regardless.

### Naming

Names beginning with `_` are reserved for system logs — `_config` holds the voting set — and are
refused with `403` on the public API. Every other name is 1–128 characters of `a-z0-9._-`, not
`.`-prefixed, not ending in `.`, `.tmp` or `.old`, and not a Windows device name (`con`, `nul`,
`com1`…); anything else is `400`. Both checks read the name after percent-decoding, so `%5Fconfig`
is `_config` and is refused as one.

Uppercase letters are refused rather than folded: the name is a directory as well as a replicated
identity, and `Orders` beside `orders` is one directory on Windows and macOS and two on Linux. A
data directory holding a collection under such a name refuses to open — rename the directory to its
canonical form first.

Keys are not restricted that way — any string is a key — and the router escapes each path segment it
forwards, so a key containing `/`, `?` or `#` is stored under the name you wrote it with.

---

## 12. Add replicas and watch a failover

Three nodes, each with its own data directory and port.

`n1.json` — the primary:

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

`n2.json` — a replica:

```json
{
  "node_id": "n2",
  "role": "shard",
  "shard_role": "replica",
  "listen_addr": "127.0.0.1:9502",
  "data_dir": "./n2",
  "primary_addr": "http://127.0.0.1:9501",
  "peers": ["http://127.0.0.1:9501", "http://127.0.0.1:9503"]
}
```

`n3.json` is the same with `node_id` `n3`, port `9503`, `data_dir ./n3`, and `peers` naming 9501 and
9502.

Two rules the node will warn you about if you break them:

- `peers` lists the **other** nodes, never this one — otherwise the majority threshold is computed
  against an inflated cluster size.
- every node in `replicas` should also be in `peers`, so it can vote.

Start all three, then write with `w=majority`:

```bash
curl -s -X PUT 'localhost:9501/collections/users/docs/u1?w=majority' \
  -H 'content-type: application/json' -d '{"value":{"name":"ada"}}'
```

Check what the leader thinks of its replicas:

```bash
curl -s localhost:9501/metrics | python -c "import json,sys; print(json.dumps(json.load(sys.stdin)['replication'], indent=2))"
```

```json
"replication": {
  "role": "primary", "term": 0, "durable_lsn": 1, "commit_index": 1,
  "replica_count": 2, "voting_replicas": 2, "learners": [], "max_replica_lag": 0,
  "repairs": {"gaps": 0, "divergences": 0, "resyncs_triggered": 0},
  "replicas": [{"voting": true, "url": "http://127.0.0.1:9502", "matched": {"users": 1}, "lag": 0}]
}
```

A direct write to a replica is refused with `403` — writes go to the leader, or through a router.
Reads work on any node.

### Watch a failover

Stop `n1` (`Ctrl-C`). Within a few seconds — `heartbeat_timeout_secs`, 6 by default, plus election
jitter — one of the survivors takes over:

```bash
curl -s localhost:9502/health
curl -s localhost:9503/health
```

One of them now reports `"leader":true` at a higher `term`. Writes work against it immediately:

```bash
curl -s -X PUT 'localhost:9502/collections/users/docs/u2?w=majority' \
  -H 'content-type: application/json' -d '{"value":{"name":"lin"}}'
```

Bring `n1` back. It rejoins as a follower — a node that was leading does not assume it still is —
discovers the new leader, resyncs, and starts receiving frames again. Watch `repairs` in the new
leader's metrics: a gap here is normal and self-healing, and a rising `resyncs_triggered` means
chains kept breaking and a full snapshot was used instead.

None of this needs an operator. Elections require a majority of voters, so a two-of-three cluster
keeps serving writes and a one-of-three partition does not elect itself. A node cut off from its
voters does not raise its term: it asks whether it could win first, is told no, and stops — so it
cannot depose the leader that was elected without it when the partition heals.

Each node persists its term and vote together, so restarting cannot reopen a vote already cast.

The reverse case is covered too. A leader that can still *hear* its replicas but can no longer
*reach* a majority — an asymmetric partition, where every inbound signal looks healthy — probes them
itself and steps down inside its own term. Look for `checkquorum` in its logs:
`"No reply from a majority in 6s; stepping down as leader of term N"`.

---

## 13. Add a node at runtime

A node admitted while the cluster is running joins as a **learner**: it replicates immediately and
can serve reads, and is counted in no quorum. That is the first half of adding a voter — a node
nobody has been shipping frames to holds nothing, so admitting it straight into the quorum would
narrow every majority to the nodes that do hold entries. Promote it once it has caught up.

Start the new node telling it what it is:

```json
{
  "node_id": "n4",
  "role": "shard",
  "shard_role": "replica",
  "membership_mode": "learner",
  "listen_addr": "127.0.0.1:9504",
  "data_dir": "./n4",
  "primary_addr": "http://127.0.0.1:9501"
}
```

`membership_mode: "learner"` matters: a lone `voter` replica with no peers would elect itself the
moment its timeout expired. The node warns you loudly if you get this wrong.

Admit it at the shard **leader**:

```bash
curl -s -X POST localhost:9501/cluster/members \
  -H 'content-type: application/json' \
  -d '{"url":"http://127.0.0.1:9504","node_id":"n4","shard_role":"replica",
       "follows":"http://127.0.0.1:9501"}'
```

```json
{"status":"joined","url":"http://127.0.0.1:9504","voting":false,
 "follows":"http://127.0.0.1:9501","version":2,
 "note":"learners replicate but are not counted in any quorum"}
```

Catch-up starts on the leader's next tick — you do not have to write anything to kick it off. The
learner shows up in the leader's metrics under `learners`, and its acknowledgements never count
toward `w=majority`.

Remove it again:

```bash
curl -s -X DELETE 'localhost:9501/cluster/members?url=http://127.0.0.1:9504'
```

Send membership changes to a shard leader, not a router. A follower answers `409` and tells you the
primary's address; a router says so plainly. A *voting* member is demoted first, then removed from
the view.

### Change the voting set

The voting set lives in the log, not in the cluster view, and it moves through joint consensus: a
transitional entry naming both the old and new sets, then the new set. While the transition is in
force, every decision — the commit index, an election tally, `w=majority` — needs a majority of each
set separately, so there is no moment at which the leaving and joining halves could each decide on
their own.

Read the set in force from any shard node:

```bash
curl -s localhost:9501/cluster/configuration
```

```json
{"voters":["http://127.0.0.1:9501","http://127.0.0.1:9502","http://127.0.0.1:9503"],
 "outgoing":null,"joint":false,"uncommitted":false}
```

Promote the learner by sending the set you *want*, not a delta, to the leader:

```bash
curl -s -X POST localhost:9501/cluster/configuration \
  -H 'content-type: application/json' \
  -d '{"voters":["http://127.0.0.1:9501","http://127.0.0.1:9502",
                 "http://127.0.0.1:9503","http://127.0.0.1:9504"]}'
```

Removing one is the same call with that node left out.

Things worth knowing before you run it:

- **Let the learner catch up first.** Compare its `matched` in the leader's `/metrics` against the
  leader's tail. Promoting a node that is far behind narrows every majority until it arrives.
- **A change that removes the leader hands office over first.** It is answered `409` with a
  `primary` field naming the voter that took over; send the same request there and it applies.
  Nothing is appended on the old leader, so this costs no failover and no downtime.
- **A node has to be a member first.** Admit it with `/cluster/members`; a `voters` entry naming a
  stranger is refused with that instruction.
- **One change at a time.** A different change while one is still in flight is refused; retrying the
  *same* change is not — that is how you finish one. Disconnecting or timing out does not cancel an
  in-flight transition; read `/cluster/configuration` before retrying.
- **Each node once.** A `voters` list naming the same node twice is refused, and two spellings that
  resolve to the same `host:port` — a different scheme, a trailing slash, another letter case — are
  the same node.
- **`503` means in force but short of a quorum.** The entry is durable and deciding; nothing is
  half-applied. Check `joint` in `GET /cluster/configuration` and retry the same body. If the leader
  dies mid-change, whoever is elected next completes it.

Moving leadership on its own, without changing who votes — draining a node before maintenance, say:

```bash
# to a voter you pick, or send an empty body to take the readiest one
curl -s -X POST localhost:9501/cluster/transfer-leadership \
  -H 'content-type: application/json' -d '{"to":"http://127.0.0.1:9502"}'
```

The leader holds its writes still, brings the target up to its own tail, and tells it to stand for
election immediately, so nothing waits out a heartbeat timeout. `422` means it was refused and
nothing happened; `503` means it was attempted and did not finish, and this node still leads.

A learner that is never promoted is a read replica and hot standby — promotion is about who gets to
vote and whose acknowledgement counts toward `w=majority`.

---

## 14. Shard with a router

A router holds no data: it hashes `collection:key`, finds the owning group, and forwards. Clients
talk to routers and never need to know which shard owns what.

`examples/cluster/shard1.json` / `examples/cluster/shard2.json` are ordinary primaries (ports 8081
and 8082, own data dirs, optionally with replicas as in
[§12](#12-add-replicas-and-watch-a-failover)).

`examples/cluster/router.json`:

```json
{
  "node_id": "router-1",
  "role": "router",
  "listen_addr": "127.0.0.1:8080",
  "data_dir": "./data/examples/router",
  "shard_map": [
    {
      "start_hash": 0,
      "end_hash": 9223372036854775808,
      "node_url": "http://127.0.0.1:8081",
      "replica_urls": ["http://127.0.0.1:8083"]
    },
    {
      "start_hash": 9223372036854775808,
      "end_hash": 0,
      "node_url": "http://127.0.0.1:8082"
    }
  ]
}
```

A router needs ownership in its config to boot: either a `shard_map` covering the whole 64-bit space
with no overlap, or a `ring`. Give each shard `replica_urls` so reads and writes there can fail over
— the node warns when a shard has none.

Now use the cluster through the router as if it were one node:

```bash
curl -s -X POST localhost:8080/collections/users/docs \
  -H 'content-type: application/json' -d '{"value":{"name":"ada"}}'

curl -s localhost:8080/collections/users/docs/u1
curl -s 'localhost:8080/collections/users/query?limit=100'
curl -s localhost:8080/collections
```

What the router does for you:

- **Failover.** If a group's primary stops answering, it tries the replicas and caches whichever one
  answers as that group's primary.
- **Leadership tracking.** A background probe every few seconds follows elections, so reads redirect
  without waiting for a write to fail first. The highest-term node answering as primary wins, so a
  partitioned old primary cannot reclaim traffic.
- **Cross-shard queries.** Filters, sort and projection are pushed to every group; sorted results are
  merged, unsorted ones are paged with a cursor that tracks each shard separately.
- **Fan-out admin.** Drop, compact and snapshot reach every group and report per node; `207` means
  some nodes failed and the body says which. A drop answers `200` only when every group committed
  and `202` when they all answered but at least one staged.
- **Listing is all-or-nothing.** `GET /collections` unions every group, and a group that cannot
  answer makes it a `502` naming that group — a partial union reads as a collection that is not
  there.

Router state is visible in its metrics:

```bash
curl -s localhost:8080/metrics | python -c "import json,sys; print(json.dumps(json.load(sys.stdin)['router'], indent=2))"
```

```json
"router": {"shards": [
  {"shard":"http://127.0.0.1:8081","effective_primary":"http://127.0.0.1:8083",
   "failed_over":true,"replicas":["http://127.0.0.1:8083"]}]}
```

### Move to a hash ring

Explicit ranges work, and every topology change on them is a hand-written re-partition. A
consistent-hash ring makes adding or removing a shard cost roughly `1/n` of the keyspace instead.

Publish a ring at a shard **leader** (not a router). Look before you leap:

```bash
curl -s -X POST 'localhost:8081/cluster/ring?dry_run=true' \
  -H 'content-type: application/json' \
  -d '{"vnodes":128,"shards":[
        {"node_url":"http://127.0.0.1:8081","replica_urls":["http://127.0.0.1:8083"]},
        {"node_url":"http://127.0.0.1:8082","replica_urls":["http://127.0.0.1:8084"]}]}'
```

Once a ring is in force, the same dry run reports an exact `moved_fraction` and the per-pair
`transfers`, computed over the ring's arcs rather than sampled. Drop `?dry_run=true` to apply.

Things to know:

- **`/cluster/ring` moves ownership without moving data.** It is refused when any current owner
  holds data, and the refusal names which one. Use it to establish the *initial* ring, and use
  `/cluster/migrate` ([§15](#15-add-a-shard-without-downtime)) for every change after that.
- **Establish the ring before loading data.** In development `allow_unsafe_ring_changes` forces a
  publish through.
- **Once a ring is in force, shards enforce ownership themselves.** A shard asked for a key it does
  not own answers `409` naming the owner, and routers follow that redirect automatically. This is
  what keeps a topology change safe while views are still converging.
- **A node is its `host:port`, case-insensitively.** The scheme, a trailing slash, a path and host
  case are outside its identity, so re-spelling a shard's URL moves no keys. Listing one host twice
  under two spellings is `422`. `localhost` and `127.0.0.1` are still two different nodes.
- **The layout is bounded.** At most 1024 shards, at most 4096 `vnodes`, and at most 2^20
  `shards x vnodes` tokens. The product usually binds. Over a ceiling is `422` with the number in
  the message, on the dry run too.

The published view propagates on its own: routers pull it from whichever peer is furthest ahead,
followers pull it when a heartbeat advertises a better one, and every node takes the best view any
peer it can name is holding on its three-second catalogue round. "Better" is the version and, at
equal versions, the publisher's node id — so two operators who published at the same moment converge
on one of the two rather than staying split. Confirm everyone agrees:

```bash
for p in 8080 8081 8082; do curl -s localhost:$p/cluster | python -c \
  'import json,sys; v=json.load(sys.stdin); print(v["seen_by"], v["version"], v["model"])'; done
```

```
router-1 2 ring
shard-1  2 ring
shard-2  2 ring
```

Give routers the same ring in their config so they can boot before the published view reaches them;
after that, the durable view is what decides.

---

## 15. Add a shard without downtime

`POST /cluster/migrate` takes the same target ring but **copies the data before ownership moves**.
Send it to a shard leader that already holds a ring.

Start the new shard (`shard3.json`, port 8085, its own data dir), then:

```bash
curl -s -X POST localhost:8081/cluster/migrate \
  -H 'content-type: application/json' \
  -d '{"vnodes":128,"shards":[
        {"node_url":"http://127.0.0.1:8081","replica_urls":["http://127.0.0.1:8083"]},
        {"node_url":"http://127.0.0.1:8082","replica_urls":["http://127.0.0.1:8084"]},
        {"node_url":"http://127.0.0.1:8085","replica_urls":[]}]}'
```

```json
{"status":"migrating","migration_id":"9f2c…","version":3,"moved_fraction":0.33,
 "transfers":[{"from":"http://127.0.0.1:8081","to":"http://127.0.0.1:8085","fraction":0.17},
              {"from":"http://127.0.0.1:8082","to":"http://127.0.0.1:8085","fraction":0.16}],
 "note":"bulk copying in the background; writes pause only during finalization; watch GET /cluster/migrate"}
```

Watch it:

```bash
curl -s localhost:8081/cluster/migrate
```

```json
{"in_progress":true,
 "migration":{"id":"9f2c…","started_by":"shard-1","phase":"copy",
              "target":["http://127.0.0.1:8081","http://127.0.0.1:8082","http://127.0.0.1:8085"]},
 "local_progress":{"id":"9f2c…","phase":"copy","pushed":8400,"total":12000,"done":false,"error":null}}
```

What happens, and what your clients see:

1. **Copy** — each source pushes the keys it is giving away, in batches, at a pace you can tune
   (`data_movement`). Reads and writes are served normally throughout; queries and aggregates hide
   the destination copies until ownership flips.
2. **Finalize** — a short barrier: the keys actually about to move are refused, with `503` and
   `Retry-After: 1`. Everything else keeps working.
3. **Flip** — the new ring is published; ownership moves as each node adopts it.
4. **Cleanup** — sources delete what they handed over, after re-checking each key against the live
   view.

Change your mind before the flip:

```bash
curl -s -X DELETE localhost:8081/cluster/migrate
```

Ownership never moved, so nothing is lost; the copies already pushed are unreferenced.

If the coordinating node dies mid-migration, the plan is in the cluster view: sources keep pushing
and coordination resumes on its own — on whoever holds office next, whether that node learns of the
plan by winning the election, by adopting the view afterwards, or by booting into it.

Removing a shard is the same call with that node left out of the target ring — its keys are copied
away first, and cleanup reaches it even though it is no longer in the ring.

### Automatic rebalancing

Off by default. Turn it on per shard node and the cluster reconciles ownership with membership by
itself, through the same copy-then-flip path:

```json
"rebalance": { "enabled": true, "interval_secs": 10, "stabilization_secs": 30 }
```

Membership churn is debounced over `stabilization_secs`, so a rolling restart does not trigger a
handover. One node coordinates; the others stay out of the way.

Check what it would do — this works on any node:

```bash
curl -s localhost:8081/cluster/rebalance
```

```json
{"enabled":true,"interval_secs":10,"stabilization_secs":30,"coordinator":true,
 "in_progress":false,"balanced":false,
 "current_shards":["http://127.0.0.1:8081"],
 "desired_shards":["http://127.0.0.1:8081","http://127.0.0.1:8085"],
 "moved_fraction":0.5,"transfers":[…]}
```

---

## 16. Turn on authentication

All credentials are optional, and the node warns at boot while any is unset.

```json
"auth": {
  "internal_secret": "cluster-shared-secret",
  "api_keys": ["client-key-1", "client-key-2"],
  "admin_keys": ["operator-key-1"],
  "upstream_api_key": "client-key-1"
}
```

- `api_keys` guards the public API. Several keys at once means rotation is append-then-remove.
- `admin_keys` guards `/cluster/*`, `DELETE /collections/:name`, defining or dropping a secondary
  index, and every webhook route. Leave it empty and those routes fall back to `api_keys`.
- `internal_secret` guards `/internal/*`, the node-to-node routes. Use the same value on every node.
- `upstream_api_key` is the key this node presents when it calls another node's public API — a
  router needs it when the shards are locked down.

Then:

```bash
curl -s localhost:8081/collections/users/docs/u1 -H 'x-api-key: client-key-1'
# or
curl -s localhost:8081/collections/users/docs/u1 -H 'Authorization: Bearer client-key-1'
```

`/health` stays open so probes keep working. `/metrics` does not, because it exposes topology. An
internal secret never unlocks the public API and an API key never unlocks internal routes.
Comparisons are constant-time.

An admin key opens the public API too, so one credential covers reading a collection and then
dropping it. The tier is topology, destruction and schema: compact and snapshot stay on `api_keys`,
and so does deleting a single document.

On a router, set `admin_keys` there as well and make `upstream_api_key` a key the shards accept as an
admin key — a routed collection drop is forwarded to each shard on the public path:

```json
"auth": {
  "api_keys": ["client-key-1"],
  "admin_keys": ["operator-key-1"],
  "upstream_api_key": "operator-key-1"
}
```

### Rotating a key

`api_keys` and `admin_keys` are re-read from the config file while the node runs. Append the new key,
restart nothing, move your clients over, then remove the old one — within about five seconds the node
stops accepting it:

```bash
# after editing dew.json to drop "client-key-1"
curl -s -o /dev/null -w '%{http_code}\n' localhost:8081/collections/users/docs/u1 \
  -H 'x-api-key: client-key-1'   # 401
```

Removing a key also ends what it had already opened. A change stream — SSE or WebSocket — gets a
last `error` frame saying the credential is no longer accepted, and a webhook subscription that key
registered disables itself with the reason in `delivery.disabled`. Nothing keeps reading the
collection on a credential you took away.

`internal_secret` and `upstream_api_key` are wired into the node's outbound clients at boot, so
changing either takes a restart. The node logs a warning when it sees one change under it.

TLS is terminated in front of the node; keep `/internal/*` off untrusted networks.

---

## 17. Monitoring

### Health, for load balancers

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:8081/health
```

`200` when healthy, `503` when degraded — with the reason spelled out:

```json
{"status":"degraded","node_id":"n2","role":"shard","leader":false,"term":4,
 "reasons":["no primary heartbeat for 19s"]}
```

### Metrics, for humans

```bash
curl -s localhost:8081/metrics | python -m json.tool
```

The four numbers worth alerting on:

| Signal | Where | Means |
|---|---|---|
| `replication.max_replica_lag` | leader | how far the furthest replica is behind |
| `replication.repairs.resyncs_triggered` | leader | chains keep breaking; replicas are taking full snapshots |
| `replication.flow_control.writes_rejected` | leader | writes are being refused for backlog |
| `storage.collections[].dead_ratio` | any | space waiting to be reclaimed by compaction |

`GET /cluster/configuration` is worth watching too: `joint: true` for longer than a moment means a
membership change did not finish.

### Metrics, for Prometheus

```bash
curl -s 'localhost:8081/metrics?format=prometheus'
```

```
dewdb_leader{node_id="n1"} 1
dewdb_term{node_id="n1"} 4
dewdb_commit_index{node_id="n1"} 10421
dewdb_durable_lsn{node_id="n1"} 10421
dewdb_replication_lag_lsn{node_id="n1",replica="http://127.0.0.1:9502"} 0
dewdb_wal_bytes{node_id="n1",collection="users"} 41943040
dewdb_request_duration_ms_bucket{node_id="n1",method="PUT",route="/collections/:name/docs/:id",le="2.5"} 812
```

### Logs

```json
"logging": { "level": "info", "format": "json" }
```

JSON gives one object per line, stamped with `node_id` and a subsystem `target` (`election`, `vote`,
`checkquorum`, `read_index`, `membership`, `replication`, `repair`, `migration`, `compaction`,
`router_probe`, …). `RUST_LOG` overrides the configured level and takes per-target directives, so
`RUST_LOG=info,repair=debug,election=debug` turns up the subsystem you are chasing.

---

## 18. Backups and restarts

**Restarts** need nothing from you. A node replays its log from the last index snapshot, truncates a
torn tail, and re-stages any uncommitted entries instead of publishing them.

**Backups.** Stop the node and copy its `data_dir`; or, on a live node, take a snapshot first and
copy the collection directory — a WAL plus its index snapshot is self-describing. Copy
`applied.meta` and `applied.pos` with it: after a compaction they are the record that a collection
was dropped and of the configuration in force. If a `wal.cut` file is present, copy that too.

```bash
curl -s -X POST localhost:8081/collections/users/snapshot
cp -r ./data/users /backup/users-$(date +%F)
```

**Restoring one node into a live cluster** is usually best done by letting it resync: start it empty
pointing at its primary, and the leader streams it a snapshot.

**Config changes to topology.** `shard_map`, `ring` and membership in the config file are a
*bootstrap seed*. After the first boot the durable view wins, and an edit is logged as ignored. Two
ways to change topology:

- through the API — `/cluster/members`, `/cluster/configuration`, `/cluster/ring`,
  `/cluster/migrate` (preferred: online, validated and propagated);
- by stopping the node, deleting `cluster.meta` from its `data_dir`, and starting it again to
  re-seed from config. That file also holds this node's copy of the index catalogue; the copy comes
  back from its peers within a few seconds, and index definitions are durable in each group's log
  either way.

Deleting a file to recover is specific to `cluster.meta`. If a node refuses to start because
`applied.meta`, `applied.pos` or `replication.meta` is unreadable, restore it or the whole collection
directory from a backup, or let the node resync from its leader — deleting it tells the node to
publish everything in the log, including entries the cluster never committed.

The voting set is a third thing: it lives in the `_config` log, so config seeds it only until a
configuration entry exists, and after that `/cluster/configuration` is the only way to change it.

Everything else in the config — timeouts, flow control, maintenance thresholds, logging, auth — is
read at boot, so a restart is all it takes. A key the config schema does not recognise fails the
boot rather than defaulting quietly, so a typo shows up immediately.

---

## 19. Handling responses in a client

Write a client against these and it will behave well during elections and topology changes:

| Code | Meaning | Do |
|---|---|---|
| `200`/`201` | applied, write concern met | continue |
| `202` | durable, write concern **not** met (`acks`, `required`); from a router's collection drop or index change, at least one group staged | treat as written **here**; a `202` is not a promise the entry survives a leader change |
| `400` | bad request — `limit` above the cap, an unknown `read` preference, a cursor from a differently shaped query, a collection name outside `a-z0-9._-` | fix the request |
| `403` | direct write to a follower, or a `_`-prefixed collection name | use the router, or the primary the reply names |
| `404` | no such document, or no such collection | expected for reads and patches; check the collection name before assuming the document is gone |
| `409` | wrong owner (`owner` in body), a stale-view conflict, or a cursor issued against an older shard layout | retry via the router; restart the scan for a stale cursor |
| `410` | a change-stream position older than the buffer holds | resume from the `resume_floor` in the body |
| `413` | the request body is over 2 MiB, or a bulk write has more documents than `max_uncommitted_frames` | split it — this one does not clear on retry |
| `422` | a ring that fails validation, or a refused voting-set change | fix the layout or the set |
| `429` | every scan slot on the node is taken: aggregation, or a filtered or sorted page | back off and retry |
| `502` | no node in the group could be reached, including for `/collections` and `/query` | retry with backoff |
| `503` + `Retry-After` | replication backlog, a key mid-handover, or a `primary`/`quorum` read this node cannot answer yet | back off and retry; it clears itself |
| `503` *no longer leading* | the node lost the leadership term while the write was in flight; nothing was acknowledged | retry — the router already sends it to the rest of the group |
| `207` | fan-out partially failed, or a bulk write whose items did not all meet their concern | inspect the per-node or per-item results |

Two rules of thumb: retries are safe for `PUT`, `PATCH` and `DELETE` on a known key (they are
idempotent), and a `202` is not a failure — it is a durability report.

---

## 20. Client snippets

### JavaScript

```js
const base = 'http://localhost:8080';
const headers = { 'content-type': 'application/json', 'x-api-key': 'client-key-1' };

export async function put(collection, id, value, w = 'majority') {
  const r = await fetch(`${base}/collections/${collection}/docs/${id}?w=${w}`, {
    method: 'PUT', headers, body: JSON.stringify({ value }),
  });
  const body = await r.json();
  if (r.status === 202) console.warn('write concern not met', body); // durable, under-replicated
  else if (!r.ok) throw new Error(body.error ?? r.statusText);
  return body;
}

export async function* queryAll(collection, filter) {
  let cursor = null;
  do {
    const q = new URLSearchParams({ limit: '500', filter: JSON.stringify(filter) });
    if (cursor) q.set('cursor', cursor);
    const page = await fetch(`${base}/collections/${collection}/query?${q}`, { headers })
      .then(r => r.json());
    yield* page.items;
    cursor = page.next_cursor;
  } while (cursor);
}
```

### Python

```python
import json, requests

BASE = "http://localhost:8080"
S = requests.Session()
S.headers["x-api-key"] = "client-key-1"

def put(collection, key, value, w="majority"):
    r = S.put(f"{BASE}/collections/{collection}/docs/{key}",
              params={"w": w}, json={"value": value})
    if r.status_code == 202:
        print("durable but under-replicated:", r.json())   # not a failure
    elif not r.ok:
        r.raise_for_status()
    return r.json()

def query_all(collection, filter=None, page=500):
    cursor = None
    while True:
        params = {"limit": page}
        if filter:
            params["filter"] = json.dumps(filter)
        if cursor:
            params["cursor"] = cursor
        body = S.get(f"{BASE}/collections/{collection}/query", params=params).json()
        yield from body["items"]
        cursor = body.get("next_cursor")
        if not cursor:
            return
```

### Bulk load

```python
def bulk_load(collection, docs, chunk=500):
    for i in range(0, len(docs), chunk):
        batch = [{"id": k, "value": v} for k, v in docs[i:i + chunk]]
        r = S.post(f"{BASE}/collections/{collection}/docs/bulk",
                   params={"w": "majority"}, json=batch)
        r.raise_for_status()
```

---

## 21. Troubleshooting

| Symptom | What to check |
|---|---|
| Writes return `403` | You are writing to a follower. Use the router, or the primary named in the reply. |
| Writes return `202` | `acks`/`required` in the body; then `max_replica_lag` and per-replica `matched` in the leader's metrics. |
| Writes return `503` with a backlog | The quorum is not acknowledging: check replicas are reachable and watch `writes_rejected`. |
| Reads return `409` | The caller's view is stale; the body names the owner. Routers retry for you — a direct client should follow it. |
| No leader anywhere | `/health` on each shard, compare `dewdb_term`, read the `election` and `vote` logs. Elections need a majority of voters. After a failed pre-vote, candidates recover newer collection histories from their voting peers before retrying. |
| A leader stepped down with nobody replacing it | `checkquorum` in its logs: it could not reach a majority. Its term does not move, so this is one-sided reachability, not an election. |
| `read=quorum` returns `503` | The body names which step failed — leadership too new, no confirmation from a majority, or the index not yet visible. All are retryable; a direct request to the shard gives the precise reason a router's answer omits. |
| A voting-set change returns `503` | `GET /cluster/configuration`. `joint: true` means the transition is in force and deciding; retry the same body against the leader. |
| A replica never catches up | `repairs` counters; a rising `resyncs_triggered` means full snapshots are being used — the replica is further behind than `maintenance.wal_retention_bytes` keeps for it. |
| Nodes disagree about topology | `version` from `GET /cluster` on each node; they converge by version, so watch it move. |
| A ring change was refused | It names the node that holds data — use `/cluster/migrate`, which copies first. |
| A migration never finishes | `GET /cluster/migrate` on each source; a destination that has not adopted the plan refuses batches and the push retries. |
| A webhook stopped firing | `GET /collections/:name/webhooks/:id`. `disabled` means the endpoint answered `410`; a rising `failures` with a `last_error` means it is failing and being retried; `delivering: false` means this node is not the leader any more. |
| A webhook lost events | `gaps` in its delivery state. The endpoint fell further behind than `changefeed.buffer_events` holds, so it resumed at the floor. Raise the buffer, or reconcile with `/query`. |
| Disk keeps growing | `dead_ratio` per collection, and whether `maintenance.enabled` is true. Compaction runs on the leader. |
| A node will not boot | The log's `boot`/`config` lines name the exact field. An unreadable `cluster.meta` or `replication.meta` is deliberate: delete it only if you intend to re-seed. |

---

## Where next

- [features.md](features.md) — every capability, with the guarantee and the bound that goes with it.
- [documentation.md](documentation.md) — the reference manual: config fields, endpoints, on-disk
  formats, and how consensus, replication and sharding work underneath.
