//! Bounded-memory collection transfer, used when incremental repair cannot converge.

use crate::storage::index::{AppliedMeta, INDEX_FILENAME, IndexSnapshot};
use crate::storage::{Collection, Database};
use crate::util::remove_dir_with_retry;
use axum::body::Body;
use futures::TryStreamExt;
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;
use tracing::info;
use uuid::Uuid;

const SNAPSHOT_MAGIC: &[u8; 8] = b"DEWSNAP1";
pub const SNAPSHOT_CONTENT_TYPE: &str = "application/vnd.dewdb.snapshot-v1";
pub const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;
const SNAPSHOT_CHANNEL_DEPTH: usize = 2;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_SNAPSHOT_FILES: usize = 10_000;
const MAX_FILENAME_BYTES: usize = 255;

/// Matches `IndexSnapshot` on the wire without cloning the in-memory map.
#[derive(Serialize)]
struct BorrowedIndexSnapshot<'a> {
    last_wal_id: u64,
    last_offset: u64,
    last_lsn: u64,
    last_term: u64,
    map: &'a std::collections::BTreeMap<String, crate::storage::index::IndexEntry>,
}

struct ChannelWriter {
    sender: tokio::sync::mpsc::Sender<Result<Vec<u8>, io::Error>>,
    buffer: Vec<u8>,
}

impl ChannelWriter {
    fn new(sender: tokio::sync::mpsc::Sender<Result<Vec<u8>, io::Error>>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(SNAPSHOT_CHUNK_BYTES),
        }
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let full = std::mem::replace(&mut self.buffer, Vec::with_capacity(SNAPSHOT_CHUNK_BYTES));
        self.sender
            .blocking_send(Ok(full))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "snapshot client disconnected"))
    }

    fn finish(mut self) -> io::Result<()> {
        self.send_buffer()
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let supplied = bytes.len();
        while !bytes.is_empty() {
            let room = SNAPSHOT_CHUNK_BYTES - self.buffer.len();
            let take = room.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == SNAPSHOT_CHUNK_BYTES {
                self.send_buffer()?;
            }
        }
        Ok(supplied)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send_buffer()
    }
}

/// Adds protocol chunk lengths and a byte-count/CRC footer independently of HTTP framing.
struct EntryWriter<'a> {
    wire: &'a mut ChannelWriter,
    buffer: Vec<u8>,
    hasher: crc32fast::Hasher,
    written: u64,
}

impl<'a> EntryWriter<'a> {
    fn new(wire: &'a mut ChannelWriter) -> Self {
        Self {
            wire,
            buffer: Vec::with_capacity(SNAPSHOT_CHUNK_BYTES),
            hasher: crc32fast::Hasher::new(),
            written: 0,
        }
    }

    fn emit(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let len = self.buffer.len() as u32;
        self.wire.write_all(&len.to_le_bytes())?;
        self.wire.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        self.emit()?;
        self.wire.write_all(&0u32.to_le_bytes())?;
        self.wire.write_all(&self.written.to_le_bytes())?;
        self.wire.write_all(&self.hasher.finalize().to_le_bytes())
    }
}

impl Write for EntryWriter<'_> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let supplied = bytes.len();
        self.hasher.update(bytes);
        self.written = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "snapshot entry is too large")
            })?;
        while !bytes.is_empty() {
            let room = SNAPSHOT_CHUNK_BYTES - self.buffer.len();
            let take = room.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == SNAPSHOT_CHUNK_BYTES {
                self.emit()?;
            }
        }
        Ok(supplied)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit()
    }
}

fn lock_error(name: &str) -> io::Error {
    io::Error::other(format!("{} lock is poisoned", name))
}

fn start_entry(wire: &mut ChannelWriter, filename: &str) -> io::Result<()> {
    let name = filename.as_bytes();
    if name.is_empty() || name.len() > MAX_FILENAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid snapshot filename length",
        ));
    }
    wire.write_all(&[1])?;
    wire.write_all(&(name.len() as u16).to_le_bytes())?;
    wire.write_all(name)
}

struct SnapshotSpool(PathBuf);

impl Drop for SnapshotSpool {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn stream_file(wire: &mut ChannelWriter, filename: &str, path: &Path) -> io::Result<()> {
    start_entry(wire, filename)?;
    let mut entry = EntryWriter::new(wire);
    io::copy(&mut File::open(path)?, &mut entry)?;
    entry.finish()
}

fn stream_snapshot(
    collection: &Collection,
    sender: tokio::sync::mpsc::Sender<Result<Vec<u8>, io::Error>>,
) -> io::Result<()> {
    // Hold the compaction boundary while rotating under the WAL -> pending -> index lock order.
    // Frozen WALs remain immutable while writes continue in the new active WAL.
    let _boundary = collection
        .snapshot_boundary
        .lock()
        .map_err(|_| lock_error("snapshot boundary"))?;

    let spool = SnapshotSpool(
        collection
            .root_path
            .join(format!(".snapshot-{}.tmp", Uuid::new_v4())),
    );
    fs::create_dir(&spool.0)?;
    let spool_index = spool.0.join(INDEX_FILENAME);
    let spool_applied = spool.0.join("applied.meta");

    let frozen_through = {
        // Rotation would put a WAL id in a directory an install has replaced, and a released
        // handle has an empty index -- serving from one ships a snapshot that describes nothing.
        let _rewriting = collection
            .rewriting
            .lock()
            .map_err(|_| lock_error("collection rewrite"))?;
        if collection.released.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "collection handle is no longer active",
            ));
        }

        let mut wal = collection
            .wal_writer
            .lock()
            .map_err(|_| lock_error("WAL"))?;
        wal.current_wal.sync_data()?;
        let frozen_through = wal.current_wal_id;

        let _pending = collection
            .pending
            .lock()
            .map_err(|_| lock_error("pending"))?;
        let index = collection.index.read().map_err(|_| lock_error("index"))?;

        let persisted = BorrowedIndexSnapshot {
            // Replay above applied_lsn to include frames appended before pending insertion.
            last_wal_id: 0,
            last_offset: 0,
            last_lsn: wal.last_appended_lsn,
            last_term: wal.last_appended_term,
            map: &index,
        };
        let mut index_file = File::create(&spool_index)?;
        bincode::serialize_into(&mut index_file, &persisted).map_err(io::Error::other)?;
        index_file.sync_all()?;

        let mut applied_file = File::create(&spool_applied)?;
        serde_json::to_writer(
            &mut applied_file,
            &AppliedMeta {
                applied_lsn: collection.applied_lsn(),
                dropped: collection.is_dropped(),
                config: collection.committed_config(),
                handover: collection.committed_handover(),
                // Definitions only. The receiver installs the directory and reopens it, and the
                // reopen rebuilds the postings from the keys the snapshot actually carried.
                indexes: collection.committed_indexes(),
            },
        )
        .map_err(io::Error::other)?;
        applied_file.sync_all()?;

        // Rotation is what makes the streamed set immutable, and an active WAL with nothing appended
        // to it already is: a run of requests on an idle leader then costs no files. Not below id 2,
        // where there is no earlier WAL and the receiver refuses a snapshot carrying none.
        if wal.current_wal_size == 0 && frozen_through > 1 {
            frozen_through - 1
        } else {
            // An id already on disk fails `create_new` and takes the whole transfer down with it.
            let mut next_wal_id = frozen_through + 1;
            let mut next_path = collection.root_path.join(format!("wal-{:05}.log", next_wal_id));
            while next_path.exists() {
                next_wal_id += 1;
                next_path = collection.root_path.join(format!("wal-{:05}.log", next_wal_id));
            }
            let next_wal = OpenOptions::new()
                .create_new(true)
                .append(true)
                .read(true)
                .open(next_path)?;
            wal.current_wal = next_wal;
            wal.current_wal_id = next_wal_id;
            wal.current_wal_size = 0;
            frozen_through
        }
    };

    let mut wal_files = Vec::new();
    for entry in fs::read_dir(&collection.root_path)? {
        let entry = entry?;
        let filename = entry.file_name().to_string_lossy().to_string();
        if entry.file_type()?.is_file() {
            if let Some(id) = wal_id_from_filename(&filename).filter(|id| *id <= frozen_through) {
                wal_files.push((id, filename, entry.path()));
            }
        }
    }
    // By parsed id: `{:05}` stops padding at 99999, and wal-100000 sorts before wal-99999 by name.
    wal_files.sort_by_key(|(id, _, _)| *id);

    let mut wire = ChannelWriter::new(sender);
    wire.write_all(SNAPSHOT_MAGIC)?;
    stream_file(&mut wire, INDEX_FILENAME, &spool_index)?;
    stream_file(&mut wire, "applied.meta", &spool_applied)?;

    for (_, filename, path) in wal_files {
        stream_file(&mut wire, &filename, &path)?;
    }

    wire.write_all(&[0])?;
    wire.finish()
}

/// A two-chunk channel backpressures the file producer on slow clients.
pub fn snapshot_body(collection: Arc<Collection>) -> Body {
    let (sender, receiver) = tokio::sync::mpsc::channel(SNAPSHOT_CHANNEL_DEPTH);
    tokio::task::spawn_blocking(move || {
        let errors = sender.clone();
        if let Err(e) = stream_snapshot(&collection, sender) {
            let _ = errors.blocking_send(Err(e));
        }
    });

    Body::from_stream(futures::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    ))
}

fn is_wal_filename(filename: &str) -> bool {
    wal_id_from_filename(filename).is_some()
}

fn wal_id_from_filename(filename: &str) -> Option<u64> {
    filename
        .strip_prefix("wal-")
        .and_then(|s| s.strip_suffix(".log"))
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))?
        .parse()
        .ok()
}

fn safe_snapshot_filename(filename: &str) -> bool {
    let mut components = Path::new(filename).components();
    let one_normal =
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
    one_normal
        && (filename == INDEX_FILENAME || filename == "applied.meta" || is_wal_filename(filename))
}

async fn receive_snapshot<R: AsyncRead + Unpin>(
    reader: &mut R,
    target: &Path,
) -> io::Result<usize> {
    let mut magic = [0u8; SNAPSHOT_MAGIC.len()];
    reader.read_exact(&mut magic).await?;
    if &magic != SNAPSHOT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported snapshot stream",
        ));
    }

    let mut seen = HashSet::new();
    let mut file_count = 0usize;
    loop {
        let tag = reader.read_u8().await?;
        if tag == 0 {
            break;
        }
        if tag != 1 || file_count >= MAX_SNAPSHOT_FILES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid snapshot entry tag or file count",
            ));
        }

        let name_len = reader.read_u16_le().await? as usize;
        if name_len == 0 || name_len > MAX_FILENAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid snapshot filename length",
            ));
        }
        let mut name = vec![0u8; name_len];
        reader.read_exact(&mut name).await?;
        let filename = String::from_utf8(name).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "snapshot filename is not UTF-8")
        })?;
        if !safe_snapshot_filename(&filename) || !seen.insert(filename.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsafe or duplicate snapshot filename '{}'", filename),
            ));
        }

        let path = target.join(&filename);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        let mut hasher = crc32fast::Hasher::new();
        let mut written = 0u64;
        loop {
            let chunk_len = reader.read_u32_le().await? as usize;
            if chunk_len == 0 {
                break;
            }
            if chunk_len > SNAPSHOT_CHUNK_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot chunk exceeds protocol limit",
                ));
            }
            let mut chunk = vec![0u8; chunk_len];
            reader.read_exact(&mut chunk).await?;
            file.write_all(&chunk).await?;
            hasher.update(&chunk);
            written = written.checked_add(chunk_len as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "snapshot entry is too large")
            })?;
        }

        let expected_bytes = reader.read_u64_le().await?;
        let expected_crc = reader.read_u32_le().await?;
        if written != expected_bytes || hasher.finalize() != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot entry '{}' failed length/CRC validation", filename),
            ));
        }
        file.flush().await?;
        file.sync_all().await?;
        file_count += 1;
    }

    if !seen.contains(INDEX_FILENAME)
        || !seen.contains("applied.meta")
        || !seen.iter().any(|name| is_wal_filename(name))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot is missing required files",
        ));
    }

    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing).await? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot has trailing bytes",
        ));
    }
    Ok(file_count)
}

/// An install replaces rather than merges, so one stopping below `local_applied` -- this node's own
/// watermark -- would retract entries a quorum committed on our ack. A legitimate leader holds every
/// committed entry and so never sends one; divergent local frames are uncommitted and below the
/// watermark, which is why comparing watermarks and not tails still allows a divergence repair.
fn validate_staged_snapshot(target: &Path, local_applied: u64) -> io::Result<()> {
    let index: IndexSnapshot = bincode::deserialize_from(File::open(target.join(INDEX_FILENAME))?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let applied: AppliedMeta = serde_json::from_reader(File::open(target.join("applied.meta"))?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if applied.applied_lsn > index.last_lsn {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot applied watermark is beyond its WAL tail",
        ));
    }
    if applied.applied_lsn < local_applied {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot stops at applied lsn {} but this node has already published {}",
                applied.applied_lsn, local_applied
            ),
        ));
    }
    Ok(())
}

pub async fn replica_sync_from_primary(
    client: &reqwest::Client,
    primary_addr: &str,
    db: &Database,
    collection_name: &str,
) -> Result<(), String> {
    info!(target: "replica_sync", "Syncing collection '{}' from primary {}", collection_name, primary_addr);

    let url = format!(
        "{}/internal/snapshot?collection={}",
        primary_addr, collection_name
    );
    let resp = client
        .get(&url)
        .timeout(SNAPSHOT_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("Snapshot request failed: {}", e))?;

    // Named apart from the rest because it is a disagreement about what exists, not a transfer
    // that failed: retrying reaches the same answer, and the local copy is what to look at.
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(format!("{} holds no collection '{}'", primary_addr, collection_name));
    }
    if !resp.status().is_success() {
        return Err(format!("Primary returned {}", resp.status()));
    }

    let target = db.root_path.join(format!("{}.tmp", collection_name));
    if target.exists() {
        remove_dir_with_retry(&target)
            .map_err(|e| format!("Failed to wipe snapshot staging dir: {}", e))?;
    }
    fs::create_dir_all(&target)
        .map_err(|e| format!("Failed to create snapshot staging dir: {}", e))?;

    let stream = resp.bytes_stream().map_err(io::Error::other);
    let mut reader = StreamReader::new(stream);
    let received = match receive_snapshot(&mut reader, &target).await {
        Ok(count) => count,
        Err(e) => {
            let _ = remove_dir_with_retry(&target);
            return Err(format!("Snapshot stream failed: {}", e));
        }
    };

    // Stable across the transfer, not merely current: the caller holds the install lock and
    // `resyncing` sheds replication for this collection, so nothing can apply while we stream.
    let local_applied = db.existing_collection(collection_name).map_or(0, |c| c.applied_lsn());

    let verify_path = target.clone();
    if let Err(e) =
        tokio::task::spawn_blocking(move || validate_staged_snapshot(&verify_path, local_applied))
            .await
            .map_err(|e| format!("Snapshot validation task failed: {}", e))?
    {
        let _ = remove_dir_with_retry(&target);
        return Err(format!("Snapshot validation failed: {}", e));
    }

    if let Err(e) = db.install_staged_collection(collection_name, &target) {
        if target.exists() {
            let _ = remove_dir_with_retry(&target);
        }
        return Err(format!("Failed to install snapshot: {}", e));
    }

    info!(target: "replica_sync", files = received, "Collection '{}' restored and ready", collection_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Retention;
    use crate::replication::ReplicateRequest;
    use crate::test_support::{TestNode, live_put, make_frame, next_test_port, put_doc_http, temp_root};
    use futures::StreamExt;
    use std::io::Cursor;

    async fn committed_put(
        collection: &Arc<Collection>,
        key: &str,
        value: serde_json::Value,
    ) -> u64 {
        let lsn = collection.put(key.to_string(), value, 1).unwrap().3;
        collection.enqueue_commit().await.unwrap().unwrap();
        collection.apply_committed(lsn).unwrap();
        lsn
    }

    async fn collect_body(body: Body) -> Vec<u8> {
        let mut stream = body.into_data_stream();
        let mut wire = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(
                chunk.len() <= SNAPSHOT_CHUNK_BYTES,
                "an HTTP body frame exceeded the bounded producer buffer"
            );
            wire.extend_from_slice(&chunk);
        }
        wire
    }

    async fn collect_snapshot(collection: Arc<Collection>) -> Vec<u8> {
        collect_body(snapshot_body(collection)).await
    }

    async fn decode_bytes(bytes: Vec<u8>, target: &Path) -> io::Result<usize> {
        let stream = futures::stream::iter(vec![Ok::<_, io::Error>(Cursor::new(bytes))]);
        let mut reader = StreamReader::new(stream);
        receive_snapshot(&mut reader, target).await
    }

    /// Entry names in the order the stream carries them, which `receive_snapshot` discards.
    fn streamed_names(wire: &[u8]) -> Vec<String> {
        let mut names = Vec::new();
        let mut i = SNAPSHOT_MAGIC.len();
        while wire[i] == 1 {
            i += 1;
            let name_len = u16::from_le_bytes(wire[i..i + 2].try_into().unwrap()) as usize;
            i += 2;
            names.push(String::from_utf8(wire[i..i + name_len].to_vec()).unwrap());
            i += name_len;
            loop {
                let chunk = u32::from_le_bytes(wire[i..i + 4].try_into().unwrap()) as usize;
                i += 4;
                if chunk == 0 {
                    break;
                }
                i += chunk;
            }
            i += 12;
        }
        names
    }

    fn wal_ids_on_disk(root: &Path) -> Vec<u64> {
        let mut ids: Vec<u64> = fs::read_dir(root).unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| wal_id_from_filename(&e.file_name().to_string_lossy()))
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn wal_files_stream_in_id_order_once_the_padding_runs_out() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        for id in [99999u64, 100000] {
            fs::write(col.root_path.join(format!("wal-{:05}.log", id)), b"").unwrap();
        }
        {
            // Reaching these ids for real would take 5 TB of WAL; the scan is what is under test.
            let mut wal = col.wal_writer.lock().unwrap();
            wal.current_wal_id = 100001;
            wal.current_wal_size = 0;
        }

        let names = streamed_names(&collect_snapshot(col).await);
        let wals: Vec<&String> = names.iter().filter(|n| is_wal_filename(n)).collect();
        assert_eq!(wals, ["wal-00001.log", "wal-99999.log", "wal-100000.log"],
            "sorting by name puts wal-100000 before wal-99999");
    }

    #[tokio::test]
    async fn an_idle_leader_does_not_burn_a_wal_per_snapshot_request() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "k", 1);
        col.enqueue_commit().await.unwrap().unwrap();

        collect_snapshot(col.clone()).await;
        let settled = wal_ids_on_disk(&col.root_path);

        collect_snapshot(col.clone()).await;
        collect_snapshot(col.clone()).await;
        assert_eq!(wal_ids_on_disk(&col.root_path), settled,
            "a request answered from an untouched active WAL must not rotate");
    }

    #[tokio::test]
    async fn a_snapshot_still_serves_when_the_next_wal_id_is_already_taken() {
        let root = temp_root();
        let db = Database::new(&root).unwrap();
        let col = db.get_collection("c").unwrap();

        live_put(&col, "k", 1);
        col.enqueue_commit().await.unwrap().unwrap();

        let taken = col.wal_writer.lock().unwrap().current_wal_id + 1;
        fs::write(col.root_path.join(format!("wal-{:05}.log", taken)), b"").unwrap();

        let wire = collect_snapshot(col.clone()).await;
        let staged = root.join("staged");
        fs::create_dir_all(&staged).unwrap();
        decode_bytes(wire, &staged).await
            .expect("a leftover file at the next id must not take the transfer down with it");
    }

    #[tokio::test]
    async fn a_large_snapshot_streams_in_bounded_chunks_and_reopens_to_the_same_state() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        let payload = "x".repeat(256 * 1024);

        for i in 0..8 {
            committed_put(
                &source,
                &format!("k{}", i),
                serde_json::json!({
                    "i": i,
                    "payload": payload,
                }),
            )
            .await;
        }

        let wire = collect_snapshot(source).await;
        assert!(
            wire.len() > 2 * 1024 * 1024,
            "the fixture must span many bounded chunks"
        );

        let replica_db = Database::new(root.join("replica")).unwrap();
        let previous = replica_db.get_collection("events").unwrap();
        live_put(&previous, "obsolete", -1);
        previous.enqueue_commit().await.unwrap().unwrap();
        let staged = replica_db.root_path.join("events.tmp");
        fs::create_dir_all(&staged).unwrap();
        let count = decode_bytes(wire, &staged).await.unwrap();
        assert!(
            count >= 3,
            "index, applied watermark and WAL must all be separate entries"
        );
        validate_staged_snapshot(&staged, 0).unwrap();
        replica_db
            .install_staged_collection("events", &staged)
            .unwrap();

        let restored = replica_db.get_collection("events").unwrap();
        assert!(
            restored.get("obsolete").unwrap().is_none(),
            "the verified snapshot replaces rather than merges the old collection"
        );
        for i in 0..8 {
            let value = restored.get(&format!("k{}", i)).unwrap().unwrap();
            assert_eq!(value["i"], i);
            assert_eq!(value["payload"].as_str().unwrap().len(), payload.len());
        }
        assert!(
            !staged.exists(),
            "a successful installation consumes the staging directory"
        );
    }

    /// H18: `snapshot_handler` resolved with `get_collection`, so being *asked* for a collection
    /// created it -- a directory, a wal and an `Arc<Collection>` nothing evicts, per distinct name,
    /// on a tier that is unauthenticated on the default config.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_snapshot_of_a_collection_the_leader_does_not_have_is_refused() {
        let root = temp_root();
        let mut leader = TestNode::new("solo", next_test_port(), &root, "primary");
        leader.start();
        let client = reqwest::Client::new();

        // A real collection alongside it, so "nothing was created" is not just "nothing exists".
        assert!(put_doc_http(&client, &leader.url(), "k1", 1).await.is_success());

        let served = client.get(format!("{}/internal/snapshot?collection=ghost", leader.url()))
            .send().await.unwrap();
        assert_eq!(served.status(), reqwest::StatusCode::NOT_FOUND,
            "an empty snapshot cannot be told apart from one of a collection this node really holds");

        let live = leader.state.as_ref().unwrap().db.as_ref().unwrap().live_collections().unwrap();
        assert_eq!(live, vec!["t".to_string()], "serving a snapshot invented a collection: {:?}", live);

        // The asking half: named apart from a transfer that failed, and nothing local is touched.
        let replica_db = Database::new(root.join("replica")).unwrap();
        let local = replica_db.get_collection("ghost").unwrap();
        live_put(&local, "mine", 1);

        let error = replica_sync_from_primary(&client, &leader.url(), &replica_db, "ghost")
            .await.expect_err("there is nothing there to install");
        assert!(error.contains("holds no collection 'ghost'"), "unexpected refusal: {}", error);
        assert_eq!(replica_db.get_collection("ghost").unwrap().get("mine").unwrap(),
            Some(serde_json::json!({"v": 1})), "a refusal must leave the local copy alone");
        assert!(!replica_db.root_path.join("ghost.tmp").exists(),
            "and must not leave a staging directory behind");

        leader.kill();
    }

    #[tokio::test]
    async fn a_truncated_stream_keeps_the_existing_collection_live() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        committed_put(&source, "replacement", serde_json::json!({"v": 2})).await;
        let mut truncated = collect_snapshot(source).await;
        truncated.truncate(truncated.len() - 5);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/internal/snapshot",
            axum::routing::get({
                let truncated = truncated.clone();
                move || {
                    let truncated = truncated.clone();
                    async move { (axum::http::StatusCode::OK, truncated) }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let replica_db = Database::new(root.join("replica")).unwrap();
        let old = replica_db.get_collection("events").unwrap();
        live_put(&old, "original", 1);
        old.enqueue_commit().await.unwrap().unwrap();

        let result = replica_sync_from_primary(
            &reqwest::Client::new(),
            &format!("http://{}", address),
            &replica_db,
            "events",
        )
        .await;

        assert!(
            result.is_err(),
            "a stream without its final footer/end marker must be rejected"
        );
        let still_live = replica_db.get_collection("events").unwrap();
        assert_eq!(
            still_live.get("original").unwrap(),
            Some(serde_json::json!({"v": 1})),
            "download failure occurs before the live collection is released"
        );
        assert!(still_live.get("replacement").unwrap().is_none());
        assert!(
            !replica_db.root_path.join("events.tmp").exists(),
            "failed transfer staging must be cleaned up"
        );

        server.abort();
    }

    /// The source is a node that was leader and no longer is: its log stops below what this node
    /// has already published, and installing it would retract entries a quorum committed.
    #[tokio::test]
    async fn a_snapshot_that_stops_below_our_watermark_is_refused() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        committed_put(&source, "stale", serde_json::json!({"v": 1})).await;
        let wire = collect_snapshot(source).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/internal/snapshot",
            axum::routing::get({
                let wire = wire.clone();
                move || {
                    let wire = wire.clone();
                    async move { (axum::http::StatusCode::OK, wire) }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let replica_db = Database::new(root.join("replica")).unwrap();
        let local = replica_db.get_collection("events").unwrap();
        for i in 0..3 {
            committed_put(&local, &format!("k{}", i), serde_json::json!({"v": i})).await;
        }
        let published = local.applied_lsn();
        assert!(published > 1, "the fixture must put this node ahead of the snapshot");

        let error = replica_sync_from_primary(
            &reqwest::Client::new(),
            &format!("http://{}", address),
            &replica_db,
            "events",
        )
        .await
        .expect_err("a snapshot behind our own watermark must not install");
        assert!(error.contains("already published"), "unexpected refusal: {}", error);

        let still_live = replica_db.get_collection("events").unwrap();
        for i in 0..3 {
            assert_eq!(still_live.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": i})),
                "a refused install must leave every published entry readable");
        }
        assert!(still_live.get("stale").unwrap().is_none());
        assert_eq!(still_live.applied_lsn(), published);
        assert!(!replica_db.root_path.join("events.tmp").exists());

        server.abort();
    }

    /// Maintenance holds an `Arc` across the install that replaces the directory under it: a
    /// compaction allowed to finish publishes an empty index and retires the installed WALs.
    #[tokio::test]
    async fn a_compaction_holding_the_old_handle_cannot_finish_onto_an_installed_snapshot() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        for i in 0..4 {
            committed_put(&source, &format!("k{}", i), serde_json::json!({"v": i})).await;
        }
        let wire = collect_snapshot(source).await;

        let replica_db = Database::new(root.join("replica")).unwrap();
        let previous = replica_db.get_collection("events").unwrap();
        live_put(&previous, "obsolete", -1);
        previous.enqueue_commit().await.unwrap().unwrap();

        let staged = replica_db.root_path.join("events.tmp");
        fs::create_dir_all(&staged).unwrap();
        decode_bytes(wire, &staged).await.unwrap();
        replica_db.install_staged_collection("events", &staged).unwrap();

        let installed = replica_db.root_path.join("events");
        let wals_before = wal_ids_on_disk(&installed);
        let index_before = fs::read(installed.join(INDEX_FILENAME)).unwrap();

        let error = previous.compact(Retention::none()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound,
            "a handle the install released must refuse compaction outright");
        assert_eq!(wal_ids_on_disk(&installed), wals_before,
            "the refused compaction must not rotate or retire the installed WALs");
        assert_eq!(fs::read(installed.join(INDEX_FILENAME)).unwrap(), index_before,
            "nor overwrite the installed index with the released handle's empty one");

        let restored = replica_db.get_collection("events").unwrap();
        for i in 0..4 {
            assert_eq!(restored.get(&format!("k{}", i)).unwrap(), Some(serde_json::json!({"v": i})));
        }
    }

    /// The boot resync enumerates names off disk, and `adopt_collection_tails` has opened every one
    /// of them, so the watermark an install is measured against survives a restart with the log.
    #[tokio::test]
    async fn the_watermark_a_stale_snapshot_is_measured_against_survives_a_restart() {
        let root = temp_root();
        let published = {
            let db = Database::new(&root).unwrap();
            let col = db.get_collection("events").unwrap();
            committed_put(&col, "k", serde_json::json!({"v": 1})).await;
            col.applied_lsn()
        };
        assert!(published > 0);

        let reopened = Database::new(&root).unwrap();
        assert_eq!(reopened.existing_collection("events").unwrap().applied_lsn(), published,
            "a collection reopened at boot still has a watermark an install must clear");
        assert!(reopened.existing_collection("absent").is_none(),
            "and probing a name we have never held must not create it");
    }

    #[tokio::test]
    async fn a_snapshot_at_our_own_watermark_still_installs() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        committed_put(&source, "fresh", serde_json::json!({"v": 1})).await;
        let wire = collect_snapshot(source.clone()).await;

        let replica_db = Database::new(root.join("replica")).unwrap();
        let local = replica_db.get_collection("events").unwrap();
        committed_put(&local, "diverged", serde_json::json!({"v": 9})).await;
        assert_eq!(local.applied_lsn(), source.applied_lsn(),
            "the fixture must sit exactly at the snapshot's watermark, not below it");

        let staged = replica_db.root_path.join("events.tmp");
        fs::create_dir_all(&staged).unwrap();
        decode_bytes(wire, &staged).await.unwrap();
        validate_staged_snapshot(&staged, local.applied_lsn())
            .expect("equal watermarks are not a regression; only a lower one is");
    }

    #[tokio::test]
    async fn a_slow_snapshot_client_does_not_hold_the_active_wal_closed() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        committed_put(
            &source,
            "before",
            serde_json::json!({"payload": "x".repeat(1024 * 1024)}),
        )
        .await;

        // Leave the body unread so the two-slot channel blocks after WAL rotation.
        let body = snapshot_body(source.clone());
        tokio::time::sleep(Duration::from_millis(100)).await;
        tokio::time::timeout(
            Duration::from_secs(1),
            committed_put(&source, "after", serde_json::json!({"v": 2})),
        )
        .await
        .expect("a slow snapshot consumer must not block writes in the new active WAL");

        let wire = collect_body(body).await;
        let replica_db = Database::new(root.join("replica")).unwrap();
        let staged = replica_db.root_path.join("events.tmp");
        fs::create_dir_all(&staged).unwrap();
        decode_bytes(wire, &staged).await.unwrap();
        replica_db
            .install_staged_collection("events", &staged)
            .unwrap();

        let restored = replica_db.get_collection("events").unwrap();
        assert!(restored.get("before").unwrap().is_some());
        assert!(
            restored.get("after").unwrap().is_none(),
            "the later write belongs to the new active WAL, beyond this snapshot boundary"
        );
        assert!(source.get("after").unwrap().is_some());
    }

    #[tokio::test]
    async fn a_replica_never_acknowledges_frames_that_snapshot_installation_can_discard() {
        let root = temp_root();
        let mut replica = TestNode::new("replica", next_test_port(), &root, "replica");
        replica.membership_mode = "learner".to_string();
        replica.primary_addr = Some("http://127.0.0.1:1".to_string());
        replica.start();
        replica
            .state
            .as_ref()
            .unwrap()
            .resyncing
            .lock()
            .unwrap()
            .insert("events".to_string());

        let frame = make_frame(1, 1, 0, 0, "during", 1);
        let request = ReplicateRequest {
            collection: "events".to_string(),
            term: 1,
            lsn: 1,
            prev_lsn: 0,
            commit_index: Some(0),
            wal_frame: frame,
            frames: Vec::new(),
        };
        let response = reqwest::Client::new()
            .post(format!("{}/internal/replicate", replica.url()))
            .json(&request)
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(
            !replica.data_dir.join("events").exists(),
            "a refused in-flight frame must not create or advance the collection"
        );

        replica.kill();
    }

    #[tokio::test]
    async fn the_decoder_refuses_paths_outside_the_staging_directory() {
        let root = temp_root();
        let target = root.join("staged");
        fs::create_dir_all(&target).unwrap();

        let mut wire = Vec::new();
        wire.extend_from_slice(SNAPSHOT_MAGIC);
        wire.push(1);
        wire.extend_from_slice(&8u16.to_le_bytes());
        wire.extend_from_slice(b"../x.log");

        let error = decode_bytes(wire, &target).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!root.join("x.log").exists());
    }

    /// IB-008 follow-up: `<name>.tmp` is both the install staging directory and the directory a
    /// download streams into. Treating a complete-looking download as an install to finish let
    /// `existing_collection` -- called here, between the transfer and validation -- promote the
    /// staged directory out from under this function, so every first sync of a collection the
    /// replica does not already hold failed with the snapshot already live and unvalidated.
    #[tokio::test]
    async fn a_first_sync_does_not_install_the_download_before_it_is_validated() {
        let root = temp_root();
        let source_db = Database::new(root.join("source")).unwrap();
        let source = source_db.get_collection("events").unwrap();
        committed_put(&source, "k", serde_json::json!({"v": 1})).await;
        let wire = collect_snapshot(source).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/internal/snapshot",
            axum::routing::get({
                let wire = wire.clone();
                move || {
                    let wire = wire.clone();
                    async move { (axum::http::StatusCode::OK, wire) }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let replica_db = Database::new(root.join("replica")).unwrap();
        assert!(!replica_db.root_path.join("events").exists(), "the replica starts without it");

        replica_sync_from_primary(
            &reqwest::Client::new(),
            &format!("http://{}", address),
            &replica_db,
            "events",
        )
        .await
        .expect("a first sync of a collection the replica lacks must install the snapshot");

        assert_eq!(
            replica_db.get_collection("events").unwrap().get("k").unwrap(),
            Some(serde_json::json!({"v": 1})),
        );
        assert!(!replica_db.root_path.join("events.tmp").exists());

        server.abort();
    }
}
