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

/// The serialized shape is deliberately identical to `IndexSnapshot`, but borrows the map. This
/// keeps index serialization bounded instead of cloning the whole in-memory index before streaming.
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

/// Buffers one logical file into protocol chunks and appends a byte-count/CRC footer. Chunk lengths
/// are inside the wire stream, independent of the HTTP body's own framing.
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
    // Compaction cannot remove files while the producer holds this boundary. The active WAL is
    // rotated under the normal WAL/pending/index lock order, giving the stream an immutable prefix.
    // Only index serialization pauses writes; the network transfer reads frozen WALs while new
    // writes continue in the next WAL.
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
        let mut wal = collection
            .wal_writer
            .lock()
            .map_err(|_| lock_error("WAL"))?;
        wal.current_wal.sync_data()?;
        let frozen_through = wal.current_wal_id;
        let next_wal_id = frozen_through + 1;

        let _pending = collection
            .pending
            .lock()
            .map_err(|_| lock_error("pending"))?;
        let index = collection.index.read().map_err(|_| lock_error("index"))?;

        let persisted = BorrowedIndexSnapshot {
            // Resume from the beginning. This closes the append-before-stage window: a frame
            // already in the frozen WAL but not yet in `pending` is replayed above applied_lsn.
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
            },
        )
        .map_err(io::Error::other)?;
        applied_file.sync_all()?;

        let next_path = collection
            .root_path
            .join(format!("wal-{:05}.log", next_wal_id));
        let next_wal = OpenOptions::new()
            .create_new(true)
            .append(true)
            .read(true)
            .open(next_path)?;
        wal.current_wal = next_wal;
        wal.current_wal_id = next_wal_id;
        wal.current_wal_size = 0;
        frozen_through
    };

    let mut wal_files = Vec::new();
    for entry in fs::read_dir(&collection.root_path)? {
        let entry = entry?;
        let filename = entry.file_name().to_string_lossy().to_string();
        if entry.file_type()?.is_file()
            && wal_id_from_filename(&filename).is_some_and(|id| id <= frozen_through)
        {
            wal_files.push((filename, entry.path()));
        }
    }
    wal_files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut wire = ChannelWriter::new(sender);
    wire.write_all(SNAPSHOT_MAGIC)?;
    stream_file(&mut wire, INDEX_FILENAME, &spool_index)?;
    stream_file(&mut wire, "applied.meta", &spool_applied)?;

    for (filename, path) in wal_files {
        stream_file(&mut wire, &filename, &path)?;
    }

    wire.write_all(&[0])?;
    wire.finish()
}

/// Starts a blocking producer behind a two-chunk channel. Slow clients apply backpressure all the
/// way to the file reader instead of causing the server to accumulate the collection in memory.
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

fn validate_staged_snapshot(target: &Path) -> io::Result<()> {
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

    let verify_path = target.clone();
    if let Err(e) = tokio::task::spawn_blocking(move || validate_staged_snapshot(&verify_path))
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
    use crate::replication::ReplicateRequest;
    use crate::storage::FrameHeader;
    use crate::test_support::{TestNode, live_put, make_frame, next_test_port, temp_root};
    use futures::StreamExt;
    use std::io::Cursor;

    async fn committed_put(
        collection: &Arc<Collection>,
        key: &str,
        value: serde_json::Value,
    ) -> u64 {
        let (frame, wal_id, offset, lsn) = collection.put(key.to_string(), value, 1).unwrap();
        let header = FrameHeader::parse(&frame).unwrap();
        let payload =
            &frame[crate::storage::HEADER_LEN..crate::storage::HEADER_LEN + header.len as usize];
        let entry = collection.build_entry(wal_id, offset, payload);
        collection.stage(lsn, key.to_string(), wal_id, offset, Some(entry));
        collection.enqueue_commit().await.unwrap().unwrap();
        collection.apply_committed(lsn);
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
        validate_staged_snapshot(&staged).unwrap();
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

        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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

        // Do not poll the body yet. The two-slot channel fills and leaves the producer blocked on
        // the simulated slow client after it has rotated to an immutable snapshot WAL.
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

        let _ = fs::remove_dir_all(&root);
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
        let _ = fs::remove_dir_all(&root);
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

        let _ = fs::remove_dir_all(&root);
    }
}
