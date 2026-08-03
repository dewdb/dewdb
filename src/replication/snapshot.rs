//! Bulk collection transfer, used when incremental repair cannot converge.

use crate::storage::Database;
use crate::util::{base64_bytes, remove_dir_with_retry};
use serde::{Deserialize, Serialize};
use std::fs;
use std::time::Duration;
use tracing::info;

#[derive(Serialize, Deserialize)]
pub struct SnapshotFileEntry {
    pub filename: String,
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
}

pub async fn replica_sync_from_primary(
    client: &reqwest::Client,
    primary_addr: &str,
    db: &Database,
    collection_name: &str,
) -> Result<(), String> {
    info!(target: "replica_sync", "Syncing collection '{}' from primary {}", collection_name, primary_addr);

    let url = format!("{}/internal/snapshot?collection={}", primary_addr, collection_name);
    let resp = client.get(&url)
        .timeout(Duration::from_secs(30))
        .send().await
        .map_err(|e| format!("Snapshot request failed: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!("Primary returned {}", resp.status()));
    }

    let files: Vec<SnapshotFileEntry> = resp.json().await
        .map_err(|e| format!("Failed to parse snapshot response: {}", e))?;

    if files.is_empty() {
        info!(target: "replica_sync", "No files received for collection '{}', it may not exist on primary yet", collection_name);
        return Ok(());
    }

    let col_path = db.root_path.join(collection_name);
    let tmp_path = db.root_path.join(format!("{}.tmp", collection_name));

    if tmp_path.exists() {
        fs::remove_dir_all(&tmp_path).map_err(|e| format!("Failed to wipe tmp dir: {}", e))?;
    }
    fs::create_dir_all(&tmp_path).map_err(|e| format!("Failed to create tmp dir: {}", e))?;

    for entry in &files {
        let file_path = tmp_path.join(&entry.filename);
        fs::write(&file_path, &entry.data).map_err(|e| format!("Failed to write {}: {}", entry.filename, e))?;
    }

    let tombstone = db.release_collection(collection_name)
        .map_err(|e| format!("Failed to release collection handles: {}", e))?;

    let old_path = db.root_path.join(format!("{}.old", collection_name));
    if old_path.exists() {
        let _ = remove_dir_with_retry(&old_path);
    }
    if col_path.exists() {
        fs::rename(&col_path, &old_path).map_err(|e| format!("Failed to backup old col dir: {}", e))?;
    }
    fs::rename(&tmp_path, &col_path).map_err(|e| format!("Failed to finalize new col dir: {}", e))?;
    if old_path.exists() {
        let _ = remove_dir_with_retry(&old_path);
    }
    if let Some(path) = tombstone {
        let _ = fs::remove_file(path);
    }

    info!(target: "replica_sync", "Restored {} files for collection '{}'", files.len(), collection_name);

    let _ = db.get_collection(collection_name)
        .map_err(|e| format!("Failed to reopen collection after sync: {}", e))?;

    info!(target: "replica_sync", "Collection '{}' ready", collection_name);
    Ok(())
}
