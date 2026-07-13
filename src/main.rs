use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const WAL_ROTATION_LIMIT: u64 = 50 * 1024 * 1024;
const MAX_RECORD_SIZE: u64 = 10 * 1024 * 1024;
const INDEX_FILENAME: &str = "index-current.bin";

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "op", rename_all = "lowercase")]
enum LogEntry {
    Put {
        key: String,
        value: serde_json::Value,
        ts: u64,
    },
    Del {
        key: String,
        ts: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct IndexEntry {
    wal_id: u64,
    offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct IndexSnapshot {
    last_wal_id: u64,
    last_offset: u64,
    map: BTreeMap<String, IndexEntry>,
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> std::io::Result<()> {
    let root = PathBuf::from("./data");

    fs::create_dir_all(&root)?;

    let wal_path = root.join("wal-00001.log");

    let wal = File::create(&wal_path)?;

    let mut snapshot = IndexSnapshot {
        last_wal_id: 1,
        last_offset: 0,
        map: BTreeMap::new(),
    };

    let entry = LogEntry::Put {
        key: "example".into(),
        value: serde_json::json!({
            "message": "dewdb"
        }),
        ts: current_timestamp(),
    };

    snapshot.map.insert(
        "example".into(),
        IndexEntry {
            wal_id: 1,
            offset: 0,
        },
    );

    println!("{entry:?}");
    println!("{snapshot:#?}");
    println!("Created {:?}", wal_path);
    println!("File size: {}", wal.metadata()?.len());

    println!(
        "{} {} {}",
        WAL_ROTATION_LIMIT,
        MAX_RECORD_SIZE,
        INDEX_FILENAME
    );

    Ok(())
}