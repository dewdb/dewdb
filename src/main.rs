use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
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

struct WalsState {
    current_wal: File,
    current_wal_id: u64,
    current_wal_size: u64,
}

struct Collection {
    name: String,
    root_path: PathBuf,
    index: RwLock<BTreeMap<String, IndexEntry>>,
    wal_writer: Mutex<WalsState>,
}

struct Database {
    root_path: PathBuf,
}

impl Database {
    fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_path)?;
        Ok(Self { root_path })
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> std::io::Result<()> {
    let db = Database::new("./data")?;

    let collection_path = db.root_path.join("users");
    fs::create_dir_all(&collection_path)?;

    let wal_path = collection_path.join("wal-00001.log");

    let wal = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&wal_path)?;

    let collection = Collection {
        name: "users".into(),
        root_path: collection_path,
        index: RwLock::new(BTreeMap::new()),
        wal_writer: Mutex::new(WalsState {
            current_wal: wal,
            current_wal_id: 1,
            current_wal_size: 0,
        }),
    };

    let entry = LogEntry::Put {
        key: "example".into(),
        value: serde_json::json!({
            "message": "dewdb"
        }),
        ts: current_timestamp(),
    };

    {
        let mut index = collection.index.write().unwrap();

        index.insert(
            "example".into(),
            IndexEntry {
                wal_id: 1,
                offset: 0,
            },
        );
    }

    let snapshot = IndexSnapshot {
        last_wal_id: 1,
        last_offset: 0,
        map: collection.index.read().unwrap().clone(),
    };

    let wal = collection.wal_writer.lock().unwrap();

    println!("{entry:?}");
    println!("{snapshot:#?}");
    println!("Collection: {}", collection.name);
    println!("Root: {:?}", collection.root_path);
    println!("WAL id: {}", wal.current_wal_id);
    println!("WAL size: {}", wal.current_wal_size);
    println!("Current file size: {}", wal.current_wal.metadata()?.len());

    println!(
        "{} {} {}",
        WAL_ROTATION_LIMIT,
        MAX_RECORD_SIZE,
        INDEX_FILENAME
    );

    Ok(())
}