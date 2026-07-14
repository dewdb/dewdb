use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const WAL_ROTATION_LIMIT: u64 = 50 * 1024 * 1024; // 50 MB
const MAX_RECORD_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
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

#[derive(Serialize, Deserialize)]
struct IndexSnapshot {
    last_wal_id: u64,
    last_offset: u64,
    map: BTreeMap<String, IndexEntry>,
}

struct Collection {
    name: String,
    root_path: PathBuf,
    index: RwLock<BTreeMap<String, IndexEntry>>,
    wal_writer: std::sync::Mutex<WalsState>,
}

struct WalsState {
    current_wal: File,
    current_wal_id: u64,
    current_wal_size: u64,
}

struct Database {
    root_path: PathBuf,
}



impl Database {
    fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_path)?;
        Ok(Self { root_path })
    }
}

impl Collection {
    fn current_timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }


    fn put(&self, key: String, value: serde_json::Value) -> io::Result<(Vec<u8>, u64, u64)> {
        let entry = LogEntry::Put {
            key: key.clone(),
            value,
            ts: Self::current_timestamp(),
        };
        self.append(entry)
    }

    fn delete(&self, key: String) -> io::Result<(Vec<u8>, u64, u64)> {
        let entry = LogEntry::Del {
            key: key.clone(),
            ts: Self::current_timestamp(),
        };
        self.append(entry)
    }

  
    fn append(&self, entry: LogEntry) -> io::Result<(Vec<u8>, u64, u64)> {
        let json_bytes = serde_json::to_vec(&entry)?;
        let len = json_bytes.len() as u64;

        if len > MAX_RECORD_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Record exceeds maximum size"));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&json_bytes);
        let crc = hasher.finalize();

        let mut header = [0u8; 8];
        header[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        header[4..8].copy_from_slice(&crc.to_le_bytes());
        let frame_len = 8 + len;

        let mut frame = Vec::with_capacity(frame_len as usize);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&json_bytes);

        let mut wal = self.wal_writer.lock().unwrap();

        
        if wal.current_wal_size >= WAL_ROTATION_LIMIT {
            wal.current_wal.sync_all()?;
            wal.current_wal_id += 1;
            let new_path = self.root_path.join(format!("wal-{:05}.log", wal.current_wal_id));
            wal.current_wal = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&new_path)?;
            wal.current_wal_size = 0;
        }

        
        wal.current_wal.write_all(&header)?;
        wal.current_wal.write_all(&json_bytes)?;
        
        wal.current_wal.sync_data()?;

       
        let offset = wal.current_wal_size;
        wal.current_wal_size += frame_len;

        let wal_id = wal.current_wal_id;

        drop(wal);

        Ok((frame, wal_id, offset))
    }
}

fn main() -> io::Result<()> {
    let db = Database::new("./data")?;

    let collection_path = db.root_path.join("users");
    fs::create_dir_all(&collection_path)?;

    let wal_path = collection_path.join("wal-00001.log");

    let wal = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&wal_path)?;

    
    let current_wal_size = wal.metadata()?.len();

    let collection = Collection {
        name: "users".into(),
        root_path: collection_path,
        index: RwLock::new(BTreeMap::new()),
        wal_writer: std::sync::Mutex::new(WalsState {
            current_wal: wal,
            current_wal_id: 1,
            current_wal_size,
        }),
    };

    
    let (frame, wal_id, offset) = collection.put(
        "example".into(),
        serde_json::json!({ "message": "dewdb" }),
    )?;
    collection
        .index
        .write()
        .unwrap()
        .insert("example".into(), IndexEntry { wal_id, offset });

    println!(
        "PUT 'example' -> frame {} bytes | wal {} | offset {}",
        frame.len(),
        wal_id,
        offset
    );

    let (frame, wal_id, offset) = collection.delete("example".into())?;
    collection.index.write().unwrap().remove("example");

    println!(
        "DEL 'example' -> frame {} bytes | wal {} | offset {}",
        frame.len(),
        wal_id,
        offset
    );

    let wal = collection.wal_writer.lock().unwrap();
    println!("Collection: {}", collection.name);
    println!(
        "WAL id {} | tracked size {} | file size {}",
        wal.current_wal_id,
        wal.current_wal_size,
        wal.current_wal.metadata()?.len()
    );
    println!("Index entries: {}", collection.index.read().unwrap().len());

    println!(
        "{} {} {}",
        WAL_ROTATION_LIMIT, MAX_RECORD_SIZE, INDEX_FILENAME
    );

    Ok(())
}