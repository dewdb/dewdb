use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
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
    fn open(name: String, root_path: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root_path)?;

        let mut index = BTreeMap::new();
        let mut wal_files = Vec::new();

        for entry in fs::read_dir(&root_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.starts_with("wal-") && fname.ends_with(".log") {
                    let id_part = &fname[4..fname.len() - 4];
                    if let Ok(id) = id_part.parse::<u64>() {
                        wal_files.push((id, path));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        println!("[{}] Replaying all WALs...", name);
        for (id, path) in &wal_files {
            Self::replay_file_from(*id, path, 0, &mut index)?;
        }

        let current_wal_id = wal_files.last().map(|(id, _)| *id).unwrap_or(0) + 1;

        let wal_path = root_path.join(format!("wal-{:05}.log", current_wal_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        let current_wal_size = file.metadata()?.len();

        Ok(Self {
            name,
            root_path,
            index: RwLock::new(index),
            wal_writer: std::sync::Mutex::new(WalsState {
                current_wal: file,
                current_wal_id,
                current_wal_size,
            }),
        })
    }

    fn replay_file_from(wal_id: u64, path: &PathBuf, mut start_offset: u64, index: &mut BTreeMap<String, IndexEntry>) -> io::Result<()> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_len = file.metadata()?.len();

        if start_offset > file_len {
            start_offset = 0;
        }

        file.seek(SeekFrom::Start(start_offset))?;

        let mut offset = start_offset;
        let mut valid_end_offset = start_offset;

        loop {
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

            if len == 0 || (len as u64) > MAX_RECORD_SIZE {
                eprintln!("[{}] Invalid WAL frame length {}. Truncating.", path.display(), len);
                break;
            }

            let mut payload = vec![0u8; len as usize];
            if let Err(_) = file.read_exact(&mut payload) {
                eprintln!("[{}] Unexpected EOF while reading payload. Truncating.", path.display());
                break;
            }

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != crc {
                eprintln!("[{}] CRC mismatch. Truncating file at chunk boundary.", path.display());
                break;
            }

            if let Ok(entry) = serde_json::from_slice::<LogEntry>(&payload) {
                match entry {
                    LogEntry::Put { key, .. } => {
                        index.insert(key, IndexEntry { wal_id, offset });
                    },
                    LogEntry::Del { key, .. } => {
                        index.remove(&key);
                    }
                }
            }
            offset += 8 + len as u64;
            valid_end_offset = offset;
        }

        if valid_end_offset < file_len {
            file.set_len(valid_end_offset)?;
            println!("[{}] Truncated corrupted WAL file down to size {}", path.display(), valid_end_offset);
        }

        Ok(())
    }

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

    pub fn iter(&self) -> Vec<(String, IndexEntry)> {
        let index = self.index.read().unwrap();
        index.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn range(&self, start: Option<&str>, end: Option<&str>) -> Vec<(String, IndexEntry)> {
        let index = self.index.read().unwrap();

        let range_bound = (
            start.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
            end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
        );

        index.range::<str, _>(range_bound).map(|(k, v)| (k.clone(), *v)).collect()
    }

    fn get(&self, key: &str) -> io::Result<Option<serde_json::Value>> {
        let idx_entry = {
            let index = self.index.read().unwrap();
            index.get(key).copied()
        };

        if let Some(entry) = idx_entry {
            let path = self.root_path.join(format!("wal-{:05}.log", entry.wal_id));
            let mut file = File::open(&path)?;
            file.seek(SeekFrom::Start(entry.offset))?;

            let mut header = [0u8; 8];
            file.read_exact(&mut header)?;
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap());

            let mut payload = vec![0u8; len as usize];
            file.read_exact(&mut payload)?;

            if let Ok(LogEntry::Put { value, .. }) = serde_json::from_slice(&payload) {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn list_all(&self) -> io::Result<Vec<serde_json::Value>> {
        let index = self.index.read().unwrap();
        let mut results = Vec::new();
        for (key, _) in index.iter() {
           if let Some(val) = self.get(key)? {
               results.push(val);
           }
        }
        Ok(results)
    }
}

fn main() -> io::Result<()> {
    let db = Database::new("./data")?;

    let col = Collection::open("users".into(), db.root_path.join("users"))?;

    println!("Recovered {} index entries", col.index.read().unwrap().len());

    let docs = col.list_all()?;
    println!("list_all -> {} docs", docs.len());
    for doc in docs.iter().take(3) {
        println!("  {}", doc);
    }

    let key = format!("key:{}", Collection::current_timestamp() % 100000);
    let (_frame, wal_id, offset) = col.put(key.clone(), serde_json::json!({ "message": "dewdb", "written_by": "reads-commit" }))?;
    col.index
        .write()
        .unwrap()
        .insert(key.clone(), IndexEntry { wal_id, offset });

    match col.get(&key)? {
        Some(val) => println!("GET '{}' -> {}", key, val),
        None => println!("GET '{}' -> not found", key),
    }

    match col.get("missing-key")? {
        Some(val) => println!("GET 'missing-key' -> {}", val),
        None => println!("GET 'missing-key' -> not found"),
    }

    let entries = col.range(Some("key:"), None);
    println!("range 'key:'.. -> {} entries", entries.len());
    for (k, e) in entries.iter().take(5) {
        println!("  {} @ wal {} offset {}", k, e.wal_id, e.offset);
    }

    println!("iter -> {} total entries", col.iter().len());

    Ok(())
}