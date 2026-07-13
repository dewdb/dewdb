use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

fn main() {
    let entry = LogEntry::Put {
        key: "example".into(),
        value: serde_json::json!({
            "message": "dewdb"
        }),
        ts: 0,
    };

    let mut snapshot = IndexSnapshot {
        last_wal_id: 0,
        last_offset: 0,
        map: BTreeMap::new(),
    };

    snapshot.map.insert(
        "example".into(),
        IndexEntry {
            wal_id: 0,
            offset: 0,
        },
    );

    println!("{entry:?}");
    println!("{snapshot:#?}");
}