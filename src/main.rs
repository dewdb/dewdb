use serde::{Deserialize, Serialize};

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

fn main() {
    let entry = LogEntry::Put {
        key: "example".into(),
        value: serde_json::json!({
            "message": "dewdb"
        }),
        ts: 0,
    };

    println!("{entry:?}");
}