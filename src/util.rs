//! Dependency-free helpers shared across subsystems.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

const DIR_REMOVE_ATTEMPTS: usize = 5;

// Configs mix "http://host:port" and "host:port" for the same node, so
// comparisons must ignore the scheme or a node fails to recognise itself.
pub fn endpoint_of(url: &str) -> &str {
    url.trim_end_matches('/')
        .rsplit("//")
        .next()
        .unwrap_or(url)
}

pub fn same_endpoint(a: &str, b: &str) -> bool {
    endpoint_of(a) == endpoint_of(b)
}

// Windows can briefly hold a closed file open, so removal is retried instead of
// failing the caller.
pub fn remove_file_with_retry(path: &Path) -> io::Result<()> {
    let mut last_err = None;
    for attempt in 0..DIR_REMOVE_ATTEMPTS {
        match fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(20 * (attempt + 1) as u64));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to remove file")))
}

pub fn remove_dir_with_retry(path: &Path) -> io::Result<()> {
    let mut last_err = None;
    for attempt in 0..DIR_REMOVE_ATTEMPTS {
        match fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(20 * (attempt + 1) as u64));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to remove directory")))
}

// serialize/deserialize look dead: they are named by #[serde(with =
// "base64_bytes")] at the fields using them, so a dead-code sweep will
// wrongly flag them.
pub mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde::de;

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Serialize;
        let encoded = base64_encode(bytes);
        encoded.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64_decode(&s).map_err(de::Error::custom)
    }

    pub fn base64_encode(input: &[u8]) -> String {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = String::new();
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let combined = (b0 << 16) | (b1 << 8) | b2;
            result.push(CHARS[((combined >> 18) & 0x3F) as usize] as char);
            result.push(CHARS[((combined >> 12) & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                result.push(CHARS[((combined >> 6) & 0x3F) as usize] as char);
            } else {
                result.push('=');
            }
            if chunk.len() > 2 {
                result.push(CHARS[(combined & 0x3F) as usize] as char);
            } else {
                result.push('=');
            }
        }
        result
    }

    pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
        let input = input.trim_end_matches('=');
        let mut result = Vec::new();
        let mut buf: u32 = 0;
        let mut bits: u32 = 0;
        for c in input.chars() {
            let val = match c {
                'A'..='Z' => c as u32 - 'A' as u32,
                'a'..='z' => c as u32 - 'a' as u32 + 26,
                '0'..='9' => c as u32 - '0' as u32 + 52,
                '+' => 62,
                '/' => 63,
                _ => return Err(format!("Invalid base64 char: {}", c)),
            };
            buf = (buf << 6) | val;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                result.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_compare_across_scheme_and_trailing_slash() {
        assert!(same_endpoint("http://127.0.0.1:9501", "127.0.0.1:9501"));
        assert!(same_endpoint("http://127.0.0.1:9501/", "127.0.0.1:9501"));
        assert!(same_endpoint("https://127.0.0.1:9501", "http://127.0.0.1:9501"));
        assert!(!same_endpoint("http://127.0.0.1:9501", "127.0.0.1:9502"));
    }
}
