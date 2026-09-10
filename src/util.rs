//! Dependency-free helpers shared across subsystems.

use std::cmp::Ordering;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

const DIR_REMOVE_ATTEMPTS: usize = 5;

/// The `host:port` of `url`, with scheme, path, query and fragment cut away. Parsing only: sameness is
/// `same_endpoint` and map identity is `node_key`. A path can name a second node, so it is cut.
pub fn endpoint_of(url: &str) -> &str {
    let authority = url.split_once("//").map_or(url, |(_, rest)| rest);
    authority.split(['/', '?', '#']).next().unwrap_or(authority)
}

/// Host case is insensitive per the DNS spec, so `LOCALHOST:8080` and `localhost:8080` are one node --
/// two spellings counted twice inflate a voter set (L16). Compared, not lowercased, to stay allocation-free.
pub fn same_endpoint(a: &str, b: &str) -> bool {
    endpoint_of(a).eq_ignore_ascii_case(endpoint_of(b))
}

/// The canonical node identity: what to hash, key by and sort by. It has to agree with `same_endpoint`,
/// or one node spelled two ways gets two ring tokens while ownership treats it as one (IB-022).
pub fn node_key(url: &str) -> String {
    endpoint_of(url).to_ascii_lowercase()
}

/// `node_key` ordering without the allocation, for sorts and tie-breaks that must not disagree
/// with it.
pub fn cmp_endpoint(a: &str, b: &str) -> Ordering {
    let (a, b) = (endpoint_of(a).as_bytes(), endpoint_of(b).as_bytes());
    for (x, y) in a.iter().zip(b) {
        match x.to_ascii_lowercase().cmp(&y.to_ascii_lowercase()) {
            Ordering::Equal => {}
            unequal => return unequal,
        }
    }
    a.len().cmp(&b.len())
}

/// One path segment of a forwarded URL. Anything outside RFC 3986 unreserved is escaped, so a key
/// carrying `/`, `?` or `#` stays one segment instead of restructuring the request it is spliced into.
pub fn encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

// Windows can briefly hold a closed file open.
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

/// Durable on return. The staging file is `<name>.tmp`; readers that recover from it depend on
/// that spelling, and on it being fsynced before the rename.
pub fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{}.tmp", name));
    {
        let mut f = fs::File::create(&tmp)?;
        io::Write::write_all(&mut f, bytes)?;
        f.sync_all()?;
    }
    rename_with_retry(&tmp, &dir.join(name))?;

    // The rename is durable only once the directory entry is synced.
    // Windows refuses to open a directory as a file: best effort there.
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

// Same Windows quirk: an indexer holding the destination fails the replace transiently.
pub fn rename_with_retry(from: &Path, to: &Path) -> io::Result<()> {
    let mut last_err = None;
    for attempt in 0..DIR_REMOVE_ATTEMPTS {
        match fs::rename(from, to) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(20 * (attempt + 1) as u64));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to rename file")))
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

// serialize/deserialize are reached only via #[serde(with = "base64_bytes")]; dead-code sweeps flag them.
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

    const STANDARD: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const URL_SAFE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    /// The standard alphabet, for values that travel in a JSON body: replicated WAL frames. Not
    /// for anything handed back to be put in a URL -- see `base64_encode_url`.
    pub fn base64_encode(input: &[u8]) -> String {
        encode_with(input, STANDARD)
    }

    /// `+` in a query string is a space, and a cursor exists to be pasted into one (M20). Kept apart from
    /// `base64_encode`, which is on the wire between nodes and cannot change under a rolling upgrade.
    pub fn base64_encode_url(input: &[u8]) -> String {
        encode_with(input, URL_SAFE)
    }

    fn encode_with(input: &[u8], chars: &[u8]) -> String {
        let mut result = String::new();
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let combined = (b0 << 16) | (b1 << 8) | b2;
            result.push(chars[((combined >> 18) & 0x3F) as usize] as char);
            result.push(chars[((combined >> 12) & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                result.push(chars[((combined >> 6) & 0x3F) as usize] as char);
            } else {
                result.push('=');
            }
            if chunk.len() > 2 {
                result.push(chars[(combined & 0x3F) as usize] as char);
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
                // Both alphabets: a cursor issued before the URL-safe change still decodes, so
                // the one page of a scan in flight across an upgrade is not lost.
                '+' | '-' => 62,
                '/' | '_' => 63,
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

    #[test]
    fn two_nodes_sharing_a_path_tail_are_not_the_same_node() {
        assert_eq!(endpoint_of("http://a:1/x//shared"), "a:1");
        assert!(!same_endpoint("http://a:1/x//shared", "http://b:2/y//shared"),
            "identity is host:port, not the segment after the last //");
        assert!(same_endpoint("http://h:1/data", "http://h:1"),
            "a path names a route on a node, not a different node");
        assert_eq!(endpoint_of("//h:1"), "h:1");
        assert_eq!(endpoint_of("h:1?x=1#f"), "h:1");
    }

    #[test]
    fn a_path_segment_survives_the_characters_that_would_restructure_a_url() {
        assert_eq!(encode_path_segment("a?x=1"), "a%3Fx%3D1");
        assert_eq!(encode_path_segment("a#frag"), "a%23frag");
        assert_eq!(encode_path_segment("a/b"), "a%2Fb");
        assert_eq!(encode_path_segment("a%2Fb"), "a%252Fb");
        assert_eq!(encode_path_segment("plain-name.1_x~"), "plain-name.1_x~");
        assert_eq!(encode_path_segment("\u{e9}"), "%C3%A9");
    }

    #[test]
    fn a_well_formed_node_url_still_resolves_exactly_as_before() {
        // The ring hashes this, so any drift here silently reshuffles an existing cluster.
        for url in ["http://127.0.0.1:9501", "127.0.0.1:9501", "https://h", "http://h/"] {
            assert_eq!(endpoint_of(url), url.trim_end_matches('/').rsplit("//").next().unwrap());
        }
    }

    /// L16: `same_endpoint` was `==` on the authority, so one node spelled two ways counted twice.
    /// The consequence is `C8`'s -- a majority over an inflated voter set is not a majority.
    #[test]
    fn one_node_spelled_two_ways_is_one_node() {
        assert!(same_endpoint("http://LOCALHOST:8080", "localhost:8080"));
        assert!(same_endpoint("HTTP://Host.Example:9000/x", "host.example:9000"));
        assert_eq!(node_key("http://LOCALHOST:8080/x"), node_key("localhost:8080"));
        assert_eq!(node_key("HOST:1"), "host:1", "keys have to agree with the comparison");

        // What no string comparison settles, and the entry does not claim to.
        assert!(!same_endpoint("localhost:8080", "127.0.0.1:8080"));
        assert!(!same_endpoint("host:8080", "host:8081"), "the port is not case, it is identity");
    }

    /// IB-022: a sort or tie-break that disagrees with `node_key` puts one node in two places.
    #[test]
    fn canonical_ordering_agrees_with_the_canonical_key() {
        assert_eq!(cmp_endpoint("http://HOST:1", "host:1/x"), Ordering::Equal);
        assert_eq!(cmp_endpoint("http://Alpha:1", "beta:1"), Ordering::Less);
        assert_eq!(cmp_endpoint("host:10", "host:2"), Ordering::Less, "ordering is bytewise");

        let mut urls = ["http://Beta:1", "alpha:1", "http://BETA:1/x", "Gamma:1"];
        urls.sort_by(|a, b| cmp_endpoint(a, b));
        let mut keyed = urls;
        keyed.sort_by_key(|url| node_key(url));
        assert_eq!(urls, keyed);
    }

    /// M20: the standard alphabet's `+` is a space in a query string. Only the cursor encoding moved --
    /// replicated frames travel in a JSON body, where `+/` is fine.
    #[test]
    fn cursor_encoding_survives_a_query_string_and_still_reads_the_old_one() {
        use base64_bytes::{base64_decode, base64_encode, base64_encode_url};

        // Bytes chosen to hit index 62 and 63, which are `+/` in one alphabet and `-_` in the other.
        let bytes: Vec<u8> = vec![0xFB, 0xFF, 0xFE, 0xFF];
        let url = base64_encode_url(&bytes);
        assert!(!url.contains('+') && !url.contains('/'), "cursor alphabet leaked `+` or `/`: {}", url);
        assert_eq!(base64_decode(&url).unwrap(), bytes);

        let standard = base64_encode(&bytes);
        assert!(standard.contains('+') || standard.contains('/'),
            "the fixture must exercise the characters that differ: {}", standard);
        assert_eq!(base64_decode(&standard).unwrap(), bytes,
            "a cursor issued before the change is still one page of a scan in flight");
    }
}
