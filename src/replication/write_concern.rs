//! How many acknowledgements a write needs before the client hears success.

use serde::Deserialize;

#[derive(Clone, Copy)]
pub enum WriteConcern {
    Local,
    Majority,
    All,
    N(usize),
}

pub fn parse_write_concern(w: Option<&str>) -> WriteConcern {
    match w {
        None | Some("1") => WriteConcern::Local,
        Some("majority") => WriteConcern::Majority,
        Some("all") => WriteConcern::All,
        Some(s) => s.parse::<usize>().map(WriteConcern::N).unwrap_or(WriteConcern::Local),
    }
}

// Counts the primary itself, so a majority of a 3-node group is 2 acks total.
pub fn required_acks(wc: &WriteConcern, replica_count: usize) -> usize {
    let total = 1 + replica_count;
    match wc {
        WriteConcern::Local => 1,
        WriteConcern::Majority => total / 2 + 1,
        WriteConcern::All => total,
        WriteConcern::N(n) => (*n).max(1).min(total),
    }
}

#[derive(Deserialize)]
pub struct WriteConcernParams {
    pub w: Option<String>,
    pub wtimeout: Option<u64>,
}

pub const DEFAULT_WTIMEOUT_MS: u64 = 5000;

pub fn wc_query_string(p: &WriteConcernParams) -> String {
    let mut parts = Vec::new();
    if let Some(w) = &p.w {
        parts.push(format!("w={}", w));
    }
    if let Some(t) = p.wtimeout {
        parts.push(format!("wtimeout={}", t));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_concern_resolves_required_acks() {
        assert_eq!(required_acks(&parse_write_concern(None), 2), 1);
        assert_eq!(required_acks(&parse_write_concern(Some("1")), 2), 1);

        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 2), 2);
        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 1), 2);
        assert_eq!(required_acks(&parse_write_concern(Some("majority")), 4), 3);

        assert_eq!(required_acks(&parse_write_concern(Some("all")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern(Some("all")), 0), 1);

        assert_eq!(required_acks(&parse_write_concern(Some("3")), 2), 3);
        assert_eq!(required_acks(&parse_write_concern(Some("9")), 2), 3, "N is capped at total node count");
        assert_eq!(required_acks(&parse_write_concern(Some("0")), 2), 1, "N is floored at 1");

        assert_eq!(required_acks(&parse_write_concern(Some("garbage")), 2), 1, "unparseable w falls back to local");
    }

    #[test]
    fn wc_query_string_roundtrips() {
        let p = WriteConcernParams { w: Some("majority".into()), wtimeout: Some(2000) };
        assert_eq!(wc_query_string(&p), "?w=majority&wtimeout=2000");

        let p2 = WriteConcernParams { w: None, wtimeout: None };
        assert_eq!(wc_query_string(&p2), "");

        let p3 = WriteConcernParams { w: Some("all".into()), wtimeout: None };
        assert_eq!(wc_query_string(&p3), "?w=all");
    }
}
