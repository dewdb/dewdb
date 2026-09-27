//! `If-Match` and `If-None-Match` on single-document writes, evaluated per RFC 9110 §13.2.2.

use crate::model::err_json;
use crate::storage::Version;
use axum::http::{header, HeaderMap, HeaderName, StatusCode};
use axum::response::IntoResponse;
use axum::Json;

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntityTag {
    weak: bool,
    opaque: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tags {
    Any,
    List(Vec<EntityTag>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preconditions {
    if_match: Option<Tags>,
    if_none_match: Option<Tags>,
}

pub const CONDITIONAL_HEADERS: [HeaderName; 2] = [header::IF_MATCH, header::IF_NONE_MATCH];

impl Preconditions {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.if_match.is_none() && self.if_none_match.is_none()
    }

    /// An unparseable header is refused, never read as "no condition": that is a silent lost update.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, String> {
        Ok(Self {
            if_match: parse_header(headers, header::IF_MATCH)?,
            if_none_match: parse_header(headers, header::IF_NONE_MATCH)?,
        })
    }

    /// `current` is `None` when the key holds no document.
    pub fn holds(&self, current: Option<Version>) -> bool {
        let current = current.map(|v| v.etag());
        let current = current.as_deref().map(|tag| &tag[1..tag.len() - 1]);
        if let Some(tags) = &self.if_match {
            let matched = match (tags, current) {
                (_, None) => false,
                (Tags::Any, Some(_)) => true,
                (Tags::List(list), Some(cur)) => list.iter().any(|t| !t.weak && t.opaque == cur),
            };
            if !matched {
                return false;
            }
        }
        if let Some(tags) = &self.if_none_match {
            let matched = match (tags, current) {
                (_, None) => false,
                (Tags::Any, Some(_)) => true,
                (Tags::List(list), Some(cur)) => list.iter().any(|t| t.opaque == cur),
            };
            if matched {
                return false;
            }
        }
        true
    }
}

fn parse_header(headers: &HeaderMap, name: HeaderName) -> Result<Option<Tags>, String> {
    let mut values = headers.get_all(&name).iter().peekable();
    if values.peek().is_none() {
        return Ok(None);
    }
    let mut joined = String::new();
    for value in values {
        let text = value.to_str().map_err(|_| format!("{} is not visible ASCII", name))?;
        if !joined.is_empty() {
            joined.push(',');
        }
        joined.push_str(text);
    }
    parse_tags(&joined).map(Some).map_err(|e| format!("{}: {}", name, e))
}

fn parse_tags(text: &str) -> Result<Tags, String> {
    if text.trim() == "*" {
        return Ok(Tags::Any);
    }
    let bytes = text.as_bytes();
    let mut tags = Vec::new();
    let mut i = 0;
    loop {
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b',') {
            i += 1;
        }
        if i == bytes.len() {
            break;
        }
        let weak = bytes[i..].starts_with(b"W/");
        if weak {
            i += 2;
        }
        if bytes.get(i) != Some(&b'"') {
            return Err("expected a quoted entity tag or a lone *".to_string());
        }
        let start = i + 1;
        // etagc excludes DQUOTE, so a comma inside the quotes is part of the tag, not a separator.
        let end = bytes[start..].iter().position(|b| *b == b'"')
            .map(|p| start + p)
            .ok_or("unterminated entity tag")?;
        if !bytes[start..end].iter().all(|b| *b == 0x21 || (0x23..=0x7e).contains(b)) {
            return Err("entity tag holds a character outside etagc".to_string());
        }
        tags.push(EntityTag { weak, opaque: text[start..end].to_string() });
        i = end + 1;
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t') {
            i += 1;
        }
        if i < bytes.len() && bytes[i] != b',' {
            return Err("entity tags must be separated by commas".to_string());
        }
    }
    if tags.is_empty() {
        return Err("no entity tag given".to_string());
    }
    Ok(Tags::List(tags))
}

/// `etag` is what the document is now, `null` if it is absent, so the client can re-read or retry.
pub fn precondition_failed(current: Option<Version>) -> axum::response::Response {
    (StatusCode::PRECONDITION_FAILED, Json(serde_json::json!({
        "error": "precondition failed: the document is not at the version the request names",
        "etag": current.map(|v| v.etag()),
    }))).into_response()
}

pub fn parse_or_refuse(headers: &HeaderMap) -> Result<Preconditions, axum::response::Response> {
    Preconditions::from_headers(headers).map_err(|e| err_json(StatusCode::BAD_REQUEST, e))
}

pub fn with_etag(mut response: axum::response::Response, version: Version) -> axum::response::Response {
    if let Ok(value) = header::HeaderValue::from_str(&version.etag()) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pre(if_match: Option<&str>, if_none_match: Option<&str>) -> Result<Preconditions, String> {
        let mut headers = HeaderMap::new();
        if let Some(v) = if_match {
            headers.insert(header::IF_MATCH, v.parse().unwrap());
        }
        if let Some(v) = if_none_match {
            headers.insert(header::IF_NONE_MATCH, v.parse().unwrap());
        }
        Preconditions::from_headers(&headers)
    }

    const AT: Version = Version { term: 3, lsn: 17 };

    #[test]
    fn if_match_is_a_strong_comparison_against_a_document_that_exists() {
        assert!(pre(Some("\"3.17\""), None).unwrap().holds(Some(AT)));
        assert!(pre(Some("\"1.1\", \"3.17\""), None).unwrap().holds(Some(AT)));
        assert!(!pre(Some("\"3.18\""), None).unwrap().holds(Some(AT)));
        assert!(!pre(Some("W/\"3.17\""), None).unwrap().holds(Some(AT)),
            "a weak tag never satisfies If-Match");
        assert!(!pre(Some("\"3.17\""), None).unwrap().holds(None));
        assert!(pre(Some("*"), None).unwrap().holds(Some(AT)));
        assert!(!pre(Some("*"), None).unwrap().holds(None));
    }

    #[test]
    fn if_none_match_refuses_what_it_names_and_star_means_create_only() {
        assert!(pre(None, Some("*")).unwrap().holds(None));
        assert!(!pre(None, Some("*")).unwrap().holds(Some(AT)));
        assert!(!pre(None, Some("W/\"3.17\"")).unwrap().holds(Some(AT)),
            "If-None-Match compares weakly");
        assert!(pre(None, Some("\"3.18\"")).unwrap().holds(Some(AT)));
    }

    #[test]
    fn both_headers_must_hold() {
        assert!(!pre(Some("\"3.17\""), Some("\"3.17\"")).unwrap().holds(Some(AT)));
        assert!(pre(Some("\"3.17\""), Some("\"9.9\"")).unwrap().holds(Some(AT)));
    }

    #[test]
    fn a_malformed_header_is_refused_rather_than_read_as_no_condition() {
        for bad in ["3.17", "\"3.17", "\"a\" \"b\"", "", ",", "W/", "\"a\"b", "* , \"x\""] {
            assert!(pre(Some(bad), None).is_err(), "{:?} must not parse", bad);
        }
        assert_eq!(pre(Some("\"a,b\""), None).unwrap().if_match,
            Some(Tags::List(vec![EntityTag { weak: false, opaque: "a,b".into() }])));
    }

    #[test]
    fn repeated_header_lines_are_one_list() {
        let mut headers = HeaderMap::new();
        headers.append(header::IF_MATCH, "\"1.1\"".parse().unwrap());
        headers.append(header::IF_MATCH, "\"3.17\"".parse().unwrap());
        assert!(Preconditions::from_headers(&headers).unwrap().holds(Some(AT)));
    }
}
