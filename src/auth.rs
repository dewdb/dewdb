//! Credential config and the authorization decision for a request path.

use serde::Deserialize;
use std::time::Duration;

pub const INTERNAL_SECRET_HEADER: &str = "x-dew-internal-secret";
pub const API_KEY_HEADER: &str = "x-api-key";
pub const NODE_HEADER: &str = "x-dew-node";

#[derive(Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default)]
    pub internal_secret: Option<String>,
    #[serde(default)]
    pub api_keys: Vec<String>,
    #[serde(default)]
    pub upstream_api_key: Option<String>,
}

fn valid_header_value(v: &str) -> bool {
    !v.is_empty() && v.bytes().all(|b| (0x20..=0x7e).contains(&b))
}

impl AuthConfig {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(secret) = &self.internal_secret {
            if !valid_header_value(secret) {
                return Err("auth.internal_secret must be non-empty printable ASCII".to_string());
            }
        }
        for key in &self.api_keys {
            if !valid_header_value(key) {
                return Err("auth.api_keys entries must be non-empty printable ASCII".to_string());
            }
        }
        if let Some(key) = &self.upstream_api_key {
            if !valid_header_value(key) {
                return Err("auth.upstream_api_key must be non-empty printable ASCII".to_string());
            }
        }
        Ok(())
    }

    pub fn public_locked(&self) -> bool {
        !self.api_keys.is_empty()
    }
}

// Constant time: a byte-by-byte comparison leaks the secret's prefix.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[derive(Debug, PartialEq)]
pub enum AuthOutcome {
    Allow,
    Deny(&'static str),
}

fn presented_api_key<'a>(api_key_header: Option<&'a str>, authorization: Option<&'a str>) -> Option<&'a str> {
    if let Some(k) = api_key_header {
        return Some(k);
    }
    authorization.and_then(|a| a.strip_prefix("Bearer ")).map(|k| k.trim())
}

pub fn authorize(
    path: &str,
    cfg: &AuthConfig,
    secret_header: Option<&str>,
    api_key_header: Option<&str>,
    authorization: Option<&str>,
) -> AuthOutcome {
    if path.starts_with("/internal/") {
        return match &cfg.internal_secret {
            None => AuthOutcome::Allow,
            Some(expected) => match secret_header {
                Some(got) if constant_time_eq(got, expected) => AuthOutcome::Allow,
                Some(_) => AuthOutcome::Deny("invalid internal secret"),
                None => AuthOutcome::Deny("missing internal secret header"),
            },
        };
    }

    // /health stays open for probes; /metrics exposes topology.
    if path == "/health" || !cfg.public_locked() {
        return AuthOutcome::Allow;
    }

    match presented_api_key(api_key_header, authorization) {
        None => AuthOutcome::Deny("missing api key"),
        Some(got) => {
            if cfg.api_keys.iter().any(|k| constant_time_eq(got, k)) {
                AuthOutcome::Allow
            } else {
                AuthOutcome::Deny("invalid api key")
            }
        }
    }
}

/// `from` names this node on every internal request it makes: attribution in a peer's logs, and
/// the key a test fault injector cuts a link on.
pub fn build_client(auth: &AuthConfig, from: &str) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();

    if let Ok(v) = reqwest::header::HeaderValue::from_str(from) {
        headers.insert(NODE_HEADER, v);
    }

    if let Some(secret) = &auth.internal_secret {
        if let Ok(v) = reqwest::header::HeaderValue::from_str(secret) {
            headers.insert(INTERNAL_SECRET_HEADER, v);
        }
    }
    if let Some(key) = &auth.upstream_api_key {
        if let Ok(v) = reqwest::header::HeaderValue::from_str(key) {
            headers.insert(API_KEY_HEADER, v);
        }
    }

    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .default_headers(headers)
        .build()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;

    fn auth_cfg(secret: Option<&str>, keys: &[&str]) -> AuthConfig {
        AuthConfig {
            internal_secret: secret.map(|s| s.to_string()),
            api_keys: keys.iter().map(|k| k.to_string()).collect(),
            upstream_api_key: None,
        }
    }

    #[test]
    fn internal_routes_require_the_shared_secret_when_configured() {
        let cfg = auth_cfg(Some("s3cret"), &[]);

        assert_eq!(authorize("/internal/replicate", &cfg, Some("s3cret"), None, None), AuthOutcome::Allow);
        assert_eq!(authorize("/internal/replicate", &cfg, Some("wrong"), None, None),
            AuthOutcome::Deny("invalid internal secret"));
        assert_eq!(authorize("/internal/replicate", &cfg, None, None, None),
            AuthOutcome::Deny("missing internal secret header"));

        assert_eq!(authorize("/internal/vote", &cfg, Some("s3cretx"), None, None),
            AuthOutcome::Deny("invalid internal secret"), "a prefix of the secret must not pass");

        let open = auth_cfg(None, &[]);
        assert_eq!(authorize("/internal/replicate", &open, None, None, None), AuthOutcome::Allow,
            "an unset secret leaves internal routes open for backward compatibility");
    }

    #[test]
    fn public_routes_require_an_api_key_only_when_keys_are_configured() {
        let open = auth_cfg(None, &[]);
        assert_eq!(authorize("/collections/c/docs", &open, None, None, None), AuthOutcome::Allow);

        let locked = auth_cfg(None, &["alpha", "beta"]);
        assert_eq!(authorize("/collections/c/docs", &locked, None, Some("alpha"), None), AuthOutcome::Allow);
        assert_eq!(authorize("/collections/c/docs", &locked, None, Some("beta"), None), AuthOutcome::Allow,
            "any configured key is accepted, so keys can be rotated");
        assert_eq!(authorize("/collections/c/docs", &locked, None, Some("gamma"), None),
            AuthOutcome::Deny("invalid api key"));
        assert_eq!(authorize("/collections/c/docs", &locked, None, None, None),
            AuthOutcome::Deny("missing api key"));
    }

    #[test]
    fn bearer_tokens_are_accepted_as_api_keys() {
        let locked = auth_cfg(None, &["alpha"]);
        assert_eq!(authorize("/collections/c/docs", &locked, None, None, Some("Bearer alpha")), AuthOutcome::Allow);
        assert_eq!(authorize("/collections/c/docs", &locked, None, None, Some("Bearer nope")),
            AuthOutcome::Deny("invalid api key"));
        assert_eq!(authorize("/collections/c/docs", &locked, None, None, Some("Basic alpha")),
            AuthOutcome::Deny("missing api key"), "only the Bearer scheme is read as an api key");
        assert_eq!(authorize("/collections/c/docs", &locked, None, Some("alpha"), Some("Bearer nope")),
            AuthOutcome::Allow, "the explicit header wins over Authorization");
    }

    #[test]
    fn health_stays_reachable_but_metrics_does_not() {
        let locked = auth_cfg(Some("s"), &["alpha"]);
        assert_eq!(authorize("/health", &locked, None, None, None), AuthOutcome::Allow,
            "probes must reach /health without credentials");
        assert_eq!(authorize("/metrics", &locked, None, None, None),
            AuthOutcome::Deny("missing api key"), "metrics can expose topology, so it stays behind the key");
        assert_eq!(authorize("/metrics", &locked, None, Some("alpha"), None), AuthOutcome::Allow);
    }

    #[test]
    fn internal_secret_does_not_satisfy_the_public_api() {
        let cfg = auth_cfg(Some("s3cret"), &["alpha"]);
        assert_eq!(authorize("/collections/c/docs", &cfg, Some("s3cret"), None, None),
            AuthOutcome::Deny("missing api key"), "the internal secret must not unlock the public API");
        assert_eq!(authorize("/internal/replicate", &cfg, None, Some("alpha"), None),
            AuthOutcome::Deny("missing internal secret header"), "an api key must not unlock internal routes");
    }

    #[test]
    fn constant_time_compare_matches_only_identical_strings() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "ab"));
        assert!(!constant_time_eq("", "a"));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn auth_config_rejects_unusable_credentials() {
        assert!(auth_cfg(Some("fine"), &["ok"]).validate().is_ok());
        assert!(auth_cfg(Some(""), &[]).validate().is_err(), "an empty secret would silently disable checks");
        assert!(auth_cfg(None, &[""]).validate().is_err());
        assert!(auth_cfg(Some("bad\nvalue"), &[]).validate().is_err(), "a newline cannot go in a header");
        assert!(auth_cfg(Some("caf\u{e9}"), &[]).validate().is_err(), "non-ASCII cannot go in a header");

        let parsed: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1"}"#).unwrap();
        assert!(parsed.auth.internal_secret.is_none());
        assert!(!parsed.auth.public_locked(), "auth is off by default");
    }
}
