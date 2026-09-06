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
    /// Empty means the admin surface shares `api_keys`, as it did before this tier existed.
    #[serde(default)]
    pub admin_keys: Vec<String>,
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
        for key in &self.admin_keys {
            if !valid_header_value(key) {
                return Err("auth.admin_keys entries must be non-empty printable ASCII".to_string());
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

    pub fn admin_locked(&self) -> bool {
        !self.admin_keys.is_empty()
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

/// Topology, collection destruction, and index definitions. Segments are counted before
/// percent-decoding, the way axum routes them, so an encoded slash cannot make a drop look like a
/// document delete.
fn is_admin_route(path: &str, method: &str) -> bool {
    if path == "/cluster" || path.starts_with("/cluster/") {
        return true;
    }
    if method == "DELETE"
        && path.strip_prefix("/collections/")
            .is_some_and(|name| !name.is_empty() && !name.contains('/'))
    {
        return true;
    }
    // Defining an index is a schema change and a replicated write, so it sits with the drop rather
    // than with the data path. Listing them does not, the way listing collections does not.
    if matches!(method, "POST" | "DELETE") && is_index_route(path) {
        return true;
    }
    // Every method, unlike indexes: a listing names the destinations this node pushes documents to.
    is_webhook_route(path)
}

/// `/collections/<name>/webhooks` and `/collections/<name>/webhooks/<id>`.
fn is_webhook_route(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("/collections/") else { return false };
    let mut parts = rest.split('/');
    parts.next().is_some_and(|name| !name.is_empty())
        && parts.next() == Some("webhooks")
        && match parts.next() {
            None => true,
            Some(id) => !id.is_empty() && parts.next().is_none(),
        }
}

/// `/collections/<name>/indexes` and `/collections/<name>/indexes/<index>`, and nothing else under
/// a collection.
fn is_index_route(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("/collections/") else { return false };
    let mut parts = rest.split('/');
    parts.next().is_some_and(|name| !name.is_empty())
        && parts.next() == Some("indexes")
        && match parts.next() {
            None => true,
            Some(index) => !index.is_empty() && parts.next().is_none(),
        }
}

pub fn authorize(
    path: &str,
    method: &str,
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
    if path == "/health" {
        return AuthOutcome::Allow;
    }

    let presented = presented_api_key(api_key_header, authorization);
    let accepted = |keys: &[String]| {
        presented.is_some_and(|got| keys.iter().any(|k| constant_time_eq(got, k)))
    };

    // Ahead of the public gate, not inside it: an operator can lock the admin surface alone.
    if cfg.admin_locked() && is_admin_route(path, method) {
        return match presented {
            None => AuthOutcome::Deny("missing admin key"),
            Some(_) if accepted(&cfg.admin_keys) => AuthOutcome::Allow,
            Some(_) => AuthOutcome::Deny("invalid admin key"),
        };
    }

    if !cfg.public_locked() {
        return AuthOutcome::Allow;
    }

    match presented {
        None => AuthOutcome::Deny("missing api key"),
        // An admin key opens the data path too: the tier is a superset, not a separate identity.
        Some(_) if accepted(&cfg.api_keys) || accepted(&cfg.admin_keys) => AuthOutcome::Allow,
        Some(_) => AuthOutcome::Deny("invalid api key"),
    }
}

/// Longer than the change feed's keep-alive, so a connection that has gone quiet is a dead one
/// rather than a feed with nothing to say.
const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(45);

/// `from` names this node on every internal request it makes: attribution in a peer's logs, and
/// the key a test fault injector cuts a link on.
fn node_headers(auth: &AuthConfig, from: &str) -> reqwest::header::HeaderMap {
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
    headers
}

pub fn build_client(auth: &AuthConfig, from: &str) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .default_headers(node_headers(auth, from))
        .build()
        .unwrap()
}

/// The same credentials with no whole-request deadline, for the change streams a router holds open
/// for as long as its own subscriber does. `STREAM_READ_TIMEOUT` is the liveness check in its place.
pub fn build_stream_client(auth: &AuthConfig, from: &str) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(STREAM_READ_TIMEOUT)
        .default_headers(node_headers(auth, from))
        .build()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;

    fn auth_cfg(secret: Option<&str>, keys: &[&str]) -> AuthConfig {
        with_admin(secret, keys, &[])
    }

    fn with_admin(secret: Option<&str>, keys: &[&str], admin: &[&str]) -> AuthConfig {
        AuthConfig {
            internal_secret: secret.map(|s| s.to_string()),
            api_keys: keys.iter().map(|k| k.to_string()).collect(),
            admin_keys: admin.iter().map(|k| k.to_string()).collect(),
            upstream_api_key: None,
        }
    }

    #[test]
    fn internal_routes_require_the_shared_secret_when_configured() {
        let cfg = auth_cfg(Some("s3cret"), &[]);

        assert_eq!(authorize("/internal/replicate", "GET", &cfg, Some("s3cret"), None, None), AuthOutcome::Allow);
        assert_eq!(authorize("/internal/replicate", "GET", &cfg, Some("wrong"), None, None),
            AuthOutcome::Deny("invalid internal secret"));
        assert_eq!(authorize("/internal/replicate", "GET", &cfg, None, None, None),
            AuthOutcome::Deny("missing internal secret header"));

        assert_eq!(authorize("/internal/vote", "GET", &cfg, Some("s3cretx"), None, None),
            AuthOutcome::Deny("invalid internal secret"), "a prefix of the secret must not pass");

        let open = auth_cfg(None, &[]);
        assert_eq!(authorize("/internal/replicate", "GET", &open, None, None, None), AuthOutcome::Allow,
            "an unset secret leaves internal routes open for backward compatibility");
    }

    #[test]
    fn public_routes_require_an_api_key_only_when_keys_are_configured() {
        let open = auth_cfg(None, &[]);
        assert_eq!(authorize("/collections/c/docs", "GET", &open, None, None, None), AuthOutcome::Allow);

        let locked = auth_cfg(None, &["alpha", "beta"]);
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, Some("alpha"), None), AuthOutcome::Allow);
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, Some("beta"), None), AuthOutcome::Allow,
            "any configured key is accepted, so keys can be rotated");
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, Some("gamma"), None),
            AuthOutcome::Deny("invalid api key"));
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, None, None),
            AuthOutcome::Deny("missing api key"));
    }

    #[test]
    fn bearer_tokens_are_accepted_as_api_keys() {
        let locked = auth_cfg(None, &["alpha"]);
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, None, Some("Bearer alpha")), AuthOutcome::Allow);
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, None, Some("Bearer nope")),
            AuthOutcome::Deny("invalid api key"));
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, None, Some("Basic alpha")),
            AuthOutcome::Deny("missing api key"), "only the Bearer scheme is read as an api key");
        assert_eq!(authorize("/collections/c/docs", "GET", &locked, None, Some("alpha"), Some("Bearer nope")),
            AuthOutcome::Allow, "the explicit header wins over Authorization");
    }

    #[test]
    fn health_stays_reachable_but_metrics_does_not() {
        let locked = auth_cfg(Some("s"), &["alpha"]);
        assert_eq!(authorize("/health", "GET", &locked, None, None, None), AuthOutcome::Allow,
            "probes must reach /health without credentials");
        assert_eq!(authorize("/metrics", "GET", &locked, None, None, None),
            AuthOutcome::Deny("missing api key"), "metrics can expose topology, so it stays behind the key");
        assert_eq!(authorize("/metrics", "GET", &locked, None, Some("alpha"), None), AuthOutcome::Allow);
    }

    #[test]
    fn internal_secret_does_not_satisfy_the_public_api() {
        let cfg = auth_cfg(Some("s3cret"), &["alpha"]);
        assert_eq!(authorize("/collections/c/docs", "GET", &cfg, Some("s3cret"), None, None),
            AuthOutcome::Deny("missing api key"), "the internal secret must not unlock the public API");
        assert_eq!(authorize("/internal/replicate", "GET", &cfg, None, Some("alpha"), None),
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

        assert!(with_admin(None, &["ok"], &[""]).validate().is_err());

        let parsed: NodeConfig = serde_json::from_str(
            r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1"}"#).unwrap();
        assert!(parsed.auth.internal_secret.is_none());
        assert!(!parsed.auth.public_locked(), "auth is off by default");
        assert!(!parsed.auth.admin_locked(), "the admin tier is off by default");
    }

    /// L3: `/cluster/*` and a collection drop were guarded by `api_keys`, so any client key that
    /// could write a document could also repartition the cluster.
    #[test]
    fn admin_routes_require_an_admin_key_when_one_is_configured() {
        let cfg = with_admin(None, &["client"], &["root"]);

        for path in ["/cluster", "/cluster/ring", "/cluster/members", "/cluster/configuration",
                     "/cluster/migrate", "/cluster/rebalance"] {
            assert_eq!(authorize(path, "POST", &cfg, None, Some("root"), None), AuthOutcome::Allow, "{}", path);
            assert_eq!(authorize(path, "POST", &cfg, None, Some("client"), None),
                AuthOutcome::Deny("invalid admin key"), "{}", path);
            assert_eq!(authorize(path, "GET", &cfg, None, Some("client"), None),
                AuthOutcome::Deny("invalid admin key"),
                "reading the topology is admin too: {}", path);
            assert_eq!(authorize(path, "POST", &cfg, None, None, None),
                AuthOutcome::Deny("missing admin key"), "{}", path);
        }

        assert_eq!(authorize("/collections/c", "DELETE", &cfg, None, Some("client"), None),
            AuthOutcome::Deny("invalid admin key"), "dropping a collection is destructive");
        assert_eq!(authorize("/collections/c", "DELETE", &cfg, None, Some("root"), None), AuthOutcome::Allow);
    }

    /// Defining an index is a schema change and a replicated write; reading the definitions is not.
    #[test]
    fn index_definitions_are_admin_and_listing_them_is_not() {
        let cfg = with_admin(None, &["client"], &["root"]);

        for (path, method) in [("/collections/c/indexes", "POST"),
                               ("/collections/c/indexes/by_age", "DELETE")] {
            assert_eq!(authorize(path, method, &cfg, None, Some("root"), None), AuthOutcome::Allow,
                "{} {}", method, path);
            assert_eq!(authorize(path, method, &cfg, None, Some("client"), None),
                AuthOutcome::Deny("invalid admin key"), "{} {}", method, path);
        }

        assert_eq!(authorize("/collections/c/indexes", "GET", &cfg, None, Some("client"), None),
            AuthOutcome::Allow, "listing them is data path, like listing collections");

        // Not the index routes, whatever they contain: only these two shapes are.
        for path in ["/collections/c/docs/indexes", "/collections/c/indexes/a/b", "/collections//indexes"] {
            assert_eq!(authorize(path, "POST", &cfg, None, Some("client"), None), AuthOutcome::Allow,
                "{}", path);
        }
    }

    #[test]
    fn the_admin_tier_covers_only_topology_and_the_collection_drop() {
        let cfg = with_admin(None, &["client"], &["root"]);

        // Same verb one segment deeper, and the same paths under other verbs: all data path.
        for (path, method) in [("/collections/c/docs/1", "DELETE"), ("/collections/c/docs", "POST"),
                               ("/collections", "GET"), ("/collections/c/compact", "POST"),
                               ("/collections/c/snapshot", "POST"), ("/metrics", "GET")] {
            assert_eq!(authorize(path, method, &cfg, None, Some("client"), None), AuthOutcome::Allow,
                "{} {}", method, path);
        }

        assert_eq!(authorize("/collections/a%2Fb", "DELETE", &cfg, None, Some("client"), None),
            AuthOutcome::Deny("invalid admin key"),
            "an encoded slash is one routed segment, so this is still the drop route");
    }

    #[test]
    fn an_admin_key_also_opens_the_data_path() {
        let cfg = with_admin(None, &["client"], &["root"]);
        assert_eq!(authorize("/collections/c/docs", "POST", &cfg, None, Some("root"), None),
            AuthOutcome::Allow, "an operator must not need a second credential to read what it drops");
    }

    #[test]
    fn an_unset_admin_tier_leaves_the_public_key_governing_admin_routes() {
        let locked = auth_cfg(None, &["alpha"]);
        assert_eq!(authorize("/cluster/ring", "POST", &locked, None, Some("alpha"), None), AuthOutcome::Allow);
        assert_eq!(authorize("/collections/c", "DELETE", &locked, None, Some("alpha"), None), AuthOutcome::Allow);

        let open = auth_cfg(None, &[]);
        assert_eq!(authorize("/cluster/ring", "POST", &open, None, None, None), AuthOutcome::Allow,
            "an operator who configured no keys at all keeps the pre-tier behaviour");
    }

    #[test]
    fn admin_keys_alone_lock_the_admin_surface_and_nothing_else() {
        let cfg = with_admin(None, &[], &["root"]);
        assert_eq!(authorize("/collections/c/docs", "POST", &cfg, None, None, None), AuthOutcome::Allow,
            "an empty api_keys still means an open public API");
        assert_eq!(authorize("/cluster/ring", "POST", &cfg, None, None, None),
            AuthOutcome::Deny("missing admin key"));
    }
}
