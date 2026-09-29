//! In-process TLS: the listener's certificate, its hot reload, and the trust peers are dialed with.

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use rustls::crypto::CryptoProvider;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use std::io;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

const RELOAD_INTERVAL: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const EXPIRY_WARNING_REPEAT: Duration = Duration::from_secs(24 * 3600);

#[derive(Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub cert_file: Option<String>,
    #[serde(default)]
    pub key_file: Option<String>,
    /// Replaces the platform roots for dialing peers, so only this CA can vouch for one.
    #[serde(default)]
    pub ca_file: Option<String>,
    #[serde(default = "default_expiry_warning_days")]
    pub expiry_warning_days: u64,
}

fn default_expiry_warning_days() -> u64 { 21 }

impl TlsConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, path) in [("cert_file", &self.cert_file), ("key_file", &self.key_file), ("ca_file", &self.ca_file)] {
            if path.as_deref().is_some_and(|p| p.trim().is_empty()) {
                return Err(format!("tls.{} must not be empty", name));
            }
        }
        if self.cert_file.is_some() != self.key_file.is_some() {
            return Err("tls.cert_file and tls.key_file must be set together".to_string());
        }
        Ok(())
    }

    pub fn serves(&self) -> bool {
        self.cert_file.is_some()
    }
}

/// `http://` URLs among `urls`; a TLS node refuses these in its config, unlike cluster-view ones it upgrades.
pub fn plaintext_urls<'a>(urls: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    urls.into_iter()
        .filter(|u| u.get(..7).is_some_and(|s| s.eq_ignore_ascii_case("http://")))
        .collect()
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

struct Loaded {
    key: Arc<CertifiedKey>,
    expires_at: Option<i64>,
}

fn load_pair(cert_file: &str, key_file: &str) -> Result<Loaded, String> {
    let chain = CertificateDer::pem_file_iter(cert_file)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("tls.cert_file {}: {}", cert_file, e))?;
    if chain.is_empty() {
        return Err(format!("tls.cert_file {} holds no certificate", cert_file));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| format!("tls.key_file {}: {}", key_file, e))?;
    let signer = provider().key_provider.load_private_key(key)
        .map_err(|e| format!("tls.key_file {}: {}", key_file, e))?;
    let key = CertifiedKey::new(chain, signer);
    match key.keys_match() {
        Ok(()) | Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::Unknown)) => {},
        Err(e) => return Err(format!("tls.key_file {} does not belong to tls.cert_file {}: {}", key_file, cert_file, e)),
    }
    let expires_at = not_after(key.cert[0].as_ref());
    Ok(Loaded { key: Arc::new(key), expires_at })
}

#[derive(Debug)]
struct Resolver {
    current: RwLock<Arc<CertifiedKey>>,
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current.read().unwrap().clone())
    }
}

/// The listener's certificate. Read per handshake, so a reload applies to the next connection.
pub struct ServerTls {
    resolver: Arc<Resolver>,
    config: Arc<rustls::ServerConfig>,
    cert_file: String,
    key_file: String,
    expires_at: RwLock<Option<i64>>,
    warning_days: u64,
}

impl ServerTls {
    pub fn load(cfg: &TlsConfig) -> Result<Option<Arc<Self>>, String> {
        let (Some(cert_file), Some(key_file)) = (&cfg.cert_file, &cfg.key_file) else { return Ok(None) };
        let loaded = load_pair(cert_file, key_file)?;
        let resolver = Arc::new(Resolver { current: RwLock::new(loaded.key) });
        let mut config = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
            .with_cert_resolver(resolver.clone());
        // HTTP/1.1 only, as over plaintext: the WebSocket route upgrades an HTTP/1.1 connection.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Some(Arc::new(Self {
            resolver,
            config: Arc::new(config),
            cert_file: cert_file.clone(),
            key_file: key_file.clone(),
            expires_at: RwLock::new(loaded.expires_at),
            warning_days: cfg.expiry_warning_days,
        })))
    }

    fn reload(&self) -> Result<(), String> {
        let loaded = load_pair(&self.cert_file, &self.key_file)?;
        *self.resolver.current.write().unwrap() = loaded.key;
        *self.expires_at.write().unwrap() = loaded.expires_at;
        Ok(())
    }

    fn stamp(&self) -> Option<(SystemTime, SystemTime)> {
        let modified = |p: &str| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        Some((modified(&self.cert_file)?, modified(&self.key_file)?))
    }

    /// Seconds until the served certificate expires; negative once it has.
    pub fn expires_in(&self) -> Option<i64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
        self.expires_at.read().unwrap().map(|at| at - now)
    }

    fn check_expiry(&self) -> bool {
        let Some(left) = self.expires_in() else { return false };
        if left <= 0 {
            error!(target: "tls", cert_file = %self.cert_file,
                "The served certificate has expired; peers and clients verifying it refuse this node");
            true
        } else if left < (self.warning_days * 86_400) as i64 {
            warn!(target: "tls", cert_file = %self.cert_file, days_left = left / 86_400,
                "The served certificate expires soon; replace the file and the node picks it up");
            true
        } else {
            false
        }
    }
}

/// Swaps in a replaced cert and key without a restart, and warns ahead of expiry.
pub fn reload_task(tls: Arc<ServerTls>) {
    tokio::spawn(async move {
        let mut loaded = tls.stamp();
        let mut refused = None;
        let mut warned_at: Option<std::time::Instant> = None;
        loop {
            if warned_at.is_none_or(|t| t.elapsed() >= EXPIRY_WARNING_REPEAT) && tls.check_expiry() {
                warned_at = Some(std::time::Instant::now());
            }
            tokio::time::sleep(RELOAD_INTERVAL).await;
            let stamp = tls.stamp();
            if stamp.is_none() || stamp == loaded {
                continue;
            }
            // A cert written before its key fails once and is retried until the pair matches.
            match tls.reload() {
                Ok(()) => {
                    loaded = stamp;
                    refused = None;
                    warned_at = None;
                    info!(target: "tls", cert_file = %tls.cert_file,
                        expires_in_days = tls.expires_in().map(|s| s / 86_400), "Reloaded the served certificate");
                },
                Err(e) if refused != stamp => {
                    refused = stamp;
                    warn!(target: "tls", error = %e, "Could not reload the certificate; still serving the previous one");
                },
                Err(_) => {},
            }
        }
    });
}

/// Serves `app` over TLS when `tls` is set, and as `axum::serve` does otherwise.
pub async fn serve(listener: TcpListener, app: Router, tls: Option<Arc<ServerTls>>) -> io::Result<()> {
    let Some(tls) = tls else { return axum::serve(listener, app).await };
    let acceptor = TlsAcceptor::from(tls.config.clone());
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                // Out of descriptors is not fatal to the listener; axum::serve backs off the same way.
                debug!(target: "tls", error = %e, "accept failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            },
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => return debug!(target: "tls", peer = %peer, error = %e, "TLS handshake failed"),
                Err(_) => return debug!(target: "tls", peer = %peer, "TLS handshake timed out"),
            };
            let service = TowerToHyperService::new(app);
            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), service).await
            {
                debug!(target: "tls", peer = %peer, error = %e, "connection ended with an error");
            }
        });
    }
}

/// What this node's outbound clients trust and whether they may dial plaintext. The roots re-read
/// with `tls.ca_file`; `https_only` follows `tls.cert_file` and is fixed at boot.
#[derive(Clone, Default)]
pub struct PeerTrust {
    roots: Vec<reqwest::Certificate>,
    https_only: bool,
}

impl PeerTrust {
    pub fn load(cfg: &TlsConfig) -> Result<Self, String> {
        let roots = match &cfg.ca_file {
            None => Vec::new(),
            Some(path) => {
                let pem = std::fs::read(path).map_err(|e| format!("tls.ca_file {}: {}", path, e))?;
                let roots = reqwest::Certificate::from_pem_bundle(&pem)
                    .map_err(|e| format!("tls.ca_file {}: {}", path, e))?;
                if roots.is_empty() {
                    return Err(format!("tls.ca_file {} holds no certificate", path));
                }
                roots
            },
        };
        Ok(Self { roots, https_only: cfg.serves() })
    }

    pub fn apply(&self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        let mut builder = builder.https_only(self.https_only);
        if !self.roots.is_empty() {
            builder = builder.use_rustls_tls().tls_built_in_root_certs(false);
            for root in &self.roots {
                builder = builder.add_root_certificate(root.clone());
            }
        }
        builder
    }
}

#[derive(Clone)]
struct Dialer {
    inner: reqwest::Client,
    upgrade: bool,
}

/// Every peer call goes through this. Clones share one slot, so `replace` reaches every holder (IB-042).
/// A serving node dials `https://` whatever the URL records, since an old view says `http://` (IB-062).
#[derive(Clone)]
pub struct PeerClient {
    current: Arc<RwLock<Dialer>>,
}

impl From<reqwest::Client> for PeerClient {
    fn from(inner: reqwest::Client) -> Self {
        Self::with(Dialer { inner, upgrade: false })
    }
}

impl PeerClient {
    pub fn new(inner: reqwest::Client, trust: &PeerTrust) -> Self {
        Self::with(Dialer { inner, upgrade: trust.https_only })
    }

    fn with(dialer: Dialer) -> Self {
        Self { current: Arc::new(RwLock::new(dialer)) }
    }

    /// Requests already sent finish on the client they started on; the next one takes `next`'s.
    pub fn replace(&self, next: PeerClient) {
        let dialer = next.dialer();
        *self.current.write().unwrap() = dialer;
    }

    fn dialer(&self) -> Dialer {
        self.current.read().unwrap().clone()
    }

    pub fn dial_url<'a>(&self, url: &'a str) -> std::borrow::Cow<'a, str> {
        upgraded(self.dialer().upgrade, url)
    }

    pub fn request(&self, method: reqwest::Method, url: impl AsRef<str>) -> reqwest::RequestBuilder {
        let dialer = self.dialer();
        dialer.inner.request(method, upgraded(dialer.upgrade, url.as_ref()).as_ref())
    }

    pub fn get(&self, url: impl AsRef<str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, url)
    }

    pub fn post(&self, url: impl AsRef<str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, url)
    }

    pub fn put(&self, url: impl AsRef<str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::PUT, url)
    }

    pub fn patch(&self, url: impl AsRef<str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::PATCH, url)
    }

    pub fn delete(&self, url: impl AsRef<str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::DELETE, url)
    }
}

fn upgraded(upgrade: bool, url: &str) -> std::borrow::Cow<'_, str> {
    match url.get(..7) {
        Some(scheme) if upgrade && scheme.eq_ignore_ascii_case("http://") =>
            format!("https://{}", &url[7..]).into(),
        _ => url.into(),
    }
}

fn der_element(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (first as usize, rest)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        (rest[..n].iter().fold(0usize, |len, &b| (len << 8) | b as usize), &rest[n..])
    };
    (rest.len() >= len).then(|| (tag, &rest[..len], &rest[len..]))
}

/// Unix seconds of a DER certificate's notAfter.
fn not_after(cert: &[u8]) -> Option<i64> {
    let (_, cert, _) = der_element(cert)?;
    let (_, mut tbs, _) = der_element(cert)?;
    // RFC 5280: TBSCertificate is [0] version (optional), serial, signature, issuer, validity.
    if tbs.first() == Some(&0xa0) {
        tbs = der_element(tbs)?.2;
    }
    for _ in 0..3 {
        tbs = der_element(tbs)?.2;
    }
    let (_, validity, _) = der_element(tbs)?;
    let (tag, time, _) = der_element(der_element(validity)?.2)?;
    parse_time(tag, time)
}

fn parse_time(tag: u8, raw: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(raw).ok()?.strip_suffix('Z')?;
    // UTCTime years 50-99 are 19xx (RFC 5280 4.1.2.5.1).
    let (year, rest) = match (tag, s.len()) {
        (0x17, 12) => (s[..2].parse::<i64>().ok().map(|y| if y >= 50 { 1900 + y } else { 2000 + y })?, &s[2..]),
        (0x18, 14) => (s[..4].parse::<i64>().ok()?, &s[4..]),
        _ => return None,
    };
    let field = |i: usize| rest.get(i..i + 2)?.parse::<i64>().ok();
    let (month, day, hour, minute, second) = (field(0)?, field(2)?, field(4)?, field(6)?, field(8)?);
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

// Hinnant's days_from_civil: days since 1970-01-01 in the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
pub mod test_certs {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub struct Ca {
        cert: rcgen::Certificate,
        key: KeyPair,
    }

    impl Ca {
        pub fn new() -> Self {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let key = KeyPair::generate().unwrap();
            Self { cert: params.self_signed(&key).unwrap(), key }
        }

        pub fn write(&self, dir: &Path, name: &str) -> PathBuf {
            let path = dir.join(name);
            std::fs::write(&path, self.cert.pem()).unwrap();
            path
        }

        /// A leaf for `names`, written as `<stem>.pem` and `<stem>.key`.
        pub fn issue(&self, dir: &Path, stem: &str, names: &[&str], not_after: Option<SystemTime>) -> (PathBuf, PathBuf) {
            let mut params = CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
            if let Some(at) = not_after {
                params.not_after = rcgen::date_time_ymd(1970, 1, 1) + at.duration_since(UNIX_EPOCH).unwrap();
            }
            let key = KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
            let (cert_path, key_path) = (dir.join(format!("{}.pem", stem)), dir.join(format!("{}.key", stem)));
            std::fs::write(&cert_path, cert.pem()).unwrap();
            std::fs::write(&key_path, key.serialize_pem()).unwrap();
            (cert_path, key_path)
        }
    }

    pub fn tls_json(cert: &Path, key: &Path, ca: &Path) -> serde_json::Value {
        serde_json::json!({
            "cert_file": cert.to_string_lossy(),
            "key_file": key.to_string_lossy(),
            "ca_file": ca.to_string_lossy(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::test_certs::{tls_json, Ca};
    use super::*;
    use crate::test_support::{next_test_port, put_doc_http, temp_root, wait_for_doc, TestNode};
    use axum::routing::get;

    fn tls_cfg(cert: &std::path::Path, key: &std::path::Path, ca: &std::path::Path) -> TlsConfig {
        serde_json::from_value(tls_json(cert, key, ca)).unwrap()
    }

    async fn serve_hello(tls: Arc<ServerTls>) -> String {
        let port = next_test_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let app = Router::new().route("/hello", get(|| async { "hi" }));
        tokio::spawn(serve(listener, app, Some(tls)));
        format!("127.0.0.1:{}", port)
    }

    fn client(trust: &PeerTrust) -> reqwest::Client {
        trust.apply(reqwest::Client::builder().timeout(Duration::from_secs(5))).build().unwrap()
    }

    async fn presented_serial(addr: &str, ca: &std::path::Path) -> Vec<u8> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(ca).unwrap() {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = rustls::ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions().unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let name = rustls_pki_types::ServerName::try_from("127.0.0.1").unwrap();
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config)).connect(name, tcp).await.unwrap();
        stream.get_ref().1.peer_certificates().unwrap()[0].as_ref().to_vec()
    }

    #[test]
    fn cert_and_key_are_set_together_and_ca_alone_is_client_only() {
        let parse = |json: &str| serde_json::from_str::<TlsConfig>(json).unwrap().validate();
        assert!(parse(r#"{"cert_file":"a.pem"}"#).is_err());
        assert!(parse(r#"{"key_file":"a.key"}"#).is_err());
        assert!(parse(r#"{"cert_file":" ","key_file":"a.key"}"#).is_err());
        assert!(parse(r#"{"cert_file":"a.pem","key_file":"a.key"}"#).is_ok());
        let client_only: TlsConfig = serde_json::from_str(r#"{"ca_file":"ca.pem"}"#).unwrap();
        assert!(client_only.validate().is_ok() && !client_only.serves());
        assert!(serde_json::from_str::<TlsConfig>(r#"{"cert":"a.pem"}"#).is_err(), "a typo must not boot plaintext");
    }

    #[test]
    fn a_node_serving_tls_refuses_plaintext_peer_urls_at_boot() {
        let parse = |json: &str| serde_json::from_str::<crate::config::NodeConfig>(json).unwrap();
        let tls = r#""tls":{"cert_file":"n.pem","key_file":"n.key"}"#;
        let bad = parse(&format!(r#"{{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",{},
            "peers":["https://10.0.0.2:1","HTTP://10.0.0.3:1"]}}"#, tls));
        let err = bad.validate().unwrap_err();
        assert!(err.contains("HTTP://10.0.0.3:1") && !err.contains("10.0.0.2"), "{}", err);

        let router = parse(&format!(r#"{{"node_id":"r","role":"router","listen_addr":"127.0.0.1:1",{},
            "shard_map":[{{"start_hash":0,"end_hash":0,"node_url":"https://a:1","replica_urls":["http://b:1"]}}]}}"#, tls));
        assert!(router.validate().unwrap_err().contains("http://b:1"));

        let good = parse(&format!(r#"{{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1",{},
            "peers":["https://10.0.0.2:1"],"primary_addr":"https://10.0.0.2:1"}}"#, tls));
        good.validate().unwrap();
        assert_eq!(good.own_url(), "https://127.0.0.1:1");
        let seeded = crate::cluster::metadata::ClusterMetadata::seed_from_config(&good);
        assert!(plaintext_urls(seeded.members.iter().map(|m| m.url.as_str())).is_empty(),
            "the seeded view must record this node as it is dialed: {:?}", seeded.members);

        let plain = parse(r#"{"node_id":"n","role":"shard","listen_addr":"127.0.0.1:1","peers":["http://10.0.0.2:1"]}"#);
        plain.validate().unwrap();
        assert_eq!(plain.own_url(), "http://127.0.0.1:1");
    }

    #[test]
    fn not_after_reads_both_asn1_time_forms() {
        let dir = temp_root();
        let ca = Ca::new();
        // rcgen writes UTCTime before 2050 and GeneralizedTime from then on, as RFC 5280 requires.
        for unix in [1_936_742_400i64, 2_876_774_400] {
            let at = UNIX_EPOCH + Duration::from_secs(unix as u64);
            let (cert, _) = ca.issue(&dir, "leaf", &["127.0.0.1"], Some(at));
            let der = CertificateDer::from_pem_file(&cert).unwrap();
            assert_eq!(not_after(der.as_ref()), Some(unix));
        }
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(not_after(b"\x30\x03\x02\x01"), None, "a truncated certificate is no date, not a panic");
    }

    #[test]
    fn a_key_that_is_not_the_certificates_is_refused() {
        let dir = temp_root();
        let ca = Ca::new();
        let (cert, _) = ca.issue(&dir, "a", &["127.0.0.1"], None);
        let (_, other_key) = ca.issue(&dir, "b", &["127.0.0.1"], None);
        let err = load_pair(cert.to_str().unwrap(), other_key.to_str().unwrap()).err().unwrap();
        assert!(err.contains("does not belong"), "{}", err);
        let missing = load_pair("/nonexistent/cert.pem", other_key.to_str().unwrap()).err().unwrap();
        assert!(missing.contains("tls.cert_file"), "{}", missing);
    }

    #[tokio::test]
    async fn peers_are_verified_against_the_configured_ca_only() {
        let dir = temp_root();
        let ca = Ca::new();
        let ca_path = ca.write(&dir, "ca.pem");
        let (cert, key) = ca.issue(&dir, "node", &["127.0.0.1"], None);
        let tls = ServerTls::load(&tls_cfg(&cert, &key, &ca_path)).unwrap().unwrap();
        let addr = serve_hello(tls).await;

        let trusted = PeerTrust::load(&tls_cfg(&cert, &key, &ca_path)).unwrap();
        let body = client(&trusted).get(format!("https://{}/hello", addr)).send().await.unwrap().text().await.unwrap();
        assert_eq!(body, "hi");

        let stranger = Ca::new().write(&dir, "other-ca.pem");
        let untrusted = PeerTrust::load(&tls_cfg(&cert, &key, &stranger)).unwrap();
        let err = client(&untrusted).get(format!("https://{}/hello", addr)).send().await.unwrap_err();
        assert!(err.is_connect(), "a certificate from another CA must fail the handshake: {:?}", err);

        let wrong_name = ca.issue(&dir, "named", &["db.internal"], None);
        let named = ServerTls::load(&tls_cfg(&wrong_name.0, &wrong_name.1, &ca_path)).unwrap().unwrap();
        let named_addr = serve_hello(named).await;
        assert!(client(&trusted).get(format!("https://{}/hello", named_addr)).send().await.is_err(),
            "a certificate that does not name the dialed host must be refused");

        let plain = reqwest::Client::new().get(format!("http://{}/hello", addr)).send().await;
        assert!(plain.is_err() || !plain.unwrap().status().is_success(), "the TLS port serves no plaintext");
    }

    #[tokio::test]
    async fn a_serving_node_never_dials_plaintext() {
        let dir = temp_root();
        let ca = Ca::new();
        let ca_path = ca.write(&dir, "ca.pem");
        let (cert, key) = ca.issue(&dir, "node", &["127.0.0.1"], None);
        let port = next_test_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(serve(listener, Router::new().route("/hello", get(|| async { "hi" })), None));

        let serving = client(&PeerTrust::load(&tls_cfg(&cert, &key, &ca_path)).unwrap());
        assert!(serving.get(format!("http://127.0.0.1:{}/hello", port)).send().await.is_err(),
            "the shared secret would cross in the clear");
        let client_only: TlsConfig = serde_json::from_value(serde_json::json!({"ca_file": ca_path})).unwrap();
        let dialer = client(&PeerTrust::load(&client_only).unwrap());
        assert!(dialer.get(format!("http://127.0.0.1:{}/hello", port)).send().await.is_ok());
    }

    #[tokio::test]
    async fn a_replaced_certificate_is_served_without_a_restart() {
        let dir = temp_root();
        let ca = Ca::new();
        let ca_path = ca.write(&dir, "ca.pem");
        let (cert, key) = ca.issue(&dir, "node", &["127.0.0.1"], None);
        let tls = ServerTls::load(&tls_cfg(&cert, &key, &ca_path)).unwrap().unwrap();
        let addr = serve_hello(tls.clone()).await;
        let before = presented_serial(&addr, &ca_path).await;

        std::fs::write(&key, "not a key").unwrap();
        assert!(tls.reload().is_err());
        assert_eq!(presented_serial(&addr, &ca_path).await, before, "a bad file keeps the certificate in force");

        let soon = SystemTime::now() + Duration::from_secs(3 * 86_400);
        ca.issue(&dir, "node", &["127.0.0.1"], Some(soon));
        tls.reload().unwrap();
        assert_ne!(presented_serial(&addr, &ca_path).await, before, "the next handshake presents the new certificate");
        let left = tls.expires_in().unwrap();
        assert!(left > 2 * 86_400 && left <= 3 * 86_400, "{}", left);
        assert!(tls.check_expiry(), "three days out is inside the warning window");
    }

    #[test]
    fn a_serving_node_dials_recorded_http_urls_as_https() {
        let plain = reqwest::Client::new();
        let serving = PeerClient::with(Dialer { inner: plain.clone(), upgrade: true });
        assert_eq!(serving.dial_url("http://a:1/internal/x"), "https://a:1/internal/x");
        assert_eq!(serving.dial_url("HTTP://a:1"), "https://a:1");
        assert_eq!(serving.dial_url("https://a:1"), "https://a:1");
        let client_only = PeerClient::from(plain);
        assert_eq!(client_only.dial_url("http://a:1"), "http://a:1");
        assert_eq!(client_only.dial_url("https://a:1"), "https://a:1", "a plaintext node never downgrades");
    }

    /// IB-063: a CA rotation is a bundle rewrite. A client taken before the swap trusts the new bundle
    /// from its next request, because every holder of a `PeerClient` shares one slot.
    #[tokio::test]
    async fn a_replaced_client_trusts_the_new_bundle_in_every_clone() {
        let dir = temp_root();
        let (old_ca, new_ca) = (Ca::new(), Ca::new());
        let old_path = old_ca.write(&dir, "old-ca.pem");
        let new_path = new_ca.write(&dir, "new-ca.pem");
        let (cert, key) = new_ca.issue(&dir, "node", &["127.0.0.1"], None);
        let addr = serve_hello(ServerTls::load(&tls_cfg(&cert, &key, &new_path)).unwrap().unwrap()).await;
        let url = format!("https://{}/hello", addr);

        let auth = crate::auth::AuthConfig::default();
        let peer = crate::auth::build_client(&auth, &PeerTrust::load(&tls_cfg(&cert, &key, &old_path)).unwrap(), "n");
        let held = peer.clone();
        assert!(held.get(&url).send().await.is_err(), "a leaf from a CA not in the bundle must be refused");

        let bundle = dir.join("bundle.pem");
        std::fs::write(&bundle, [std::fs::read(&old_path).unwrap(), std::fs::read(&new_path).unwrap()].concat()).unwrap();
        let rotated = PeerTrust::load(&tls_cfg(&cert, &key, &bundle)).unwrap();
        peer.replace(crate::auth::build_client(&auth, &rotated, "n"));
        assert_eq!(held.get(&url).send().await.unwrap().text().await.unwrap(), "hi");
    }

    /// IB-061: a rotation rebuilds `stream_client` through `build_stream_client`, so it stays unpooled.
    /// The request client is the control that shows the counter would see a reused connection.
    #[tokio::test]
    async fn a_rebuilt_stream_client_still_opens_a_connection_per_request() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let url = format!("http://{}/x", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            while let Ok((mut conn, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while matches!(conn.read(&mut buf).await, Ok(n) if n > 0) {
                        let reply = b"HTTP/1.1 200 OK
content-length: 2

hi";
                        if conn.write_all(reply).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let config: crate::config::NodeConfig = serde_json::from_value(serde_json::json!({
            "node_id": "n", "role": "shard", "listen_addr": "127.0.0.1:1",
            "data_dir": temp_root().to_string_lossy()})).unwrap();
        let state = crate::state::AppState::for_routing_test(config);
        state.rotate_peer_trust(PeerTrust::default());
        assert_eq!(state.rotate_peer_credentials(vec!["s".to_string()], Some("k".to_string())), Ok(true));

        let twice = |client: PeerClient| {
            let (accepted, url) = (accepted.clone(), url.clone());
            async move {
                let before = accepted.load(Ordering::SeqCst);
                for _ in 0..2 {
                    client.get(&url).send().await.unwrap().text().await.unwrap();
                }
                accepted.load(Ordering::SeqCst) - before
            }
        };
        assert_eq!(twice(state.stream_client.clone()).await, 2, "the rebuilt stream client must not pool");
        assert_eq!(twice(state.client.clone()).await, 1, "the request client pools, so the counter can tell");
    }

    /// The stop-all conversion keeps `cluster.meta`, which still records the shard as `http://`; a router
    /// routes from that view, not from its config.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plaintext_cluster_converts_to_tls_keeping_its_view() {
        let root = temp_root();
        let ca = Ca::new();
        let ca_path = ca.write(&root, "ca.pem");
        let mut shard = TestNode::new("s1", next_test_port(), &root, "primary");
        let mut router = TestNode::new("router", next_test_port(), &root, "primary");
        router.role = "router".to_string();
        shard.start();
        router.shard_map = vec![(shard.url(), Vec::new())];
        router.start();
        let plain = reqwest::Client::new();
        assert!(wait_for_doc_written(&plain, &router.url(), "a", 1).await);
        shard.kill();
        router.kill();

        for node in [&mut shard, &mut router] {
            let (cert, key) = ca.issue(&root, &node.node_id, &["127.0.0.1"], None);
            node.tls = tls_json(&cert, &key, &ca_path);
        }
        router.shard_map = vec![(shard.url(), Vec::new())];
        shard.start();
        router.start();
        let view = crate::cluster::metadata::ClusterMetadata::load(router.data_dir.to_str().unwrap()).unwrap().unwrap();
        assert!(view.shards.iter().all(|s| s.node_url.starts_with("http://")), "{:?}", view.shards);

        let trust = PeerTrust::load(&serde_json::from_value(serde_json::json!({"ca_file": ca_path})).unwrap()).unwrap();
        let c = client(&trust);
        assert!(wait_for_doc(&c, &router.url(), "t", "a", 1, Duration::from_secs(10)).await,
            "the router must reach the shard at the https:// spelling of its recorded url");
        assert!(wait_for_doc_written(&c, &router.url(), "b", 2).await);
        drop((shard, router));
    }

    /// IB-042: append, promote, remove, one step at a time on a running pair. Replication survives
    /// every step, and the removed secret stops opening `/internal/*`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pair_rotates_its_internal_secret_without_a_restart() {
        let root = temp_root();
        let mut primary = TestNode::new("p", next_test_port(), &root, "primary");
        let mut replica = TestNode::new("r", next_test_port(), &root, "replica");
        for node in [&mut primary, &mut replica] {
            node.auth = serde_json::json!({"internal_secret": "old"});
        }
        primary.peers = vec![replica.url()];
        primary.replicas = vec![replica.url()];
        replica.peers = vec![primary.url()];
        replica.primary_addr = Some(primary.url());
        primary.start();
        replica.start();

        let c = reqwest::Client::new();
        assert!(put_doc_http(&c, &primary.url(), "k", 0).await.is_success());
        assert!(wait_for_doc(&c, &replica.url(), "t", "k", 0, Duration::from_secs(10)).await);

        let steps: [&[&str]; 3] = [&["old", "new"], &["new", "old"], &["new"]];
        for (v, secrets) in (1..).zip(steps) {
            for node in [&primary, &replica] {
                let secrets = secrets.iter().map(|s| s.to_string()).collect();
                assert_eq!(node.state.as_ref().unwrap().rotate_peer_credentials(secrets, None), Ok(true));
            }
            assert!(put_doc_http(&c, &primary.url(), "k", v).await.is_success(), "{:?}", secrets);
            assert!(wait_for_doc(&c, &replica.url(), "t", "k", v, Duration::from_secs(10)).await,
                "replication must survive the step to {:?}", secrets);
        }

        let heartbeat = |secret: &'static str| c.get(format!("{}/internal/heartbeat", replica.url()))
            .header(crate::auth::INTERNAL_SECRET_HEADER, secret).send();
        assert_eq!(heartbeat("old").await.unwrap().status(), reqwest::StatusCode::UNAUTHORIZED);
        assert!(heartbeat("new").await.unwrap().status().is_success());
        drop((primary, replica));
    }

    async fn wait_for_doc_written(c: &reqwest::Client, base: &str, key: &str, v: i64) -> bool {
        for _ in 0..100 {
            if put_doc_http(c, base, key, v).await.is_success() {
                return wait_for_doc(c, base, "t", key, v, Duration::from_secs(10)).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// Replication, the internal secret and a public read all over TLS, end to end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_replicated_pair_runs_over_tls() {
        let root = temp_root();
        let ca = Ca::new();
        let ca_path = ca.write(&root, "ca.pem");
        let (p_port, r_port) = (next_test_port(), next_test_port());
        let mut primary = TestNode::new("p", p_port, &root, "primary");
        let mut replica = TestNode::new("r", r_port, &root, "replica");
        for node in [&mut primary, &mut replica] {
            let (cert, key) = ca.issue(&root, &node.node_id, &["127.0.0.1"], None);
            node.tls = tls_json(&cert, &key, &ca_path);
            node.auth = serde_json::json!({"internal_secret": "s3cret"});
        }
        primary.peers = vec![replica.url()];
        primary.replicas = vec![replica.url()];
        replica.peers = vec![primary.url()];
        replica.primary_addr = Some(primary.url());
        assert!(primary.url().starts_with("https://"));
        primary.start();
        replica.start();

        let trust = PeerTrust::load(&serde_json::from_value(serde_json::json!({"ca_file": ca_path})).unwrap()).unwrap();
        let c = client(&trust);
        assert!(put_doc_http(&c, &primary.url(), "k", 7).await.is_success());
        assert!(wait_for_doc(&c, &replica.url(), "t", "k", 7, Duration::from_secs(10)).await,
            "the replica must receive the write over the TLS link");
        drop((primary, replica));
    }
}
