//! tracing setup and the node-stamped event formatter.

use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "text".to_string() }

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { level: default_log_level(), format: default_log_format() }
    }
}

impl LoggingConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.level.parse::<tracing::Level>().is_err() {
            return Err(format!("logging.level must be one of trace|debug|info|warn|error, got '{}'", self.level));
        }
        if self.format != "text" && self.format != "json" {
            return Err(format!("logging.format must be 'text' or 'json', got '{}'", self.format));
        }
        Ok(())
    }
}

struct EventFields {
    pub message: Option<String>,
    pub extra: Vec<(&'static str, serde_json::Value)>,
}

impl tracing::field::Visit for EventFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.push(field.name(), serde_json::Value::String(value.to_string()));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.push(field.name(), serde_json::Value::from(value));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.push(field.name(), serde_json::Value::from(value));
    }
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.push(field.name(), serde_json::Value::from(value));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.push(field.name(), serde_json::Value::Bool(value));
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.push(field.name(), serde_json::Value::String(format!("{:?}", value)));
    }
}

impl EventFields {
    pub fn collect(event: &tracing::Event<'_>) -> Self {
        let mut me = Self { message: None, extra: Vec::new() };
        event.record(&mut me);
        me
    }

    pub fn push(&mut self, name: &'static str, value: serde_json::Value) {
        if name == "message" {
            self.message = Some(match value {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            });
        } else {
            self.extra.push((name, value));
        }
    }
}

struct NodeFormat {
    pub node_id: String,
    pub json: bool,
}

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for NodeFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let meta = event.metadata();
        let fields = EventFields::collect(event);

        if self.json {
            let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            let mut map = serde_json::Map::new();
            map.insert("ts_ms".into(), serde_json::Value::from(millis));
            map.insert("level".into(), serde_json::Value::String(meta.level().to_string()));
            map.insert("node_id".into(), serde_json::Value::String(self.node_id.clone()));
            map.insert("target".into(), serde_json::Value::String(meta.target().to_string()));
            if let Some(msg) = fields.message {
                map.insert("message".into(), serde_json::Value::String(msg));
            }
            for (k, v) in fields.extra {
                map.insert(k.to_string(), v);
            }
            return writeln!(writer, "{}", serde_json::Value::Object(map));
        }

        tracing_subscriber::fmt::time::FormatTime::format_time(
            &tracing_subscriber::fmt::time::SystemTime, &mut writer)?;
        write!(writer, " {:>5} node={} [{}]", meta.level(), self.node_id, meta.target())?;
        if let Some(msg) = fields.message {
            write!(writer, " {}", msg)?;
        }
        for (k, v) in fields.extra {
            match v {
                serde_json::Value::String(s) => write!(writer, " {}={}", k, s)?,
                other => write!(writer, " {}={}", k, other)?,
            }
        }
        writeln!(writer)
    }
}

pub fn init_logging(cfg: &LoggingConfig, node_id: &str) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.level));

    let layer = tracing_subscriber::fmt::layer()
        .event_format(NodeFormat { node_id: node_id.to_string(), json: cfg.format == "json" });

    tracing_subscriber::registry().with(filter).with(layer).init();
}
