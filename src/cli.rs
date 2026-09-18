//! Argument parsing and the first-run commands, kept ahead of the config file.
//!
//! `--version` already answered before `dew.json` was read; everything else did not, so `--help` on a
//! host that has no config panicked (`Failed to read config file`). The rule here is that a question
//! about the binary -- what it is, how to run it, write me a config -- never depends on a config.

use std::fs;
use std::io;
use std::path::Path;

/// The default config, and whether the caller named it. A missing file is a first-run message when we
/// picked the path and an error about that path when the operator did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigPath {
    pub path: String,
    pub explicit: bool,
}

pub const DEFAULT_CONFIG_PATH: &str = "dew.json";

impl ConfigPath {
    fn default_path() -> Self {
        Self { path: DEFAULT_CONFIG_PATH.to_string(), explicit: false }
    }
}

/// What the argument vector asked for. The first of `--help`/`--version`/`init` seen wins, which is
/// how the old left-to-right loop treated `--version`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cli {
    Help,
    Version,
    Init(ConfigPath),
    Start(ConfigPath),
}

/// `args` is the whole vector including argv[0], as `std::env::args()` yields it.
///
/// Unrecognised arguments are ignored rather than rejected, which is what the loop this replaced did.
/// Tightening that is a separate change: it would turn a typo that boots today into a boot failure.
pub fn parse(args: &[String]) -> Cli {
    let mut config = ConfigPath::default_path();
    let mut mode: Option<Cli> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            // Still consumes the next argument unconditionally: `--config --version` named a file
            // called `--version` before this change, and still does.
            "--config" if i + 1 < args.len() => {
                config = ConfigPath { path: args[i + 1].clone(), explicit: true };
                i += 2;
                continue;
            },
            "--version" | "-V" => mode = mode.or(Some(Cli::Version)),
            "--help" | "-h" | "help" => mode = mode.or(Some(Cli::Help)),
            "init" => mode = mode.or(Some(Cli::Init(ConfigPath::default_path()))),
            _ => {},
        }
        i += 1;
    }

    match mode {
        Some(Cli::Init(_)) => Cli::Init(config),
        Some(other) => other,
        None => Cli::Start(config),
    }
}

/// Printed by `--help`, `-h` and `help`, before any file is touched.
pub fn usage() -> String {
    format!(
        "dewdb {version} -- distributed JSON document database in a single binary

USAGE
    dewdb [--config <path>]         start a node from a config file
    dewdb init [--config <path>]    write a starter config file
    dewdb --help                    print this message
    dewdb --version                 print the version

OPTIONS
    --config <path>    config file to read or write (default: ./{default})
    -h, --help         print this message
    -V, --version      print the version

FIRST RUN
    dewdb init                      writes ./{default}
    dewdb                           starts the node it describes
    curl localhost:8081/health      confirms it is up

    A single node needs five fields:

      {{
        \"node_id\": \"n1\",
        \"role\": \"shard\",
        \"shard_role\": \"primary\",
        \"listen_addr\": \"127.0.0.1:8081\",
        \"data_dir\": \"./data\"
      }}

    Collections are created by the first write. See docs/getting_started.md.
",
        version = env!("CARGO_PKG_VERSION"),
        default = DEFAULT_CONFIG_PATH,
    )
}

/// The file `dewdb init` writes: the smallest config that boots and serves, and the one the docs
/// open with. Deliberately not the full schema -- everything else has a default worth taking.
pub const STARTER_CONFIG: &str = r#"{
  "node_id": "n1",
  "role": "shard",
  "shard_role": "primary",
  "listen_addr": "127.0.0.1:8081",
  "data_dir": "./data"
}
"#;

/// Never overwrites: the config is the node's identity and its `data_dir`, so replacing one that is
/// already in use would point a running deployment at a different database.
pub fn init(config: &ConfigPath) -> Result<String, String> {
    if Path::new(&config.path).exists() {
        return Err(format!(
            "{} already exists; delete it or pass --config <path> to write somewhere else",
            config.path));
    }
    match fs::write(&config.path, STARTER_CONFIG) {
        Ok(()) => Ok(format!(
            "Wrote {}. Edit it if you like, then run `dewdb{}` to start the node.",
            config.path,
            if config.explicit { format!(" --config {}", config.path) } else { String::new() })),
        Err(e) => Err(format!("Could not write {}: {}", config.path, e)),
    }
}

/// Why a start could not read its config. All three are the user's own mistake -- nothing written
/// yet, a mistyped path, a path that cannot be read -- so none of them is a panic. What is *in* the
/// file is not judged here: malformed JSON and a config that fails validation still fail at boot.
#[derive(Debug)]
pub enum ConfigReadError {
    /// Nothing at `./dew.json` and no `--config`: a first run, pointed at `init`.
    MissingDefault,
    /// Nothing at the path the operator named. They know what they meant; name it back to them.
    Missing(String),
    Unreadable(String, io::Error),
}

impl ConfigReadError {
    /// What a user sees on stderr in place of a panic and a backtrace note.
    pub fn message(&self) -> String {
        match self {
            Self::MissingDefault => format!(
                "No {default} found.\nRun `dewdb init` to create one, or use `dewdb --config <path>`.",
                default = DEFAULT_CONFIG_PATH),
            Self::Missing(path) => format!("Config file not found: {}", path),
            Self::Unreadable(path, e) => format!("Could not read {}: {}", path, e),
        }
    }
}

pub fn read_config(config: &ConfigPath) -> Result<String, ConfigReadError> {
    match fs::read_to_string(&config.path) {
        Ok(body) => Ok(body),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(if config.explicit {
            ConfigReadError::Missing(config.path.clone())
        } else {
            ConfigReadError::MissingDefault
        }),
        Err(e) => Err(ConfigReadError::Unreadable(config.path.clone(), e)),
    }
}

/// No usable config: none written yet, the named one is not there, or it could not be read. Distinct
/// from 1 so a script can tell "no config here" from "the config is wrong", and from 101, which is
/// what a panic leaves behind.
pub const EXIT_NO_CONFIG: i32 = 2;
/// The config was read but the node will not run it: malformed JSON, or a rule `validate` refused.
/// The file is the thing to fix, which is what separates this from `EXIT_NO_CONFIG`.
pub const EXIT_BAD_CONFIG: i32 = 3;
/// `dewdb init` could not write the file.
pub const EXIT_INIT_FAILED: i32 = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn argv(rest: &[&str]) -> Vec<String> {
        std::iter::once("dewdb".to_string()).chain(rest.iter().map(|s| s.to_string())).collect()
    }

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("dew-cli-{}-{}-{:?}", tag, std::process::id(), std::thread::current().id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("a scratch directory for the test");
        dir
    }

    /// `cargo test` builds the binary next to the test binary, so this exists in a normal run. A
    /// vendored or oddly-profiled build should skip the process assertions, not fail them.
    fn binary() -> Option<std::path::PathBuf> {
        let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target").join(profile).join(format!("dewdb{}", std::env::consts::EXE_SUFFIX));
        path.exists().then_some(path)
    }

    /// (stdout, stderr, exit code) from running the binary in a directory that has no `dew.json`.
    fn run_without_a_config(tag: &str, args: &[&str]) -> Option<(String, String, i32)> {
        let bin = binary()?;
        let dir = scratch(tag);
        assert!(!dir.join(DEFAULT_CONFIG_PATH).exists(), "the test directory must start empty");
        let out = Command::new(bin).args(args).current_dir(&dir).output().expect("the binary runs");
        let result = (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
            out.status.code().unwrap_or(-1),
        );
        let _ = fs::remove_dir_all(&dir);
        Some(result)
    }

    /// The regression this module exists for. Before the fix every one of these read `dew.json` and
    /// panicked (`Failed to read config file`, exit 101) in a directory that did not have one.
    #[test]
    fn asking_the_binary_what_it_is_never_reads_a_config() {
        for args in [vec!["--help"], vec!["-h"], vec!["help"], vec!["--version"], vec!["-V"]] {
            let decided = parse(&argv(&args));
            assert!(matches!(decided, Cli::Help | Cli::Version),
                "{:?} has to be answered before the config file, got {:?}", args, decided);
        }
        assert_eq!(parse(&argv(&[])), Cli::Start(ConfigPath::default_path()),
            "a bare invocation is still a start");
    }

    #[test]
    fn help_without_a_config_prints_usage_and_succeeds() {
        for args in [["--help"], ["-h"]] {
            let Some((stdout, stderr, code)) = run_without_a_config("help", &args) else { return };
            assert_eq!(code, 0, "`dewdb {}` must succeed with no dew.json; stderr: {}", args[0], stderr);
            assert!(!stderr.contains("panicked"), "a panic is the bug being fixed: {}", stderr);
            assert!(stdout.contains("USAGE"), "usage is the point of --help: {}", stdout);
            assert!(stdout.contains("./dew.json"), "the default config path has to be named: {}", stdout);
            assert!(stdout.contains("dewdb init"), "the way to get a config has to be named: {}", stdout);
            assert!(stdout.contains("--config <path>"), "the override has to be named: {}", stdout);
        }
    }

    /// The bare subcommand spelling, which is what someone types after `git help` habits.
    #[test]
    fn the_help_subcommand_without_a_config_matches_the_flag() {
        let Some((subcommand, _, sub_code)) = run_without_a_config("help-sub", &["help"]) else { return };
        let Some((flag, _, flag_code)) = run_without_a_config("help-flag", &["--help"]) else { return };
        assert_eq!(sub_code, 0, "`dewdb help` must succeed with no dew.json");
        assert_eq!(flag_code, 0);
        assert_eq!(subcommand, flag, "`help` and `--help` must not drift apart");
    }

    /// Unchanged by this work, and asserted here so the rewrite of the argument loop cannot move it.
    #[test]
    fn version_without_a_config_is_what_it_always_was() {
        let Some((stdout, stderr, code)) = run_without_a_config("version", &["--version"]) else { return };
        assert_eq!(code, 0);
        assert_eq!(stdout, format!("dewdb {}\n", env!("CARGO_PKG_VERSION")));
        assert!(stderr.is_empty(), "nothing goes to stderr on the version path: {}", stderr);
    }

    #[test]
    fn a_bare_run_without_a_config_explains_itself_instead_of_panicking() {
        let Some((stdout, stderr, code)) = run_without_a_config("bare", &[]) else { return };
        assert_ne!(code, 0, "a node that could not start must not report success");
        assert_eq!(code, EXIT_NO_CONFIG, "the first-run exit code is distinct from a crash");
        assert!(!stderr.contains("panicked"), "the panic is the regression: {}", stderr);
        assert!(!stderr.contains("Failed to read config file"),
            "the internal expect message must not reach a first-time user: {}", stderr);
        assert!(stderr.contains("No dew.json found."), "stderr: {}", stderr);
        assert!(stderr.contains("dewdb init"), "stderr: {}", stderr);
        assert!(stderr.contains("dewdb --config <path>"), "stderr: {}", stderr);
        assert!(stdout.is_empty(), "the error belongs on stderr: {}", stdout);
    }

    /// A bare run that *does* find `dew.json` still reads it, so the fix cannot have turned a working
    /// start into the first-run message. Starting the node is not a CLI concern, so this stops at the
    /// point the config is in hand.
    #[test]
    fn a_bare_run_with_a_config_reads_it() {
        let dir = scratch("bare-ok");
        let path = dir.join(DEFAULT_CONFIG_PATH);
        fs::write(&path, STARTER_CONFIG).unwrap();

        let decided = parse(&argv(&[]));
        let Cli::Start(config) = decided else { panic!("a bare invocation is a start") };
        assert_eq!(config, ConfigPath::default_path());

        // `parse` yields a relative path; the read is the part under test, so name the real file.
        let here = ConfigPath { path: path.to_string_lossy().into_owned(), explicit: false };
        assert_eq!(read_config(&here).expect("an existing default config is read"), STARTER_CONFIG);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_explicit_config_is_taken() {
        assert_eq!(parse(&argv(&["--config", "other.json"])),
            Cli::Start(ConfigPath { path: "other.json".to_string(), explicit: true }));
        // Order and position do not matter, and `--config` still swallows whatever follows it.
        assert_eq!(parse(&argv(&["--config", "--version"])),
            Cli::Start(ConfigPath { path: "--version".to_string(), explicit: true }),
            "a path that looks like a flag was a path before this change");

        let dir = scratch("explicit");
        let path = dir.join("node.json");
        fs::write(&path, STARTER_CONFIG).unwrap();
        let named = ConfigPath { path: path.to_string_lossy().into_owned(), explicit: true };
        assert_eq!(read_config(&named).expect("a named config is read"), STARTER_CONFIG);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A path the operator named and got wrong is their typo, not a crash. It also must not be
    /// answered with the first-run message: they did not ask for `./dew.json` and `init` would write
    /// the wrong file. Before this, a missing `--config` panicked with exit 101.
    #[test]
    fn a_missing_explicit_config_names_the_path_and_does_not_panic() {
        let dir = scratch("explicit-missing");
        let absent = dir.join("nope.json");
        let named = ConfigPath { path: absent.to_string_lossy().into_owned(), explicit: true };

        let e = read_config(&named).expect_err("a path that is not there cannot be read");
        assert!(matches!(e, ConfigReadError::Missing(_)),
            "a named path that is absent is its own case, not the first-run one");
        let message = e.message();
        assert!(message.starts_with("Config file not found: "), "{}", message);
        assert!(message.contains("nope.json"), "the path the operator typed has to appear: {}", message);
        assert!(!message.contains("Failed to read config file"),
            "the old expect message must not survive anywhere: {}", message);
        assert!(!message.contains("dewdb init"),
            "init would write ./dew.json, which is not the file they asked for: {}", message);

        // And end to end, because the exit code and the absence of a panic are the point.
        if let Some(bin) = binary() {
            let out = std::process::Command::new(bin)
                .args(["--config", absent.to_string_lossy().as_ref()])
                .current_dir(&dir).output().expect("the binary runs");
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert_eq!(out.status.code(), Some(EXIT_NO_CONFIG), "stderr: {}", stderr);
            assert!(!stderr.contains("panicked"), "no panic on a user typo: {}", stderr);
            assert!(!stderr.contains("RUST_BACKTRACE"), "no backtrace note: {}", stderr);
            assert!(!stderr.contains("Failed to read config file"),
                "the internal expect message must not leak: {}", stderr);
            assert!(stderr.contains("Config file not found:") && stderr.contains("nope.json"),
                "stderr: {}", stderr);
            assert!(stdout.is_empty(), "the error belongs on stderr: {}", stdout);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Runs the binary against a `dew.json` it wrote, and returns (stdout, stderr, exit code).
    /// Every config here is refused before the node binds a port, so nothing is left running.
    fn start_with_a_config(tag: &str, body: &str) -> Option<(String, String, i32)> {
        let bin = binary()?;
        let dir = scratch(tag);
        fs::write(dir.join(DEFAULT_CONFIG_PATH), body).unwrap();
        let out = std::process::Command::new(bin).current_dir(&dir).output().expect("the binary runs");
        let result = (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
            out.status.code().unwrap_or(-1),
        );
        let _ = fs::remove_dir_all(&dir);
        Some(result)
    }

    /// The file is there and is not JSON. Someone hand-edited it; serde already knows where they went
    /// wrong, and a panic frame around that reason helps nobody. Exit 101 before this change.
    #[test]
    fn a_malformed_config_fails_cleanly_and_still_says_where() {
        let Some((stdout, stderr, code)) = start_with_a_config("malformed", "{ not json") else { return };
        assert_ne!(code, 0, "a node that did not start must not report success");
        assert_eq!(code, EXIT_BAD_CONFIG, "a config that is present but wrong is its own exit code");
        assert!(!stderr.contains("panicked at"), "stderr: {}", stderr);
        assert!(!stderr.contains("RUST_BACKTRACE"), "stderr: {}", stderr);
        // The reason is the point of keeping the message at all.
        assert!(stderr.contains("Invalid config JSON format:"), "the prefix is unchanged: {}", stderr);
        assert!(stderr.contains("line 1"), "serde's position has to survive: {}", stderr);
        assert!(stderr.contains("column"), "stderr: {}", stderr);
        assert!(stdout.is_empty(), "the error belongs on stderr: {}", stdout);
    }

    /// Valid JSON that `validate` refuses. The rule it refused on is the whole message; this asserts
    /// the rule reaches the user, not that the rule exists -- config.rs owns that.
    #[test]
    fn a_config_that_fails_validation_fails_cleanly_and_still_says_why() {
        let body = r#"{"node_id":"n1","role":"banana","listen_addr":"127.0.0.1:8081","data_dir":"./data"}"#;
        // The premise: this parses, so it is validation and not serde that refuses it.
        let parsed: crate::config::NodeConfig = serde_json::from_str(body)
            .expect("the fixture has to be valid JSON or it tests the wrong branch");
        let reason = parsed.validate().expect_err("'banana' is not a role");

        let Some((stdout, stderr, code)) = start_with_a_config("invalid", body) else { return };
        assert_ne!(code, 0, "a node that did not start must not report success");
        assert_eq!(code, EXIT_BAD_CONFIG);
        assert!(!stderr.contains("panicked at"), "stderr: {}", stderr);
        assert!(!stderr.contains("RUST_BACKTRACE"), "stderr: {}", stderr);
        assert!(stderr.contains("Invalid config map constraints:"), "the prefix is unchanged: {}", stderr);
        assert!(stderr.contains(&reason),
            "the rule validate refused on has to reach the user verbatim; wanted {:?} in {:?}",
            reason, stderr);
        assert!(stdout.is_empty(), "the error belongs on stderr: {}", stdout);
    }

    /// Not-found is a typo; anything else the filesystem says is still not a panic. A directory
    /// where a file was named is the portable way to get a non-NotFound read error.
    #[test]
    fn a_config_path_that_cannot_be_read_is_reported_rather_than_panicking() {
        let dir = scratch("unreadable");
        let path = dir.join("a-directory.json");
        fs::create_dir_all(&path).unwrap();
        let named = ConfigPath { path: path.to_string_lossy().into_owned(), explicit: true };

        let e = read_config(&named).expect_err("a directory is not a config file");
        assert!(matches!(e, ConfigReadError::Unreadable(..)), "{:?}", e);
        assert!(e.message().starts_with("Could not read "), "{}", e.message());
        assert!(!e.message().contains("Failed to read config file"), "{}", e.message());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn init_writes_a_config_that_boots_and_refuses_to_replace_one() {
        let dir = scratch("init");
        let path = dir.join(DEFAULT_CONFIG_PATH);
        let target = ConfigPath { path: path.to_string_lossy().into_owned(), explicit: false };

        let note = init(&target).expect("an empty directory gets a starter config");
        assert!(note.contains("dew.json"), "{}", note);
        let written = fs::read_to_string(&path).unwrap();
        assert_eq!(written, STARTER_CONFIG);

        // The whole point of the file: what init writes has to be a config the node accepts.
        let parsed: crate::config::NodeConfig = serde_json::from_str(&written)
            .expect("the starter config has to parse under deny_unknown_fields");
        parsed.validate().expect("the starter config has to pass boot validation");

        fs::write(&path, "{\"node_id\":\"mine\"}").unwrap();
        let e = init(&target).expect_err("init must not overwrite a config a node may be running on");
        assert!(e.contains("already exists"), "{}", e);
        assert_eq!(fs::read_to_string(&path).unwrap(), "{\"node_id\":\"mine\"}",
            "the existing file is untouched");

        assert_eq!(parse(&argv(&["init"])), Cli::Init(ConfigPath::default_path()));
        assert_eq!(parse(&argv(&["init", "--config", "n.json"])),
            Cli::Init(ConfigPath { path: "n.json".to_string(), explicit: true }),
            "init writes where --config points, whichever order they are given in");
        let _ = fs::remove_dir_all(&dir);
    }

    /// `init` must carry its own config text. `STARTER_CONFIG` is a `const`, so this is true by
    /// construction, but a later edit could reach for `examples/dew.json` and pass every other test
    /// in this file -- they all run inside the repo. So: copy the binary somewhere with no repo
    /// around it, run it in a third directory, and require the same bytes out.
    #[test]
    fn init_carries_its_own_config_and_reads_nothing_from_disk() {
        let Some(bin) = binary() else { return };
        let away = scratch("init-standalone");
        let lone = away.join(format!("dewdb{}", std::env::consts::EXE_SUFFIX));
        fs::copy(&bin, &lone).expect("the binary can be copied out of the target directory");
        assert!(!away.join("examples").exists(), "nothing from the repo may sit beside the binary");

        let elsewhere = scratch("init-standalone-cwd");
        assert!(!elsewhere.join("examples").exists(), "nor in the working directory");
        let out = std::process::Command::new(&lone)
            .arg("init").current_dir(&elsewhere).output().expect("the lone binary runs");
        assert_eq!(out.status.code(), Some(0),
            "init away from the repo: {}", String::from_utf8_lossy(&out.stderr));
        assert_eq!(fs::read_to_string(elsewhere.join(DEFAULT_CONFIG_PATH)).unwrap(), STARTER_CONFIG,
            "the same bytes, with no examples/dew.json anywhere in reach");

        let _ = fs::remove_dir_all(&away);
        let _ = fs::remove_dir_all(&elsewhere);
    }

    /// `examples/dew.json` is the config the README and the guide open with. It is the oracle here,
    /// never an input: if the two drift apart, the docs stop describing what `init` writes.
    #[test]
    fn the_starter_config_covers_the_same_fields_as_the_published_example() {
        let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples").join("dew.json");
        let Ok(body) = fs::read_to_string(&example) else { return };

        let fields = |s: &str| -> Vec<String> {
            let v: serde_json::Value = serde_json::from_str(s).expect("valid JSON");
            let mut k: Vec<String> = v.as_object().expect("an object").keys().cloned().collect();
            k.sort();
            k
        };
        assert_eq!(fields(STARTER_CONFIG), fields(&body),
            "`dewdb init` and examples/dew.json have to teach the same five fields");
    }
}
