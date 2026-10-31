//! Configuration reload (RFD 018): the configuration read again on the poll
//! interval and at once on request (SIGHUP, in the binary), its bytes'
//! digest compared with the digest of the configuration applied, and a
//! changed configuration's tables applied through `Opened::apply`, all or
//! nothing. A reload refuses a change to any setting outside the tables,
//! since the server builds those once at start. Each reload that read a
//! changed file is logged with its digest and counted under
//! `config_reloads_total` by result, each result's series registered at
//! zero when the reload starts, and `config_pending` reads 1 while the file
//! differs from what the server applied.

use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use icetray_core::budget::Budget;
use icetray_core::config::{Config, ConfigError, ConfigViolation};
use icetray_core::{Applied, Opened, Refused, Table, approx, metric};
use metrics::Counter;
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;

use crate::cli::ConfigPath;
use crate::error::ConfigRejected;
use crate::serve::{retain, ticker};

/// Bytes in a configuration digest: SHA-256's output.
pub const DIGEST_BYTES: usize = 32;

/// The SHA-256 digest of a configuration's bytes, what a reload compares to
/// decide whether the configuration changed. Display gives lowercase hex,
/// as the log carries it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Digest([u8; DIGEST_BYTES]);

impl Digest {
    /// The digest of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// One read of the configuration: the digest of the bytes read and what
/// they parse and validate to.
pub struct Reading {
    /// The digest of the bytes read.
    pub digest: Digest,
    /// The configuration the bytes hold, or why they hold none, naming the
    /// file.
    pub config: Result<Config, ConfigRejected>,
}

/// Where a running server reads its configuration again. `ConfigFile` is
/// the binary's; a test supplies its own.
pub trait ConfigSource: Send + Sync {
    /// The configuration as it stands now; `Err` when no bytes could be
    /// read. Called on the blocking pool.
    fn read(&self) -> io::Result<Reading>;
}

/// A configuration file: the one `serve` was started from, which it reads
/// again on every reload, or the one a one-shot reads once.
#[derive(Clone, Debug)]
pub struct ConfigFile {
    file: ConfigPath,
}

impl ConfigFile {
    /// The file `file` names.
    pub fn new(file: ConfigPath) -> Self {
        Self { file }
    }
}

impl ConfigSource for ConfigFile {
    /// Reads the whole file, digests its bytes, and parses them as
    /// `Config::parse` does; bytes that are not UTF-8 are
    /// `ConfigError::Read`. Every fault is a `ConfigRejected` naming the
    /// file and where its path came from.
    fn read(&self) -> io::Result<Reading> {
        let bytes = std::fs::read(&self.file.path)?;
        let digest = Digest::of(&bytes);
        let config = std::str::from_utf8(&bytes)
            .map_err(|error| ConfigError::Read {
                path: self.file.path.clone(),
                source: io::Error::new(io::ErrorKind::InvalidData, error),
            })
            .and_then(Config::parse)
            .map_err(|source| ConfigRejected {
                file: self.file.clone(),
                source,
            });
        Ok(Reading { digest, config })
    }
}

/// What a `Serving` follows its configuration through: where it reads it
/// again, and the digest of the configuration it started from.
#[derive(Clone)]
pub struct Reload {
    /// Where the configuration is read again.
    pub source: Arc<dyn ConfigSource>,
    /// The digest of the configuration the server starts from.
    pub started_from: Digest,
}

impl fmt::Debug for Reload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reload")
            .field("started_from", &self.started_from)
            .finish_non_exhaustive()
    }
}

/// Why a reload changed nothing. Display gives the log's `reason`.
#[derive(Debug)]
pub enum ReloadRefusal {
    /// The bytes do not parse or validate.
    Invalid(ConfigRejected),
    /// Settings outside the tables changed, by their names in the file.
    Restart(Vec<&'static str>),
    /// The caches' split for the new tables, which a start with the file
    /// would refuse on this machine (RFD 028): a page cache part under the
    /// minimum for its shards, which parsing checks against one shard only.
    Split(ConfigViolation),
    /// A table failed to open or load, or its serving generation does not
    /// meet its retuned `lookups`, `strict`, column formats, or
    /// `max_scan_rows`.
    Tables(Refused),
}

impl fmt::Display for ReloadRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => write!(f, "{error}"),
            Self::Split(violation) => write!(f, "{violation}"),
            Self::Restart(settings) => write!(
                f,
                "{} changed, which only a restart applies",
                settings.join(", ")
            ),
            Self::Tables(refused) => {
                let names: Vec<&str> = refused
                    .failures
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect();
                write!(f, "tables refused: {}", names.join(", "))
            }
        }
    }
}

/// A reload's outcome, the `result` label of `config_reloads_total`.
/// Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReloadResult {
    /// The configuration's tables were swapped in.
    Applied,
    /// Nothing changed.
    Refused,
}

impl ReloadResult {
    /// Every result, for the series registered when the reload starts.
    pub const ALL: [Self; 2] = [Self::Applied, Self::Refused];
}

impl fmt::Display for ReloadResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Applied => "applied",
            Self::Refused => "refused",
        })
    }
}

/// Whether the configuration file differs from the configuration the
/// server applied, what `config_pending` reads (RFD 018): set by the
/// reload, written to the gauge by every scrape (`Scrape::render`).
/// Clone shares the flag; it starts false, the file started from being the
/// one applied.
#[derive(Clone, Debug, Default)]
pub struct Pending(Arc<AtomicBool>);

impl Pending {
    fn set(&self, pending: bool) {
        self.0.store(pending, Ordering::Relaxed);
    }

    /// Sets `config_pending` to the flag: 1 while the file differs, 0 once
    /// they match.
    pub fn publish(&self) {
        let pending = self.0.load(Ordering::Relaxed);
        metrics::gauge!(metric::CONFIG_PENDING).set(approx::gauge(u64::from(pending)));
    }
}

/// The run's side of a reload: the configuration applied, its digest, the
/// last digest refused, and the two triggers.
pub(crate) struct Reloader {
    source: Arc<dyn ConfigSource>,
    /// The configuration applied: the one started from, with the tables of
    /// the last reload applied.
    running: Config,
    /// The digest of `running`'s bytes.
    applied: Digest,
    /// The digest of the last configuration refused. A poll does not try
    /// it again; a request does, since a table's load can fail for a
    /// reason that has since passed.
    refused: Option<Digest>,
    /// Whether the last read failed, so a file that stays unreadable is
    /// logged once and not at every poll.
    unreadable: bool,
    ticker: tokio::time::Interval,
    requested: Arc<Notify>,
    /// Whether the file read last differs from `running`.
    pending: Pending,
}

impl Reloader {
    /// Follows `reload` from `running`, reading on every `poll_interval`
    /// from one interval from now and whenever `requested` is notified, and
    /// keeping `pending`, which starts false. Registers
    /// `config_reloads_total` of every result.
    pub(crate) fn new(
        reload: Reload,
        running: Config,
        poll_interval: Duration,
        requested: Arc<Notify>,
        pending: Pending,
    ) -> Self {
        for result in ReloadResult::ALL {
            reloads_total(result).increment(0);
        }
        Self {
            source: reload.source,
            running,
            applied: reload.started_from,
            refused: None,
            unreadable: false,
            ticker: ticker(poll_interval),
            requested,
            pending,
        }
    }

    /// Waits for polls and requests until one reload applies, then returns
    /// what it changed and the split its tables take
    /// (`Opened::check_split`), for `Opened::fit_caches`. A read that finds the configuration applied, or on
    /// a poll the one last refused, does nothing further; a refused reload
    /// is logged and counted, and the wait goes on. A read that fails is
    /// no reload: it is logged when the file becomes unreadable, and
    /// neither counted nor reflected in `config_pending`, since whether the
    /// file differs is unknown. Dropping the future
    /// drops a reload in flight, which swaps nothing in.
    pub(crate) async fn next(&mut self, opened: &Opened) -> (Applied, Budget) {
        loop {
            let requested = tokio::select! {
                _ = self.ticker.tick() => false,
                () = self.requested.notified() => true,
            };
            if let Some(reloaded) = self.attempt(opened, requested).await {
                return reloaded;
            }
        }
    }

    /// One read, and the reload it calls for.
    async fn attempt(&mut self, opened: &Opened, requested: bool) -> Option<(Applied, Budget)> {
        let source = Arc::clone(&self.source);
        let read = match tokio::task::spawn_blocking(move || source.read()).await {
            Ok(read) => read,
            Err(join) => std::panic::resume_unwind(join.into_panic()),
        };
        let reading = match read {
            Ok(reading) => reading,
            Err(error) => {
                if !self.unreadable {
                    tracing::warn!(%error, "configuration cannot be read");
                }
                self.unreadable = true;
                return None;
            }
        };
        self.unreadable = false;
        let digest = reading.digest;
        if digest == self.applied {
            self.pending.set(false);
            return None;
        }
        self.pending.set(true);
        if !requested && self.refused == Some(digest) {
            return None;
        }
        match self.reload(opened, reading).await {
            Ok(reloaded) => Some(reloaded),
            Err(refusal) => {
                self.refuse(digest, &refusal);
                if matches!(refusal, ReloadRefusal::Tables(_)) {
                    // The tables that did load wrote chunk files no table
                    // serves; the next swap's retention would free them
                    // only then.
                    retain(opened).await;
                }
                None
            }
        }
    }

    /// Applies `reading`'s tables when nothing else in it changed and the
    /// caches' split for them is one a start with the file would take
    /// (`Opened::check_split`), and returns what it changed and that split.
    async fn reload(
        &mut self,
        opened: &Opened,
        reading: Reading,
    ) -> Result<(Applied, Budget), ReloadRefusal> {
        let config = reading.config.map_err(ReloadRefusal::Invalid)?;
        let restart = self.running.restart_settings(&config);
        if !restart.is_empty() {
            return Err(ReloadRefusal::Restart(restart));
        }
        let split = opened
            .check_split(&config.tables)
            .map_err(ReloadRefusal::Split)?;
        let applied = opened
            .apply(&config.tables)
            .await
            .map_err(ReloadRefusal::Tables)?;
        let (added, changed, removed) = changes(&applied);
        let retuned = names(&applied.retuned);
        tracing::info!(
            digest = %reading.digest,
            ?added,
            ?changed,
            ?retuned,
            ?removed,
            kept = applied.kept,
            "configuration reloaded"
        );
        // The state is set before the count, so a scrape that reads the
        // count also reads the file as applied.
        self.pending.set(false);
        self.running = config;
        self.applied = reading.digest;
        self.refused = None;
        count(ReloadResult::Applied);
        Ok((applied, split))
    }

    /// Logs and counts `refusal` of the configuration digested as `digest`,
    /// and remembers the digest.
    fn refuse(&mut self, digest: Digest, refusal: &ReloadRefusal) {
        if let ReloadRefusal::Tables(refused) = refusal {
            for (table, error) in &refused.failures {
                tracing::warn!(
                    table = table.as_str(),
                    reason = %error.kind(),
                    %error,
                    "table refused for a reload"
                );
            }
        }
        tracing::warn!(%digest, reason = %refusal, "configuration reload refused");
        self.refused = Some(digest);
        self.pending.set(true);
        count(ReloadResult::Refused);
    }
}

/// Counts one reload of `result` under `config_reloads_total`.
fn count(result: ReloadResult) {
    reloads_total(result).increment(1);
}

/// The `config_reloads_total` series of `result`.
fn reloads_total(result: ReloadResult) -> Counter {
    metrics::counter!(
        metric::CONFIG_RELOADS_TOTAL,
        metric::LABEL_RESULT => result.to_string()
    )
}

/// The names `applied` added, changed, and removed: a name both started
/// and stopped is a changed table.
fn changes(applied: &Applied) -> (Vec<&str>, Vec<&str>, Vec<&str>) {
    let started = names(&applied.started);
    let stopped = names(&applied.stopped);
    let added = started
        .iter()
        .copied()
        .filter(|name| !stopped.contains(name))
        .collect();
    let changed = started
        .iter()
        .copied()
        .filter(|name| stopped.contains(name))
        .collect();
    let removed = stopped
        .iter()
        .copied()
        .filter(|name| !started.contains(name))
        .collect();
    (added, changed, removed)
}

/// The names of `tables`, in order.
fn names(tables: &[Arc<Table>]) -> Vec<&str> {
    tables.iter().map(|table| table.name()).collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use std::path::Path;

    use super::*;
    use crate::cli::PathOrigin;

    /// A configuration `Config::parse` accepts: one static table.
    const ONE_TABLE: &str = r#"
[[tables]]
name = "kv"
ref = "main"
key_column = "id"
value_columns = ["payload"]
[tables.source]
type = "static"
location = "s3://warehouse/kv"
"#;

    /// The configuration file at `path`, as `--config` names it.
    fn flag_file(path: &Path) -> ConfigPath {
        ConfigPath {
            path: path.to_path_buf(),
            origin: PathOrigin::Flag,
        }
    }

    fn file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(bytes).unwrap();
        file
    }

    #[test]
    fn test_config_file_reads_the_digest_and_the_configuration() {
        let file = file_with(ONE_TABLE.as_bytes());
        let reading = ConfigFile::new(flag_file(file.path())).read().unwrap();
        assert_eq!(reading.digest, Digest::of(ONE_TABLE.as_bytes()));
        let config = reading.config.unwrap();
        assert_eq!(config.tables[0].name, "kv");
    }

    #[test]
    fn test_config_file_digests_bytes_that_do_not_parse() {
        let bytes = b"[[tables]\n";
        let file = file_with(bytes);
        let reading = ConfigFile::new(flag_file(file.path())).read().unwrap();
        assert_eq!(reading.digest, Digest::of(bytes));
        assert!(reading.config.is_err());
    }

    #[test]
    fn test_config_file_reads_a_file_with_no_table_as_invalid_naming_the_file() {
        let file = file_with(b"[server]\npoll_interval = \"30s\"\n");
        let reading = ConfigFile::new(flag_file(file.path())).read().unwrap();
        let Err(rejected) = reading.config else {
            panic!("a file with no table is invalid");
        };
        assert_eq!(rejected.file, flag_file(file.path()));
        assert!(matches!(
            rejected.source,
            ConfigError::Invalid(ConfigViolation::NoTables)
        ));
    }

    #[test]
    fn test_config_file_reads_bytes_that_are_not_utf8_as_a_read_error() {
        let file = file_with(&[0xff, 0xfe]);
        let reading = ConfigFile::new(flag_file(file.path())).read().unwrap();
        assert!(matches!(
            reading.config,
            Err(ConfigRejected {
                source: ConfigError::Read { .. },
                ..
            })
        ));
    }

    #[test]
    fn test_config_file_that_is_gone_is_an_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = ConfigFile::new(flag_file(&dir.path().join("missing.toml")));
        assert!(missing.read().is_err());
    }

    #[test]
    fn test_fmt_digest_is_lowercase_hex() {
        let digest = Digest::of(b"");
        assert_eq!(
            digest.to_string(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_fmt_reload_result_label_values() {
        assert_eq!(ReloadResult::Applied.to_string(), "applied");
        assert_eq!(ReloadResult::Refused.to_string(), "refused");
    }

    #[test]
    fn test_fmt_restart_refusal_names_the_settings() {
        let refusal = ReloadRefusal::Restart(vec!["server.listen", "storage.hedge"]);
        assert_eq!(
            refusal.to_string(),
            "server.listen, storage.hedge changed, which only a restart applies"
        );
    }
}
