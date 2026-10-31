//! The JSON lines the one-shot subcommands print: one object per line, the
//! `kind` first and the `table` second, so a reader routes a line before it
//! parses the rest. A kind names what the line describes, not the command
//! that printed it. The `table` line of `doctor` and of `info` flattens
//! core's `TableSummary`, so a new measurement in core reaches both without
//! a change here.

use std::fmt;
use std::io::Write as _;

use base64::Engine as _;
use bytes::Bytes;
use icetray_core::generation::{Lookup, Row};
use icetray_core::{
    ErrorKind, FileReport, Finding, Key, OrderValue, RefusalReason, ScanLookup, ScanRow,
    SnapshotId, TableSummary, Unwalked,
};
use serde::Serialize;
use serde::ser::SerializeMap as _;

use crate::error::CliError;
use crate::key_text;
use crate::order_text::OrderText;

/// The kind of a line, its first key: the thing the line describes.
/// Serialized as the lowercase name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// One table's summary, from `info` or `doctor`.
    Table,
    /// One finding of a `doctor` run.
    Finding,
    /// One data file of a `doctor --files` run.
    File,
    /// A table the doctor refused.
    Refusal,
    /// A protobuf column whose value page the doctor could not read, so
    /// its values went unchecked (RFD 024).
    Unwalked,
    /// One lookup of a `get` run, as `icetray_lookups_total` counts it.
    Lookup,
    /// One row a `scan` run returned.
    Row,
    /// The Scan of a `scan` run, after its rows, as `icetray_scans_total`
    /// counts it.
    Scan,
}

/// A lookup's outcome as the `lookup` line carries it; the same two values
/// as the wire (RFD 010, RFD 022). Serialized as the lowercase name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Presence {
    /// The key is in the snapshot.
    Present,
    /// The key is not in the snapshot.
    Absent,
}

impl Presence {
    /// The outcome of `row`.
    pub fn of(row: &Row) -> Self {
        match row {
            Row::Present(_) => Self::Present,
            Row::Absent => Self::Absent,
        }
    }
}

/// One value as a `lookup` or `row` line carries it: standard base64 with padding,
/// the text when its bytes are UTF-8, and its length.
#[derive(Debug, Serialize)]
pub struct ValueText {
    base64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    utf8: Option<String>,
    bytes: usize,
}

impl ValueText {
    /// The text of `bytes`.
    fn of(bytes: &[u8]) -> Self {
        Self {
            base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            // intentional: a value that is not UTF-8 has no text form, and
            // the base64 field already carries its bytes.
            utf8: std::str::from_utf8(bytes).ok().map(str::to_string),
            bytes: bytes.len(),
        }
    }
}

/// A present key's values by column name, in the order the columns were
/// named, a null as `None`: serialized as one JSON object whose keys keep
/// that order, a null value as JSON `null`.
#[derive(Debug)]
pub struct Values(Vec<(String, Option<ValueText>)>);

impl Values {
    /// `values`, one per column of `columns`, by name.
    fn of(columns: &[&str], values: &[Option<Bytes>]) -> Self {
        Self(
            columns
                .iter()
                .zip(values)
                .map(|(&column, value)| (column.to_string(), value.as_deref().map(ValueText::of)))
                .collect(),
        )
    }
}

impl Serialize for Values {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (column, value) in &self.0 {
            map.serialize_entry(column, value)?;
        }
        map.end()
    }
}

/// A `table` line, which `info` and `doctor` both print: `kind`, then the
/// summary, whose first field is `table`.
#[derive(Debug, Serialize)]
pub struct SummaryLine<'a> {
    kind: Kind,
    #[serde(flatten)]
    summary: &'a TableSummary,
}

impl<'a> SummaryLine<'a> {
    /// The line for `summary`.
    pub fn new(summary: &'a TableSummary) -> Self {
        Self {
            kind: Kind::Table,
            summary,
        }
    }
}

/// One finding of a `doctor` run, with the finding's own fields flattened.
#[derive(Debug, Serialize)]
pub struct FindingLine<'a> {
    kind: Kind,
    table: &'a str,
    #[serde(flatten)]
    finding: &'a Finding,
}

impl<'a> FindingLine<'a> {
    /// The line for `finding` on `table`.
    pub fn new(table: &'a str, finding: &'a Finding) -> Self {
        Self {
            kind: Kind::Finding,
            table,
            finding,
        }
    }
}

/// One data file of a `doctor --files` run, with the report's fields flattened.
#[derive(Debug, Serialize)]
pub struct FileLine<'a> {
    kind: Kind,
    table: &'a str,
    #[serde(flatten)]
    file: &'a FileReport,
}

impl<'a> FileLine<'a> {
    /// The line for `file` on `table`.
    pub fn new(table: &'a str, file: &'a FileReport) -> Self {
        Self {
            kind: Kind::File,
            table,
            file,
        }
    }
}

/// What a `refusal` line's `reason` carries: the name of the rule the
/// table breaks, or `load_failed` when an error rather than a rule stopped
/// its load. Display and serde give the `snake_case` code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefusalCode {
    /// A refusal reason of RFD 007 or RFD 020, under its own name.
    Rule(RefusalReason),
    /// `load_failed`: the load failed with an error that names no rule.
    LoadFailed,
}

impl RefusalCode {
    /// The code a failure of `kind` refuses a table under, and `kind`
    /// itself when that code is `LoadFailed`, since the code alone does not
    /// say what failed.
    pub fn of(kind: ErrorKind) -> (Self, Option<ErrorKind>) {
        match kind {
            ErrorKind::Refused(reason) => (Self::Rule(reason), None),
            failed @ (ErrorKind::Iceberg
            | ErrorKind::Parquet
            | ErrorKind::ObjectStore
            | ErrorKind::ObjectStorePath
            | ErrorKind::Json
            | ErrorKind::Config
            | ErrorKind::Location
            | ErrorKind::Cache
            | ErrorKind::KeyType
            | ErrorKind::ColumnRequest
            | ErrorKind::ScanRequest
            | ErrorKind::ResponseTooLarge
            | ErrorKind::WrongTableKind
            | ErrorKind::MalformedValue
            | ErrorKind::NoNextBound
            | ErrorKind::Table
            | ErrorKind::File
            | ErrorKind::NotReady
            | ErrorKind::Local
            | ErrorKind::CatalogTimeout
            | ErrorKind::Catalog
            | ErrorKind::LoadDeadline
            | ErrorKind::RefreshDeadline
            | ErrorKind::VerifyCanceled
            | ErrorKind::LookupCanceled) => (Self::LoadFailed, Some(failed)),
        }
    }
}

impl fmt::Display for RefusalCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rule(reason) => reason.fmt(f),
            Self::LoadFailed => f.write_str("load_failed"),
        }
    }
}

impl Serialize for RefusalCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// A table the doctor refused: the refusal code, the error's kind when the
/// code is `load_failed`, and the error's message for a person. Whichever
/// of `reason` and `error_kind` is the more specific is the `reason` label
/// `refresh_errors_total` carries for the same failure.
#[derive(Debug, Serialize)]
pub struct RefusalLine<'a> {
    kind: Kind,
    table: &'a str,
    reason: RefusalCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_kind: Option<ErrorKind>,
    message: String,
}

impl<'a> RefusalLine<'a> {
    /// The line for `error`, which refused `table`.
    pub fn new(table: &'a str, error: &icetray_core::Error) -> Self {
        let (reason, error_kind) = RefusalCode::of(error.kind());
        Self {
            kind: Kind::Refusal,
            table,
            reason,
            error_kind,
            message: error.to_string(),
        }
    }
}

/// A protobuf column the doctor's walk could not read (`Unwalked`), an
/// `unwalked` line: the column, the error's kind, and its message for a
/// person. It refuses nothing: the table's other lines stand.
#[derive(Debug, Serialize)]
pub struct UnwalkedLine<'a> {
    kind: Kind,
    table: &'a str,
    column: &'a str,
    error_kind: ErrorKind,
    message: String,
}

impl<'a> UnwalkedLine<'a> {
    /// The line for `unwalked`, a column of `table`.
    pub fn new(table: &'a str, unwalked: &'a Unwalked) -> Self {
        Self {
            kind: Kind::Unwalked,
            table,
            column: &unwalked.column,
            error_kind: unwalked.error.kind(),
            message: unwalked.error.to_string(),
        }
    }
}

/// One lookup of a `get` run, a `lookup` line: the key as the text `get`
/// takes for it (`key_text::text`), so a `key` passed back to `get` looks
/// up the same key, the outcome, for a present key its `values` by column
/// name in the order named (`Values`), the snapshot answered from, the GETs
/// the lookup issued, and the time it took. `values` is absent for an
/// absent key, and empty for a present one when no column was named.
#[derive(Debug, Serialize)]
pub struct LookupLine<'a> {
    kind: Kind,
    table: &'a str,
    key: String,
    presence: Presence,
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<Values>,
    snapshot_id: SnapshotId,
    gets: u64,
    elapsed_micros: u64,
}

impl<'a> LookupLine<'a> {
    /// The line for `lookup` of `key` on `table`, which named `columns`,
    /// issued `gets` GETs, and took `elapsed_micros`.
    pub fn new(
        table: &'a str,
        key: &Key,
        columns: &[&str],
        lookup: &Lookup,
        gets: u64,
        elapsed_micros: u64,
    ) -> Self {
        let values = match &lookup.row {
            Row::Present(values) => Some(Values::of(columns, values)),
            Row::Absent => None,
        };
        Self {
            kind: Kind::Lookup,
            table,
            key: key_text::text(key),
            presence: Presence::of(&lookup.row),
            values,
            snapshot_id: lookup.snapshot_id,
            gets,
            elapsed_micros,
        }
    }
}

/// One row of a `scan` run, a `row` line: the key as `lookup` carries it,
/// the row's order value as the doctor's `file` lines carry one, and its
/// `values` by column name in the order named, as a `lookup` line's;
/// `{}` when no column was named.
#[derive(Debug, Serialize)]
pub struct RowLine<'a> {
    kind: Kind,
    table: &'a str,
    key: String,
    order: &'a OrderValue,
    values: Values,
}

impl<'a> RowLine<'a> {
    /// The line for `row` of `key` on `table`, whose Scan named `columns`.
    pub fn new(table: &'a str, key: &Key, columns: &[&str], row: &'a ScanRow) -> Self {
        Self {
            kind: Kind::Row,
            table,
            key: key_text::text(key),
            order: &row.order,
            values: Values::of(columns, &row.values),
        }
    }
}

/// The Scan of a `scan` run, a `scan` line after its `row` lines: the key,
/// the rows returned, `next` when live rows remain past them, in the form
/// `--start` and `--before` take (`OrderText`), the snapshot answered from,
/// the GETs the Scan issued, and the time it took.
#[derive(Debug, Serialize)]
pub struct ScanLine<'a> {
    kind: Kind,
    table: &'a str,
    key: String,
    rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<OrderText<'a>>,
    snapshot_id: SnapshotId,
    gets: u64,
    elapsed_micros: u64,
}

impl<'a> ScanLine<'a> {
    /// The line for `lookup`, a Scan of `key` on `table` that issued `gets`
    /// GETs and took `elapsed_micros`.
    pub fn new(
        table: &'a str,
        key: &Key,
        lookup: &'a ScanLookup,
        gets: u64,
        elapsed_micros: u64,
    ) -> Self {
        Self {
            kind: Kind::Scan,
            table,
            key: key_text::text(key),
            rows: lookup.scanned.rows.len(),
            next: lookup.scanned.next.as_ref().map(OrderText),
            snapshot_id: lookup.snapshot_id,
            gets,
            elapsed_micros,
        }
    }
}

/// The greatest magnitude a JSON number holds exactly in a reader that
/// parses numbers as doubles: 2^53, where a double's significand ends. A
/// long order value past it is exact in the text and rounded in such a
/// reader.
pub const MAX_EXACT_JSON_INTEGER: u64 = 1 << f64::MANTISSA_DIGITS;

/// Whether `order` is written as a JSON number a double-parsing reader
/// rounds: a long whose magnitude exceeds `MAX_EXACT_JSON_INTEGER`. A string
/// or hex value is text and never rounds.
fn loses_precision_as_json(order: &OrderValue) -> bool {
    match order {
        OrderValue::Long(value) => value.unsigned_abs() > MAX_EXACT_JSON_INTEGER,
        OrderValue::Str(_) | OrderValue::Bytes(_) => false,
    }
}

/// The order values `files`' `file` lines carry, each file's least and
/// greatest. Of the lines of `doctor` and `info`, only a `file` line
/// carries one.
pub fn file_orders(files: &[FileReport]) -> impl Iterator<Item = &OrderValue> {
    files
        .iter()
        .filter_map(|file| file.order.as_ref())
        .flat_map(|order| [&order.min, &order.max])
}

/// The order values a `scan` run's lines carry: each `row` line's, and the
/// `scan` line's `next`.
pub fn scan_orders(lookup: &ScanLookup) -> impl Iterator<Item = &OrderValue> {
    let scanned = &lookup.scanned;
    scanned
        .rows
        .iter()
        .map(|row| &row.order)
        .chain(scanned.next.as_ref())
}

/// Writes the one warning of `table` on stderr, through the log stream, when
/// `orders`, the order values of the lines a run printed for it, hold a
/// long past 2^53 (`loses_precision_as_json`). A run calls it once per
/// table, after printing the table's lines, so each affected table is
/// named once. It changes nothing on stdout.
pub fn warn_if_inexact_order<'a>(table: &str, orders: impl IntoIterator<Item = &'a OrderValue>) {
    if orders.into_iter().any(loses_precision_as_json) {
        tracing::warn!(
            table,
            max_exact = MAX_EXACT_JSON_INTEGER,
            "lines carry order values past 2^53, which are exact as JSON text but lose precision in readers that parse numbers as doubles"
        );
    }
}

/// Writes one JSON object and a newline to stdout. A reader that closed the
/// pipe is `CliError::BrokenPipe`, which ends the run with exit 0 and
/// nothing more printed; any other write failure is `CliError::Io`.
pub fn emit<T: Serialize>(line: &T) -> Result<(), CliError> {
    let json = serde_json::to_string(line)?;
    let mut stdout = std::io::stdout().lock();
    match writeln!(stdout, "{json}") {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Err(CliError::BrokenPipe),
        Err(error) => Err(CliError::Io(error)),
    }
}

/// The keys of a JSON object in the order the text carries them, duplicates
/// included: what a test reads when the order or the uniqueness of keys is
/// the claim, since `serde_json::Value` keeps neither.
#[cfg(test)]
pub(crate) struct KeyOrder(pub(crate) Vec<String>);

#[cfg(test)]
impl<'de> serde::Deserialize<'de> for KeyOrder {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{IgnoredAny, MapAccess, Visitor};

        struct KeysVisitor;

        impl<'de> Visitor<'de> for KeysVisitor {
            type Value = KeyOrder;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<KeyOrder, A::Error> {
                let mut keys = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    map.next_value::<IgnoredAny>()?;
                    keys.push(key);
                }
                Ok(KeyOrder(keys))
            }
        }

        deserializer.deserialize_map(KeysVisitor)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::collections::BTreeMap;

    use icetray_core::{
        Capping, ColumnSummary, DeleteSummary, FileOrderReport, FindingCode, KeyType, Level,
        LookupCost, OrderSummary, OrderType, OrderValue, Residency, ResidentBytes, TableFault,
        UnsupportedFormat, ValueColumnReport,
    };
    use serde_json::Value as Json;

    use super::*;

    /// The table every sample line names.
    const TABLE: &str = "kv";

    /// The keys every line leads with: `kind`, then `table`.
    const LEADING_KEYS: usize = 2;

    /// The snapshot every sample line names.
    const SNAPSHOT: i64 = 42;

    /// The value column every sample names, and a second one.
    const COLUMN: &str = "payload";
    const OTHER_COLUMN: &str = "detail";

    /// A value whose bytes are not UTF-8.
    const BINARY_VALUE: [u8; 3] = [0xff, 0xfe, 0x00];

    /// A value whose bytes are UTF-8.
    const TEXT_VALUE: &str = "hello";

    /// Standard base64 with padding of `TEXT_VALUE`.
    const TEXT_VALUE_BASE64: &str = "aGVsbG8=";

    /// The GETs the sample `lookup` line carries: one cold hit.
    const GETS: u64 = 1;

    /// The microseconds the sample `lookup` line carries.
    const ELAPSED_MICROS: u64 = 250;

    /// The first `LEADING_KEYS` keys of `line`'s JSON.
    fn leading_keys<T: Serialize>(line: &T) -> Vec<String> {
        let json = serde_json::to_string(line).unwrap();
        let KeyOrder(keys) = serde_json::from_str(&json).unwrap();
        keys.into_iter().take(LEADING_KEYS).collect()
    }

    fn summary() -> TableSummary {
        TableSummary {
            table: TABLE.to_string(),
            snapshot_id: SnapshotId(SNAPSHOT),
            level: Level::Optimal,
            lookups: Level::Optimal,
            residency: Residency::Full,
            file_count: 1,
            buckets: Some(1),
            buckets_with_files: 1,
            max_files_per_bucket: 1,
            records: 1,
            index_bytes: 1,
            predicted: ResidentBytes {
                full: 1,
                disk_memory: 1,
                disk_bytes: 1,
                bloom: None,
                bloom_bits_per_key: None,
                key_chunk_bytes: 1,
                filter_bytes: None,
            },
            level_by_residency: BTreeMap::from([
                (Residency::Full, Level::Optimal),
                (Residency::Disk, Level::Optimal),
                (Residency::Bloom, Level::Balanced),
            ]),
            lookup_cost: Some(LookupCost {
                gets_per_hit: 1,
                gets_per_miss: 0,
                files_probed: 1,
            }),
            value_columns: vec![ColumnSummary {
                column: COLUMN.to_string(),
                format: icetray_core::ValueFormat::Protobuf,
                level: Level::Optimal,
                gets_per_hit: 1,
                files_predating_column: 0,
                value_bytes: 1,
            }],
            deletes: DeleteSummary {
                rows: 1,
                files: 1,
                fraction: 1.0,
                state_bytes: 1,
            },
            capping: 0,
            advice: 0,
            order: None,
        }
    }

    fn finding() -> Finding {
        Finding::advice(FindingCode::LargeFile, None, "sample".to_string(), None)
    }

    fn file_report() -> FileReport {
        FileReport {
            path: "mem://f0.parquet".to_string(),
            bucket: 0,
            key_type: KeyType::Long,
            rows: 1,
            row_groups: 1,
            key_pages: 1,
            max_key_page_rows: 1,
            max_key_bytes: 1,
            value_columns: vec![ValueColumnReport {
                column: COLUMN.to_string(),
                predates_column: false,
                value_pages: 1,
                avg_value_page_bytes: 1,
                avg_value_bytes: 1,
                pages_aligned: true,
            }],
            dictionary_columns: Vec::new(),
            bloom_bytes: None,
            bloom_bits_per_key: None,
            key_chunk_bytes: 1,
            distinct_page_mins_ratio: 1.0,
            bounds_present: true,
            resident_bytes: 1,
            order: None,
        }
    }

    fn lookup(row: Row) -> Lookup {
        Lookup {
            row,
            snapshot_id: SnapshotId(SNAPSHOT),
        }
    }

    /// A present key whose one value is `bytes`.
    fn present(bytes: &'static [u8]) -> Lookup {
        lookup(Row::Present(vec![Some(Bytes::from_static(bytes))]))
    }

    #[test]
    fn test_serialize_every_line_puts_kind_then_table_first() {
        let summary = summary();
        let finding = finding();
        let file = file_report();
        let error = icetray_core::Error::Table {
            table: TABLE.to_string(),
            fault: TableFault::MetadataLocationUnknown,
        };
        let present = present(TEXT_VALUE.as_bytes());
        let key = Key::Long(1);
        assert_eq!(leading_keys(&SummaryLine::new(&summary)), ["kind", "table"]);
        assert_eq!(
            leading_keys(&FindingLine::new(TABLE, &finding)),
            ["kind", "table"]
        );
        assert_eq!(
            leading_keys(&FileLine::new(TABLE, &file)),
            ["kind", "table"]
        );
        assert_eq!(
            leading_keys(&RefusalLine::new(TABLE, &error)),
            ["kind", "table"]
        );
        let unwalked = icetray_core::Unwalked {
            column: COLUMN.to_string(),
            error,
        };
        assert_eq!(
            leading_keys(&UnwalkedLine::new(TABLE, &unwalked)),
            ["kind", "table"]
        );
        assert_eq!(
            leading_keys(&LookupLine::new(
                TABLE,
                &key,
                &[COLUMN],
                &present,
                GETS,
                ELAPSED_MICROS
            )),
            ["kind", "table"]
        );
    }

    #[test]
    fn test_serialize_summary_line_is_a_table_line_carrying_the_summary() {
        let summary = summary();
        let line = serde_json::to_value(SummaryLine::new(&summary)).unwrap();
        assert_eq!(line["kind"], Json::String("table".to_string()));
        assert_eq!(line["table"], Json::String(TABLE.to_string()));
        assert_eq!(line["snapshot_id"], Json::String(SNAPSHOT.to_string()));
        assert_eq!(line["level"], Json::String("optimal".to_string()));
        assert_eq!(line["lookups"], Json::String("optimal".to_string()));
        assert_eq!(
            line["level_by_residency"]["bloom"],
            Json::String("balanced".to_string())
        );
        assert_eq!(line["capping"], Json::from(0));
        assert_eq!(
            line["value_columns"][0]["format"],
            Json::String("protobuf".to_string()),
            "each column's format, as the file spells it"
        );
        assert_eq!(
            line["lookup_cost"],
            serde_json::json!({"gets_per_hit": 1, "gets_per_miss": 0, "files_probed": 1})
        );
        assert_eq!(
            line["deletes"],
            serde_json::json!({"rows": 1, "files": 1, "fraction": 1.0, "state_bytes": 1})
        );
    }

    #[test]
    fn test_serialize_finding_and_file_lines_flatten_their_report() {
        let finding = finding();
        let line = serde_json::to_value(FindingLine::new(TABLE, &finding)).unwrap();
        assert_eq!(line["kind"], Json::String("finding".to_string()));
        assert_eq!(
            line["code"],
            Json::String(FindingCode::LargeFile.to_string())
        );
        assert_eq!(line["caps"], Json::Null, "advice caps nothing");
        assert_eq!(line["column"], Json::Null, "a finding about the table");
        let file = file_report();
        let line = serde_json::to_value(FileLine::new(TABLE, &file)).unwrap();
        assert_eq!(line["kind"], Json::String("file".to_string()));
        assert_eq!(line["path"], Json::String(file.path.clone()));
        assert_eq!(
            line["value_columns"][0]["column"],
            Json::String(COLUMN.to_string())
        );
    }

    /// The order facts an ordered table's sample lines carry.
    const LONGEST_RUN: u64 = 7;
    const SCAN_ROWS: u32 = 50;
    const MULTI_SCAN_VALUES: usize = 400;
    const SCAN_BYTES: usize = 2048;
    const GETS_PER_SCAN: u64 = 3;
    const FILES_PROBED: usize = 2;
    const DISTINCT_KEYS: [u64; 2] = [3, 4];
    const LEAST_ORDER: i64 = -5;

    #[test]
    fn test_serialize_order_facts_appear_only_on_an_ordered_table_s_lines() {
        let mut summary = summary();
        let mut file = file_report();
        let line = serde_json::to_value(SummaryLine::new(&summary)).unwrap();
        assert!(line.get("order").is_none(), "{line}");
        assert!(line.get("lookup_cost").is_some(), "{line}");
        let line = serde_json::to_value(FileLine::new(TABLE, &file)).unwrap();
        assert!(line.get("order").is_none(), "{line}");
        summary.lookup_cost = None;
        summary.order = Some(OrderSummary {
            column: COLUMN.to_string(),
            order_type: OrderType::Binary,
            max_scan_rows: SCAN_ROWS,
            max_multi_scan_values: MULTI_SCAN_VALUES,
            max_scan_bytes: SCAN_BYTES,
            longest_run: LONGEST_RUN,
            gets_per_scan: GETS_PER_SCAN,
            files_probed: FILES_PROBED,
        });
        file.order = Some(FileOrderReport {
            min: OrderValue::Long(LEAST_ORDER),
            max: OrderValue::Bytes(vec![0x00, 0xab]),
            longest_run: LONGEST_RUN,
            distinct_keys: DISTINCT_KEYS.to_vec(),
        });
        let line = serde_json::to_value(SummaryLine::new(&summary)).unwrap();
        assert_eq!(line["order"]["column"], Json::String(COLUMN.to_string()));
        assert_eq!(line["order"]["order_type"], Json::String("binary".into()));
        assert_eq!(line["order"]["max_scan_rows"], Json::from(SCAN_ROWS));
        assert_eq!(
            line["order"]["max_multi_scan_values"],
            Json::from(MULTI_SCAN_VALUES)
        );
        assert_eq!(line["order"]["max_scan_bytes"], Json::from(SCAN_BYTES));
        assert_eq!(line["order"]["longest_run"], Json::from(LONGEST_RUN));
        assert_eq!(line["order"]["gets_per_scan"], Json::from(GETS_PER_SCAN));
        assert_eq!(line["order"]["files_probed"], Json::from(FILES_PROBED));
        assert!(
            line.get("lookup_cost").is_none(),
            "an ordered table's line carries a Scan's cost, not a lookup's: {line}"
        );
        let line = serde_json::to_value(FileLine::new(TABLE, &file)).unwrap();
        assert_eq!(line["order"]["min"], Json::from(LEAST_ORDER));
        assert_eq!(line["order"]["max"], Json::String("0x00ab".to_string()));
        assert_eq!(line["order"]["longest_run"], Json::from(LONGEST_RUN));
        assert_eq!(line["order"]["distinct_keys"], Json::from(DISTINCT_KEYS));
    }

    /// `MAX_EXACT_JSON_INTEGER` as a signed value.
    const EXACT_LONG: i64 = MAX_EXACT_JSON_INTEGER.cast_signed();

    /// A file whose order column holds `min` through `max`.
    fn ordered_file(min: OrderValue, max: OrderValue) -> FileReport {
        let mut file = file_report();
        file.order = Some(FileOrderReport {
            min,
            max,
            longest_run: LONGEST_RUN,
            distinct_keys: DISTINCT_KEYS.to_vec(),
        });
        file
    }

    /// Whether `files`' order values hold one a double-parsing reader
    /// rounds.
    fn carries_inexact_order(files: &[FileReport]) -> bool {
        file_orders(files).any(loses_precision_as_json)
    }

    #[test]
    fn test_carries_inexact_order_is_false_at_2_pow_53_and_true_one_past_it() {
        let at = |min, max| carries_inexact_order(&[ordered_file(min, max)]);
        let long = OrderValue::Long;
        assert!(!at(long(EXACT_LONG), long(EXACT_LONG)));
        assert!(!at(long(-EXACT_LONG), long(-EXACT_LONG)));
        assert!(at(long(0), long(EXACT_LONG + 1)));
        assert!(at(long(-EXACT_LONG - 1), long(0)));
        assert!(at(long(i64::MIN), long(i64::MAX)));
    }

    #[test]
    fn test_carries_inexact_order_ignores_text_values_and_files_without_an_order() {
        let text = ordered_file(
            OrderValue::Str("9".repeat(32)),
            OrderValue::Bytes(vec![u8::MAX; 16]),
        );
        assert!(!carries_inexact_order(&[file_report(), text]));
        assert!(!carries_inexact_order(&[]));
    }

    #[test]
    fn test_carries_inexact_order_finds_one_file_among_many() {
        let exact = ordered_file(OrderValue::Long(0), OrderValue::Long(EXACT_LONG));
        let past = ordered_file(OrderValue::Long(0), OrderValue::Long(EXACT_LONG + 1));
        assert!(carries_inexact_order(&[exact.clone(), past, exact]));
    }

    #[test]
    fn test_serialize_file_line_writes_an_inexact_order_as_its_exact_digits() {
        let past = EXACT_LONG + 1;
        let file = ordered_file(OrderValue::Long(-past), OrderValue::Long(past));
        let text = serde_json::to_string(&FileLine::new(TABLE, &file)).unwrap();
        // Parsed back, an integer stays an exact `i64`, and a float or a
        // string in its place compares unequal to one.
        let line: Json = serde_json::from_str(&text).unwrap();
        assert_eq!(line["order"]["min"], Json::from(-past), "{text}");
        assert_eq!(line["order"]["max"], Json::from(past), "{text}");
    }

    #[test]
    fn test_serialize_refusal_line_rule_is_its_reason_with_no_error_kind() {
        let refused = icetray_core::Error::Refused {
            table: TABLE.to_string(),
            reason: RefusalReason::EqualityDeletes {
                path: TABLE.to_string(),
            },
        };
        let below_level = icetray_core::Error::Refused {
            table: TABLE.to_string(),
            reason: RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Permissive,
                columns: Vec::new(),
                findings: vec![Capping {
                    code: FindingCode::NoBucketPartition,
                    column: None,
                }],
            },
        };
        let data_format = icetray_core::Error::Refused {
            table: TABLE.to_string(),
            reason: RefusalReason::DataFormatUnsupported {
                path: TABLE.to_string(),
                format: UnsupportedFormat::Orc,
            },
        };
        for (error, code) in [
            (refused, "equality_deletes"),
            (data_format, "data_format_unsupported"),
            (below_level, "below_level"),
        ] {
            let line = serde_json::to_value(RefusalLine::new(TABLE, &error)).unwrap();
            assert_eq!(line["kind"], Json::String("refusal".to_string()));
            assert_eq!(line["reason"], Json::String(code.to_string()));
            assert!(line.get("error_kind").is_none(), "{line}");
            assert!(line["message"].is_string());
        }
    }

    #[test]
    fn test_serialize_refusal_line_error_is_load_failed_with_its_kind() {
        let table = icetray_core::Error::Table {
            table: TABLE.to_string(),
            fault: TableFault::MetadataLocationUnknown,
        };
        let deadline = icetray_core::Error::LoadDeadline {
            table: TABLE.to_string(),
            after: std::time::Duration::ZERO,
        };
        for (error, kind) in [(table, "table"), (deadline, "load_deadline")] {
            let line = serde_json::to_value(RefusalLine::new(TABLE, &error)).unwrap();
            assert_eq!(line["reason"], Json::String("load_failed".to_string()));
            assert_eq!(line["error_kind"], Json::String(kind.to_string()));
            assert!(line["message"].is_string());
        }
    }

    #[test]
    fn test_serialize_unwalked_line_names_the_column_and_the_error_s_kind() {
        let unwalked = icetray_core::Unwalked {
            column: COLUMN.to_string(),
            error: icetray_core::Error::Table {
                table: TABLE.to_string(),
                fault: TableFault::MetadataLocationUnknown,
            },
        };
        let line = serde_json::to_value(UnwalkedLine::new(TABLE, &unwalked)).unwrap();
        assert_eq!(line["kind"], Json::String("unwalked".to_string()));
        assert_eq!(line["column"], Json::String(COLUMN.to_string()));
        assert_eq!(line["error_kind"], Json::String("table".to_string()));
        assert!(line["message"].is_string());
    }

    #[test]
    fn test_serialize_lookup_line_present_values_by_column_in_order_as_base64_and_utf8() {
        let key = Key::Long(1);
        let both = lookup(Row::Present(vec![
            Some(Bytes::from_static(TEXT_VALUE.as_bytes())),
            None,
        ]));
        let text = serde_json::to_string(&LookupLine::new(
            TABLE,
            &key,
            &[OTHER_COLUMN, COLUMN],
            &both,
            GETS,
            ELAPSED_MICROS,
        ))
        .unwrap();
        let line: Json = serde_json::from_str(&text).unwrap();
        assert_eq!(line["kind"], Json::String("lookup".to_string()));
        assert_eq!(line["key"], Json::String("1".to_string()));
        assert_eq!(line["presence"], Json::String("present".to_string()));
        assert_eq!(
            line["values"][OTHER_COLUMN],
            serde_json::json!({
                "base64": TEXT_VALUE_BASE64,
                "utf8": TEXT_VALUE,
                "bytes": TEXT_VALUE.len(),
            })
        );
        assert_eq!(line["values"][COLUMN], Json::Null, "a null is null");
        let values = &text[text.find("\"values\"").unwrap()..];
        let KeyOrder(order) =
            serde_json::Deserializer::from_str(&values[values.find('{').unwrap()..])
                .into_iter::<KeyOrder>()
                .next()
                .unwrap()
                .unwrap();
        assert_eq!(order, [OTHER_COLUMN, COLUMN], "in the order named");
        assert_eq!(line["snapshot_id"], Json::String(SNAPSHOT.to_string()));
        assert_eq!(line["gets"], Json::from(GETS));
        assert_eq!(line["elapsed_micros"], Json::from(ELAPSED_MICROS));
    }

    /// A string key with a space, both quote characters, a backslash, and
    /// non-ASCII text.
    const AWKWARD_STRING: &str = "a \"quoted\" 'key' with a \\ in caf\u{e9} \u{1f9ca}";

    #[test]
    fn test_serialize_lookup_line_key_parses_back_to_the_key_for_every_type() {
        let keys = [
            Key::Long(-1),
            Key::Str(AWKWARD_STRING.to_string()),
            Key::Bytes(BINARY_VALUE.to_vec()),
            Key::Uuid(*b"0123456789abcdef"),
        ];
        let absent = lookup(Row::Absent);
        for key in keys {
            let line = serde_json::to_string(&LookupLine::new(
                TABLE,
                &key,
                &[COLUMN],
                &absent,
                0,
                ELAPSED_MICROS,
            ))
            .unwrap();
            let line: Json = serde_json::from_str(&line).unwrap();
            let Json::String(printed) = &line["key"] else {
                panic!("{key}: the key is a JSON string in {line}");
            };
            assert_eq!(
                key_text::parse(printed, key.key_type()),
                Ok(key.clone()),
                "{line}"
            );
        }
    }

    #[test]
    fn test_serialize_lookup_line_string_key_is_the_string_itself() {
        let key = Key::Str(AWKWARD_STRING.to_string());
        let line = serde_json::to_value(LookupLine::new(
            TABLE,
            &key,
            &[COLUMN],
            &lookup(Row::Absent),
            0,
            ELAPSED_MICROS,
        ))
        .unwrap();
        assert_eq!(line["key"], Json::String(AWKWARD_STRING.to_string()));
    }

    #[test]
    fn test_serialize_lookup_line_non_utf8_value_has_no_utf8_field() {
        let key = Key::Long(1);
        let present = present(&BINARY_VALUE);
        let line = serde_json::to_value(LookupLine::new(
            TABLE,
            &key,
            &[COLUMN],
            &present,
            GETS,
            ELAPSED_MICROS,
        ))
        .unwrap();
        let value = &line["values"][COLUMN];
        assert!(value["base64"].is_string());
        assert!(value.get("utf8").is_none());
        assert_eq!(value["bytes"], Json::from(BINARY_VALUE.len()));
    }

    #[test]
    fn test_serialize_lookup_line_absent_carries_no_values_and_presence_only_empty_ones() {
        let key = Key::Long(1);
        let absent = serde_json::to_value(LookupLine::new(
            TABLE,
            &key,
            &[COLUMN],
            &lookup(Row::Absent),
            0,
            ELAPSED_MICROS,
        ))
        .unwrap();
        assert_eq!(absent["presence"], Json::String("absent".to_string()));
        assert!(absent.get("values").is_none());
        let presence_only = serde_json::to_value(LookupLine::new(
            TABLE,
            &key,
            &[],
            &lookup(Row::Present(Vec::new())),
            0,
            ELAPSED_MICROS,
        ))
        .unwrap();
        assert_eq!(
            presence_only["presence"],
            Json::String("present".to_string())
        );
        assert_eq!(presence_only["values"], serde_json::json!({}));
    }

    #[test]
    fn test_of_maps_every_row() {
        assert_eq!(
            Presence::of(&Row::Present(vec![Some(Bytes::new())])),
            Presence::Present
        );
        assert_eq!(Presence::of(&Row::Present(Vec::new())), Presence::Present);
        assert_eq!(Presence::of(&Row::Absent), Presence::Absent);
    }
}
