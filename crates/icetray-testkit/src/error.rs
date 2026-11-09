//! The one error type of the testkit: every library failure a fixture or
//! an assertion can meet, one variant per source, plus the five faults
//! this crate raises itself: a fixture specification the writer refuses, a
//! helper called in a state it cannot measure, a truth-file line that
//! cannot be read, a snapshot the merge-on-read oracle cannot answer
//! for, and a workload that cannot be read or placed. A test matches the
//! variant; the message is for a person reading the failure.

use std::env::VarError;

use icetray_core::config::{Residency, RewriteTextFault};
use icetray_core::{Key, KeyType, OrderValue};

use crate::golden::{REWRITE_VAR, WAREHOUSE_VAR};
use crate::ordered::OrderKind;
use crate::truth::TruthFault;
use crate::workload::WorkloadFault;

/// The environment variable that names the REST catalog the container-gated
/// test writes through (`dev/test-int.sh` exports it).
pub const REST_URI_VAR: &str = "ICETRAY_REST_URI";

/// Every failure the testkit can observe.
#[derive(Debug, thiserror::Error)]
pub enum TestkitError {
    /// A failure inside `icetray-core`: a refusal, a fault, or a library
    /// error it wrapped. The refusal tests match on it.
    #[error("core: {0}")]
    Core(#[from] icetray_core::Error),
    /// A failure inside the Iceberg library: the writer, a commit, or a
    /// catalog.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// A failure inside the Parquet writer.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// A failure building an arrow array or batch.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A filesystem or socket failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Metadata that could not be written or read as JSON.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// A failure building or using an `object_store` client.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// The in-process S3 server could not open its root directory.
    /// `s3s_fs::Error` implements neither `Display` nor `std::error::Error`,
    /// so it is held whole and rendered through `Debug`.
    #[error("s3 server: {0:?}")]
    S3Server(s3s_fs::Error),
    /// A fixture specification the writer refuses.
    #[error("fixture specification: {0}")]
    Spec(#[from] SpecFault),
    /// A helper called in a state it cannot measure.
    #[error("precondition: {0}")]
    Precondition(#[from] Precondition),
    /// A truth-file line that cannot be read.
    #[error("truth file: {0}")]
    Truth(#[from] TruthFault),
    /// The merge-on-read oracle found a snapshot it cannot answer for.
    #[error("merge oracle: {0}")]
    Oracle(#[from] OracleFault),
    /// A workload that cannot be read or placed.
    #[error("workload: {0}")]
    Workload(#[from] WorkloadFault),
}

impl From<s3s_fs::Error> for TestkitError {
    fn from(error: s3s_fs::Error) -> Self {
        Self::S3Server(error)
    }
}

/// What makes a fixture specification unwritable; closed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SpecFault {
    /// `key_pad` lengthens string and binary keys; a long or uuid key has a
    /// fixed width and cannot take one.
    #[error("key_pad applies to string and binary keys, not {key_type}")]
    PadOnFixedWidthKey {
        /// The fixed-width key type the specification named.
        key_type: KeyType,
    },
    /// `KeyShape::Scattered` spells string keys; a key of another type has
    /// no scattered spelling.
    #[error("a scattered key shape applies to string keys, not {key_type}")]
    ScatteredNonStringKey {
        /// The key type the specification named.
        key_type: KeyType,
    },
    /// An id a scattered fixture was asked to write has no scattered key:
    /// it is at or above `keys::SCATTERED_ID_SPACE`.
    #[error("id {id} is outside the scattered key space of {space} ids", space = crate::keys::SCATTERED_ID_SPACE)]
    IdOutsideScatteredSpace {
        /// The first such id.
        id: u64,
    },
    /// The fixture has committed a snapshot by hand, so its catalog record
    /// is behind its root and a commit through the catalog would fork the
    /// table; only `commit_by_hand` may advance it from then on.
    #[error("the table has committed by hand and its catalog record is behind")]
    CatalogBehind,
    /// A delete or update named an id whose key has no live row.
    #[error("id {id} has no live row to delete")]
    IdNotLive {
        /// The id.
        id: u64,
    },
    /// A data file the current snapshot does not list.
    #[error("data file {path} is not listed")]
    FileNotListed {
        /// The file's location.
        path: String,
    },
    /// `Fixture::evolve` renamed or dropped a column the current schema
    /// does not hold.
    #[error("column {name} is not in the current schema")]
    ColumnNotInSchema {
        /// The column's name.
        name: String,
    },
    /// `move_ref` named a snapshot this fixture did not commit.
    #[error("snapshot {snapshot_id} was not committed by this fixture")]
    UnknownSnapshot {
        /// The snapshot id.
        snapshot_id: i64,
    },
    /// A deletion vector longer than its four-byte length prefix can say.
    #[error("a deletion vector of {bytes} bytes is past its length prefix's 4 GiB")]
    VectorTooLong {
        /// The bytes of the magic and the vector.
        bytes: usize,
    },
    /// A value a delete file's manifest entry or row records as a long is
    /// past the largest long.
    #[error("{field} {value} is past the largest long")]
    PastLong {
        /// What the value is.
        field: LongField,
        /// The value.
        value: u64,
    },
    /// A delete file entry the iceberg builder refuses for a missing
    /// required field.
    #[error("the manifest entry of delete file {path} is incomplete")]
    DeleteEntryIncomplete {
        /// The delete file's location.
        path: String,
    },
    /// A raw order value the order column's type cannot hold.
    #[error("{kind:?} cannot hold the order value {raw}")]
    OrderUnencodable {
        /// The order column's type.
        kind: OrderKind,
        /// The raw value.
        raw: i64,
    },
    /// An id's order values `start + step * j` run past the largest long.
    #[error("the order values of id {id} are past the largest long")]
    OrderOverflow {
        /// The id.
        id: u64,
    },
    /// A delete or update named a row that is not live.
    #[error("id {id} has no live row at order {order}")]
    RowNotLive {
        /// The id.
        id: u64,
        /// The raw order value.
        order: i64,
    },
    /// A call that addresses rows by order value on a fixture whose
    /// specification names no order column.
    #[error("the fixture has no order column")]
    NotOrdered,
}

/// What a value `SpecFault::PastLong` names is; closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LongField {
    /// A deletion vector's offset in its Puffin file.
    Offset,
    /// A deletion vector's length.
    Length,
    /// A position a position delete file deletes.
    Position,
}

impl std::fmt::Display for LongField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Offset => "offset",
            Self::Length => "length",
            Self::Position => "position",
        })
    }
}

/// A helper asked to measure what it cannot, from the state it was given;
/// closed. A gated test forced without its environment fails on one of
/// these rather than passing without doing its work (RFD 014).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Precondition {
    /// The cost oracle names value pages from resident key pages, which
    /// only a Full-tier generation holds.
    #[error("the cost oracle needs a full-tier generation, got {residency}")]
    OracleNotFull {
        /// The tier the generation was loaded in.
        residency: Residency,
    },
    /// A cold-cost assertion was given a cache that has already served.
    #[error("the page cache is not cold: {hits} hits and {fetches} fetches")]
    CacheNotCold {
        /// Lookups the cache had already served without a fetch.
        hits: u64,
        /// Fetches the cache had already run.
        fetches: u64,
    },
    /// The golden warehouse is not named in the environment.
    #[error("{WAREHOUSE_VAR} is not set")]
    GoldenUnset,
    /// The golden warehouse's rewrite rules hold one that is not `FROM=TO`.
    #[error("{REWRITE_VAR} holds {rule:?}, which is not a rewrite rule: {fault}")]
    GoldenRewrite {
        /// The rule as the variable holds it.
        rule: String,
        /// Why it is not one.
        fault: RewriteTextFault,
    },
    /// A reference table records no key count under the property the
    /// recipe writes, so its rows cannot be derived.
    #[error(
        "{table} records no key count under {property}; regenerate it with dev/golden-tables.py --only reference",
        property = crate::golden::REFERENCE_KEYS_PROPERTY
    )]
    ReferenceKeysUnrecorded {
        /// The table.
        table: String,
    },
    /// The REST catalog is not named in the environment.
    #[error("{REST_URI_VAR} is not set")]
    RestUriUnset,
}

/// What the merge-on-read oracle (`merge_oracle`) finds in a snapshot that
/// leaves a key without one answer; closed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OracleFault {
    /// Two live rows hold one key, or in an ordered table one key and
    /// order value.
    #[error("key {key} at order {order:?} is live in {first} and {second}")]
    LiveDuplicate {
        /// The key.
        key: Key,
        /// The order value the two rows share; `None` in a key-value
        /// table.
        order: Option<OrderValue>,
        /// The data file of one live copy.
        first: String,
        /// The data file of another.
        second: String,
    },
    /// Two deletion vectors apply to one data file.
    #[error("two deletion vectors apply to {data_file}")]
    TwoVectors {
        /// The data file's location.
        data_file: String,
    },
    /// A deletion vector whose blob does not decode or contradicts its
    /// manifest entry.
    #[error("deletion vector in {path} is malformed")]
    MalformedVector {
        /// The Puffin file's location.
        path: String,
    },
    /// A position delete file names a negative position.
    #[error("position delete file {path} names a negative position")]
    NegativePosition {
        /// The delete file's location.
        path: String,
    },
    /// A live equality delete, which the oracle does not apply.
    #[error("equality delete {path} is live")]
    EqualityDelete {
        /// The delete file's location.
        path: String,
    },
    /// A live entry whose data sequence number iceberg could not inherit.
    #[error("file {path} has no data sequence number")]
    NoSequence {
        /// The file's location.
        path: String,
    },
    /// A column a file lacks.
    #[error("{path} has no column {column}")]
    MissingColumn {
        /// The file's location.
        path: String,
        /// The column.
        column: String,
    },
    /// A column of a type the oracle does not read.
    #[error("column {column} of {path} is {found}")]
    UnexpectedColumn {
        /// The file's location.
        path: String,
        /// The column.
        column: String,
        /// What the file holds instead.
        found: String,
    },
}

/// The result type of every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, TestkitError>;

/// The environment variable `name` a gated test needs, or `missing` when it
/// is absent or not text: the one place a gate's variable is read, so a
/// test forced without its environment fails on the typed precondition.
pub fn required_var(name: &str, missing: Precondition) -> Result<String> {
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent | VarError::NotUnicode(_)) => Err(missing.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A variable no environment sets, so the unset branch is always taken.
    const NEVER_SET: &str = "ICETRAY_TESTKIT_NEVER_SET";

    #[test]
    fn test_required_var_unset_is_the_typed_precondition() {
        assert!(matches!(
            required_var(NEVER_SET, Precondition::GoldenUnset),
            Err(TestkitError::Precondition(Precondition::GoldenUnset))
        ));
        assert!(matches!(
            required_var(NEVER_SET, Precondition::RestUriUnset),
            Err(TestkitError::Precondition(Precondition::RestUriUnset))
        ));
    }
}
