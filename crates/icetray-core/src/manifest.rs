//! From a snapshot to its live data and delete files (RFD 005, RFD 021).
//! The table's layout (key and value columns, bucket transform) is derived
//! from the snapshot's own schema, and every manifest is read under its own
//! partition spec, so a schema or spec change between snapshots cannot
//! misroute a key; the table's current schema plays no part. Delete
//! manifests are read like data manifests, and wherever a delete file is
//! listed it is classified by its content and format rather than skipped.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{self, StreamExt as _, TryStreamExt as _};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, Literal, Manifest, ManifestEntry,
    ManifestFile, ManifestList, ManifestStatus, PartitionSpec, PrimitiveLiteral, PrimitiveType,
    Snapshot, Struct, TableMetadata, Transform, Type,
};

use crate::bucket;
use crate::catalog::{Resolved, TableFault};
use crate::column::{self, ColumnMismatch, UnreadDefaults};
use crate::config::{TableConfig, ValueFormat};
use crate::deletes::{Sorted, oldest_by_partition};
use crate::error::{Error, RefusalReason, Result, UnsupportedDelete, UnsupportedFormat};
use crate::key::{Key, KeyType};
use crate::order::{OrderColumnLayout, OrderType};
use crate::projection::{FieldSet, MAX_FIELD_NUMBER};
use crate::snapshot::{SequenceNumber, SnapshotId};
use crate::storage::Storage;

/// Manifests read at once while a snapshot's files are listed. A manifest
/// is one whole-object read of tens of kilobytes to a few megabytes, so
/// sixteen in flight bring a snapshot of hundreds of manifests down to a
/// few round trips of latency while holding a few dozen megabytes of Avro
/// at most.
const MANIFEST_READ_CONCURRENCY: usize = 16;

/// The sequence number of every manifest of a format v1 table, which
/// predates sequence numbers; an entry of such a manifest inherits it
/// whatever its status (`inherited_sequence`).
const INITIAL_SEQUENCE_NUMBER: i64 = 0;

/// The bucket a file of a table without a bucket transform on the key is
/// filed under: every such file is in the one bucket.
pub(crate) const UNBUCKETED: u32 = 0;

/// The bucket transform on a table's key column, `bucket[count]` over the
/// field `source_field_id` (RFD 008). Two files of one table may carry
/// different bucketings when the partition spec evolved between them; a
/// key's bucket differs under each.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bucketing {
    /// The number of buckets; above zero.
    pub count: u32,
    /// The field id of the key column the transform is over.
    pub source_field_id: i32,
}

impl Bucketing {
    /// The bucket `key` falls in under this transform.
    pub(crate) fn bucket_of(self, key: &Key) -> u32 {
        bucket::bucket(key, self.count)
    }
}

/// What routing and lookups need to know about a table, derived once per
/// generation from the snapshot's own schema and the table's default
/// partition spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableLayout {
    /// The bucket transform on the key in the default partition spec, the
    /// one new files are written under; `None` when it has none. Each data
    /// file routes by its own manifest's spec (`DataFileEntry::bucketing`);
    /// this is what `TableInfo` and the doctor report.
    pub bucketing: Option<Bucketing>,
    /// The key column's name.
    pub key_column: String,
    /// The key column's field id in the snapshot's schema.
    pub key_field_id: i32,
    /// The key column's type.
    pub key_type: KeyType,
    /// The width of every key of a `fixed` key column; `None` for every
    /// other key type.
    pub key_width: Option<u64>,
    /// The columns the table serves (RFD 022), in the order the
    /// configuration's `TableConfig::value_column_names` gives, ascending
    /// by name; a lookup names each by its position here, and every file's
    /// index holds its columns in this order.
    pub value_columns: Vec<ValueColumnLayout>,
    /// The order column of an ordered table (RFD 025); `None` for a
    /// key-value table. Every ordered-only rule is gated on it.
    pub order: Option<OrderColumnLayout>,
}

/// One served value column as a snapshot's schema gives it (RFD 022, RFD
/// 023).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueColumnLayout {
    /// The column's configured name.
    pub name: String,
    /// The column's field id in the snapshot's schema.
    pub field_id: i32,
    /// The column's Iceberg type.
    pub value_type: ValueType,
    /// What a data file written before the column was added answers for it
    /// (RFD 023): the column's `initial-default` in the snapshot's schema
    /// as bytes, or `None`, a null, when the schema gives none.
    pub default: Option<Bytes>,
}

impl ValueColumnLayout {
    /// The width of every value of a `fixed` column; `None` for binary and
    /// string, whose values have any length.
    pub fn width(&self) -> Option<u64> {
        self.value_type.width()
    }
}

/// The Iceberg types a value column may have, each served as its bytes
/// unchanged (RFD 002, RFD 022): a string as its UTF-8. Display gives the
/// type as Iceberg's `Type` displays it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueType {
    /// `binary`.
    Binary,
    /// `fixed[width]`.
    Fixed {
        /// The bytes of every value.
        width: u64,
    },
    /// `string`.
    String,
}

impl ValueType {
    /// The width of every value of a `fixed` column; `None` for binary and
    /// string, whose values have any length.
    pub fn width(self) -> Option<u64> {
        match self {
            Self::Fixed { width } => Some(width),
            Self::Binary | Self::String => None,
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binary => f.write_str("binary"),
            Self::Fixed { width } => write!(f, "fixed({width})"),
            Self::String => f.write_str("string"),
        }
    }
}

/// One column a lookup reads (RFD 022, RFD 024): the column's name, and
/// the top-level field numbers its protobuf value is trimmed to, empty for
/// the whole value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnRead<'a> {
    /// The column's configured name.
    pub name: &'a str,
    /// The field numbers to keep; empty reads the value whole.
    pub fields: &'a [u32],
}

impl<'a> ColumnRead<'a> {
    /// The whole value of the column named `name`.
    pub const fn whole(name: &'a str) -> Self {
        Self { name, fields: &[] }
    }
}

/// Column reads that hold none of the faults `check_column_reads` names:
/// no name given twice, and every field list in range with no number given
/// twice. Only `check_column_reads` makes one, and `select_columns` and
/// `TableLayout::select` take only this, so no read reaches selection
/// unchecked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckedReads<'r>(&'r [ColumnRead<'r>]);

impl<'r> CheckedReads<'r> {
    /// The reads, in the order the lookup named them.
    pub fn reads(self) -> &'r [ColumnRead<'r>] {
        self.0
    }
}

/// The column reads of one lookup checked against its table
/// (`select_columns`): per read, in the order the lookup named them, the
/// column's position in the layout and the fields its value is trimmed to,
/// `None` for the whole value. A read holds fields only when its column was
/// protobuf as the lookup was checked, so the lookup trims under the
/// format it was validated under whatever a retune declares meanwhile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Selection {
    /// The columns' positions in the layout.
    pub(crate) positions: Vec<usize>,
    /// The fields each read keeps, by the same index as `positions`.
    pub(crate) fields: Vec<Option<FieldSet>>,
}

impl Selection {
    /// Whether any read trims its value.
    pub(crate) fn projects(&self) -> bool {
        self.fields.iter().any(Option::is_some)
    }
}

/// What is wrong with a column a lookup names (RFD 022, RFD 024). Display
/// gives the fault for `Error::ColumnRequest`'s message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ColumnRequestFault {
    /// The table serves no column of that name.
    NotServed,
    /// The lookup names the column more than once.
    Repeated,
    /// The lookup names fields of a column not declared protobuf.
    NotProtobuf,
    /// The lookup names a field number outside 1 to `MAX_FIELD_NUMBER`.
    FieldOutOfRange {
        /// The number.
        field: u32,
    },
    /// The lookup names one field number twice for the column.
    RepeatedField {
        /// The number.
        field: u32,
    },
}

impl fmt::Display for ColumnRequestFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotServed => f.write_str("is not served"),
            Self::Repeated => f.write_str("is named more than once"),
            Self::NotProtobuf => {
                f.write_str("takes no field list, since it is not declared protobuf")
            }
            Self::FieldOutOfRange { field } => {
                write!(f, "names field {field}, outside 1 to {MAX_FIELD_NUMBER}")
            }
            Self::RepeatedField { field } => write!(f, "names field {field} more than once"),
        }
    }
}

impl TableLayout {
    /// The layout for `snapshot` of `table`. The schema is the snapshot's
    /// own, `snapshot.schema_id()` looked up in `md`; the table's current
    /// schema is consulted only for a snapshot that records no schema id
    /// (written before Iceberg recorded them), which Iceberg readers read
    /// under the current schema too. Refuses a key column
    /// whose type `KeyType::from_iceberg` does not admit as
    /// `RefusalReason::KeyTypeUnsupported` and a value column that is not
    /// binary, fixed, or string as `RefusalReason::ValueTypeUnsupported`
    /// naming it (RFD 002, RFD 022). Each column is a top-level field of the
    /// schema (RFD 023): a name the schema holds only nested, such as a
    /// dotted name, is `RefusalReason::ColumnMismatch` with
    /// `ColumnMismatch::NotTopLevel`, and a name it lacks altogether
    /// `TableFault::ColumnMissing`. Each value column's default is its
    /// `initial-default` (`column::value_default`); one `unread` names,
    /// which the metadata states but the parse dropped, is
    /// `ColumnMismatch::UnreadableDefault`. The value columns are kept in
    /// the order `value_columns` gives them, which
    /// `TableConfig::value_column_names` makes ascending by name. A schema
    /// id the metadata lacks is `TableFault::SchemaMissing`. Bucketing comes
    /// from `md`'s default partition spec, since a snapshot records no spec
    /// of its own; a bucket count of zero there is
    /// `TableFault::ZeroBuckets`. An `order_column` is found as a value
    /// column is, and refused as `RefusalReason::OrderTypeUnsupported`
    /// when `OrderType::from_iceberg` does not admit its type (RFD 025).
    pub(crate) fn derive(
        table: &str,
        md: &TableMetadata,
        unread: &UnreadDefaults,
        snapshot: &Snapshot,
        key_column: &str,
        value_columns: &[&str],
        order_column: Option<&str>,
    ) -> Result<Self> {
        let schema = match snapshot.schema_id() {
            Some(schema_id) => md.schema_by_id(schema_id).ok_or_else(|| {
                Error::table_fault(table, TableFault::SchemaMissing { schema_id })
            })?,
            None => md.current_schema(),
        };
        let mismatch = |name: &str, mismatch: ColumnMismatch| {
            Error::refused(
                table,
                RefusalReason::ColumnMismatch {
                    column: name.to_string(),
                    path: None,
                    mismatch,
                },
            )
        };
        let column = |name: &str| match schema.as_struct().field_by_name(name) {
            Some(field) => Ok(field),
            None if schema.field_by_name(name).is_some() => {
                Err(mismatch(name, ColumnMismatch::NotTopLevel))
            }
            None => Err(Error::table_fault(
                table,
                TableFault::ColumnMissing {
                    column: name.to_string(),
                },
            )),
        };
        let key_field = column(key_column)?;
        let key_type = KeyType::from_iceberg(&key_field.field_type)
            .ok_or_else(|| Error::refused(table, RefusalReason::KeyTypeUnsupported))?;
        let value_columns = value_columns
            .iter()
            .map(|&name| {
                let field = column(name)?;
                let value_type = value_type(table, name, &field.field_type)?;
                let default = column::value_default(
                    field,
                    value_type.width(),
                    unread.contains(schema.schema_id(), field.id),
                )
                .map_err(|cause| mismatch(name, cause))?;
                Ok(ValueColumnLayout {
                    name: name.to_string(),
                    field_id: field.id,
                    value_type,
                    default,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let order = order_column
            .map(|name| -> Result<OrderColumnLayout> {
                let field = column(name)?;
                let order_type = match field.field_type.as_ref() {
                    Type::Primitive(primitive) => OrderType::from_iceberg(primitive),
                    Type::Struct(_) | Type::List(_) | Type::Map(_) => None,
                }
                .ok_or_else(|| {
                    Error::refused(
                        table,
                        RefusalReason::OrderTypeUnsupported {
                            column: name.to_string(),
                            got: field.field_type.to_string(),
                        },
                    )
                })?;
                Ok(OrderColumnLayout {
                    name: name.to_string(),
                    field_id: field.id,
                    order_type,
                })
            })
            .transpose()?;
        let bucketing = bucket_field(table, md.default_partition_spec(), key_field.id)?
            .map(|field| field.bucketing);
        Ok(Self {
            bucketing,
            key_column: key_column.to_string(),
            key_field_id: key_field.id,
            key_type,
            key_width: fixed_width(&key_field.field_type),
            value_columns,
            order,
        })
    }

    /// The layout of `snapshot` for the table `config` describes, derived
    /// (`derive`) from `resolved`, its metadata and unread defaults, over
    /// the configured key, value, and order columns, with each column's declared
    /// format checked against its type (`check_formats`): what a load and
    /// a key-type resolution both build.
    pub(crate) fn for_entry(
        config: &TableConfig,
        resolved: &Resolved,
        snapshot: &Snapshot,
    ) -> Result<Self> {
        let layout = Self::derive(
            &config.name,
            &resolved.metadata,
            &resolved.unread_defaults,
            snapshot,
            &config.key_column,
            &config.value_column_names(),
            config.order_column.as_deref(),
        )?;
        layout.check_formats(config)?;
        Ok(layout)
    }

    /// The position in `value_columns` of the column named `name`; `None`
    /// for a name the table does not serve.
    pub fn value_column(&self, name: &str) -> Option<usize> {
        self.value_columns
            .iter()
            .position(|column| column.name == name)
    }

    /// The column reads `reads` names, checked for a lookup under `entry`,
    /// the table's entry as it stands (`select_columns`): each column's
    /// position here, and its format from `entry`, so a retune of formats
    /// applies to the next lookup. A column `entry` declares protobuf whose
    /// type here is not `binary` is taken as opaque: an entry retuned to
    /// protobuf after this layout was checked takes no field list for a
    /// column it cannot trim.
    pub(crate) fn select(&self, entry: &TableConfig, reads: CheckedReads<'_>) -> Result<Selection> {
        select_columns(&entry.name, reads, |name| {
            let at = self.value_column(name)?;
            let declared = entry.format_of(name)?;
            let format = if declared.admits(self.value_columns[at].value_type) {
                declared
            } else {
                ValueFormat::Opaque
            };
            Some((at, format))
        })
    }

    /// `ValueTypeUnsupported` for the first column `entry` declares in a
    /// format its type in this layout does not take (`check_format`): what
    /// a load refuses the snapshot for, and a retune of formats the entry
    /// for.
    pub(crate) fn check_formats(&self, entry: &TableConfig) -> Result<()> {
        let formats = entry.formats();
        self.value_columns.iter().try_for_each(|column| {
            check_format(
                &entry.name,
                &column.name,
                column.value_type,
                formats(&column.name),
            )
        })
    }
}

/// `RefusalReason::ValueTypeUnsupported` for `column` of `table`, of type
/// `value_type`, declared `format` when the format does not take the type
/// (`ValueFormat::admits`).
fn check_format(
    table: &str,
    column: &str,
    value_type: ValueType,
    format: ValueFormat,
) -> Result<()> {
    if format.admits(value_type) {
        return Ok(());
    }
    Err(Error::refused(
        table,
        RefusalReason::ValueTypeUnsupported {
            column: column.to_string(),
            got: value_type.to_string(),
            format,
        },
    ))
}

/// The faults the column reads `reads` names hold whatever table they are
/// for, checked in its order for a lookup of `table` before anything about
/// the table is: a name given twice is `Error::ColumnRequest` with
/// `ColumnRequestFault::Repeated`, and a field list with a number out of
/// range or given twice is `ColumnRequestFault::FieldOutOfRange` or
/// `ColumnRequestFault::RepeatedField` (`FieldSet::new`), whatever format
/// the column is in. The first fault in `reads` is reported. A table's
/// value columns have distinct names, so reads that pass name distinct
/// columns; they are returned as `CheckedReads`, what selection takes.
pub fn check_column_reads<'r>(
    table: &str,
    reads: &'r [ColumnRead<'r>],
) -> Result<CheckedReads<'r>> {
    let mut seen = HashSet::with_capacity(reads.len());
    for read in reads {
        let fault = |fault| Error::ColumnRequest {
            table: table.to_string(),
            column: read.name.to_string(),
            fault,
        };
        if !seen.insert(read.name) {
            return Err(fault(ColumnRequestFault::Repeated));
        }
        FieldSet::new(read.fields).map_err(fault)?;
    }
    Ok(CheckedReads(reads))
}

/// The column reads `reads` names, checked in its order for a lookup of
/// `table`, where `served` gives a served column's position and format: a
/// name `served` places nowhere, one the table does not serve, is
/// `Error::ColumnRequest` with
/// `ColumnRequestFault::NotServed`, and a field list on a column not
/// declared protobuf is `ColumnRequestFault::NotProtobuf`. The first fault
/// in `reads` is reported.
pub(crate) fn select_columns(
    table: &str,
    reads: CheckedReads<'_>,
    served: impl Fn(&str) -> Option<(usize, ValueFormat)>,
) -> Result<Selection> {
    let reads = reads.reads();
    let mut selection = Selection {
        positions: Vec::with_capacity(reads.len()),
        fields: Vec::with_capacity(reads.len()),
    };
    for read in reads {
        let fault = |fault| Error::ColumnRequest {
            table: table.to_string(),
            column: read.name.to_string(),
            fault,
        };
        let (at, format) = served(read.name).ok_or_else(|| fault(ColumnRequestFault::NotServed))?;
        let fields = match format {
            ValueFormat::Opaque if read.fields.is_empty() => None,
            ValueFormat::Opaque => return Err(fault(ColumnRequestFault::NotProtobuf)),
            ValueFormat::Protobuf => FieldSet::new(read.fields).map_err(fault)?,
        };
        selection.positions.push(at);
        selection.fields.push(fields);
    }
    Ok(selection)
}

/// The width of a `fixed` type; `None` for every other type.
fn fixed_width(ty: &Type) -> Option<u64> {
    match *ty {
        Type::Primitive(PrimitiveType::Fixed(width)) => Some(width),
        Type::Primitive(_) | Type::Struct(_) | Type::List(_) | Type::Map(_) => None,
    }
}

/// A value column is served as opaque bytes (RFD 010), so its Iceberg type
/// must be one whose bytes are the value: binary, fixed, or string, a
/// fixed one no wider than a Parquet `FIXED_LEN_BYTE_ARRAY` can store,
/// whose type length is an `i32`, as Iceberg's fixed length is a Java
/// `int`. Anything else is `RefusalReason::ValueTypeUnsupported` naming
/// the column `name` and carrying the type's `Display`.
fn value_type(table: &str, name: &str, ty: &Type) -> Result<ValueType> {
    match ty {
        Type::Primitive(PrimitiveType::Binary) => Ok(ValueType::Binary),
        Type::Primitive(PrimitiveType::Fixed(width)) if i32::try_from(*width).is_ok() => {
            Ok(ValueType::Fixed { width: *width })
        }
        Type::Primitive(PrimitiveType::String) => Ok(ValueType::String),
        Type::Primitive(
            PrimitiveType::Fixed(_)
            | PrimitiveType::Boolean
            | PrimitiveType::Int
            | PrimitiveType::Long
            | PrimitiveType::Float
            | PrimitiveType::Double
            | PrimitiveType::Decimal { .. }
            | PrimitiveType::Date
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs
            | PrimitiveType::Uuid,
        )
        | Type::Struct(_)
        | Type::List(_)
        | Type::Map(_) => Err(Error::refused(
            table,
            RefusalReason::ValueTypeUnsupported {
                column: name.to_string(),
                got: ty.to_string(),
                format: ValueFormat::Opaque,
            },
        )),
    }
}

/// The key's bucket field in one partition spec: the transform, and the
/// position of its value in the spec's partition tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BucketField {
    bucketing: Bucketing,
    position: usize,
}

/// The first field of `spec`, in spec order, that buckets the key column
/// `key_field_id`; `None` when the spec has none. A count of zero is
/// `TableFault::ZeroBuckets`. Any other transform on the key is not
/// routing and is passed over.
fn bucket_field(
    table: &str,
    spec: &PartitionSpec,
    key_field_id: i32,
) -> Result<Option<BucketField>> {
    for (position, field) in spec.fields().iter().enumerate() {
        if field.source_id != key_field_id {
            continue;
        }
        match field.transform {
            Transform::Bucket(0) => {
                return Err(Error::table_fault(
                    table,
                    TableFault::ZeroBuckets {
                        spec_id: spec.spec_id(),
                    },
                ));
            }
            Transform::Bucket(count) => {
                return Ok(Some(BucketField {
                    bucketing: Bucketing {
                        count,
                        source_field_id: key_field_id,
                    },
                    position,
                }));
            }
            Transform::Identity
            | Transform::Truncate(_)
            | Transform::Year
            | Transform::Month
            | Transform::Day
            | Transform::Hour
            | Transform::Void
            | Transform::Unknown => {}
        }
    }
    Ok(None)
}

/// One live data file of a snapshot, as its manifest records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFileEntry {
    /// The file's location as the manifest recorded it.
    pub path: String,
    /// The bucket the file holds, from its partition tuple; `UNBUCKETED`
    /// when `bucketing` is `None`. Routing keys files by `bucketing` first
    /// and this second, so the sentinel meets no bucketed file's zero there;
    /// it is a bucket number only where the doctor reports by number alone.
    pub bucket: u32,
    /// The bucket transform of the manifest's own partition spec (RFD 005),
    /// `None` when that spec has no bucket on the key. Files of one
    /// snapshot may differ here after a spec evolution.
    pub bucketing: Option<Bucketing>,
    /// The file's row count, from the manifest.
    pub records: u64,
    /// The file's data sequence number: the entry's own, or the one it
    /// inherits from its manifest (`inherited_sequence`). A delete applies
    /// to the file only when its own is no lower (RFD 021), and a path
    /// listed again at another sequence number is another file.
    pub sequence: SequenceNumber,
    /// The snapshot that added the file: the entry's own record of it, or
    /// the one its manifest records (`added_snapshot`). A snapshot that
    /// lists the file as existing is not this one, so two files share it
    /// only when a single commit wrote both.
    pub added_by: SnapshotId,
    /// The file's size in bytes, from the manifest; what `FileHandle::open`
    /// takes in place of a request.
    pub size: u64,
    /// The manifest's lower bound on the key, when it recorded one it can
    /// be read as a key. Advice only: a writer may truncate it, so routing
    /// never rests on it (RFD 007).
    pub key_min: Option<Key>,
    /// The manifest's upper bound on the key, under the same terms.
    pub key_max: Option<Key>,
    /// The file's partition, which an unscoped position delete must share
    /// to apply to it (RFD 021).
    pub(crate) partition: Partition,
}

/// A content file's partition as Iceberg's scan planning compares two for
/// a delete to apply: the spec its manifest is written under and its
/// partition tuple, equal only when both are. The tuple is shared by every
/// entry of one listing that carries it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Partition {
    spec_id: i32,
    values: Arc<Struct>,
}

#[cfg(test)]
impl Partition {
    /// The partition of a file under spec `spec_id` with tuple `values`.
    pub(crate) fn new(spec_id: i32, values: Struct) -> Self {
        Self {
            spec_id,
            values: Arc::new(values),
        }
    }

    /// The one partition of an unpartitioned spec 0.
    pub(crate) fn unpartitioned() -> Self {
        Self::new(0, Struct::empty())
    }
}

/// One live delete file of a snapshot that icetray can read, as its
/// manifest records it: a position delete file in Parquet or a deletion
/// vector (RFD 021).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeleteFileEntry {
    /// The file's location: the position delete file, or the Puffin file
    /// holding the vector.
    pub(crate) path: String,
    /// The rows it deletes, from the manifest: a vector's cardinality, a
    /// position delete file's row count.
    pub(crate) records: u64,
    /// The file's size in bytes, from the manifest; a Puffin footer is
    /// read from the end of it.
    pub(crate) size: u64,
    /// Its data sequence number, as `DataFileEntry::sequence` is found.
    pub(crate) sequence: SequenceNumber,
    /// The one data file it applies to, when the entry names one; a
    /// vector always does.
    pub(crate) referenced: Option<String>,
    /// Its partition; a position delete file that names no data file
    /// applies only to the files of this one.
    pub(crate) partition: Partition,
    /// What it is.
    pub(crate) kind: DeleteKind,
}

/// What a readable delete file is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeleteKind {
    /// A Parquet position delete file.
    Positions,
    /// A deletion vector: the blob at `offset` of `length` bytes in a
    /// Puffin file.
    Vector {
        /// The blob's offset in the Puffin file.
        offset: u64,
        /// The blob's length.
        length: u64,
    },
}

/// A snapshot's live files (`list_files`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotFiles {
    /// Every live data file, ordered by (bucketing, bucket, `key_min` with
    /// `None` first, path).
    pub(crate) data: Vec<DataFileEntry>,
    /// Every live delete file that may apply, in the order the manifests
    /// list them.
    pub(crate) deletes: Vec<DeleteFileEntry>,
}

/// The live files of `snapshot`: the manifest list and every manifest it
/// names, data and delete manifests alike, read through `storage` with
/// `MANIFEST_READ_CONCURRENCY` manifests in flight. An entry with status
/// `Deleted` is skipped. Each file's data sequence number is its entry's
/// own or the one it inherits from the manifest list's record of its
/// manifest (`inherited_sequence`); an entry that has neither is
/// `TableFault::SequenceNumberMissing`.
///
/// A data file's bucket comes from its own manifest's partition spec,
/// looked up in `md` by the id the manifest list records (missing is
/// `TableFault::SpecMissing`): the spec's bucket field on the key names the
/// position in the partition tuple whose integer is the bucket (a
/// non-integer there is `TableFault::BucketValueMissing`, one at or past
/// the spec's count `TableFault::BucketValueOutOfRange`), and a spec with
/// no such field files the entry under `UNBUCKETED` with no bucketing. Key
/// bounds come from the manifest's lower and upper bounds for the key field
/// through `Key::from_datum`.
///
/// A data file whose entry records a format other than Parquet is set
/// aside unread (`unsupported_format`), and a delete file is classified
/// wherever it is listed (`classify_delete`). Refused, after every manifest
/// is read so that the verdict is the same for the same manifests in any
/// order: first the data file with the least path among those not in
/// Parquet, as `RefusalReason::DataFormatUnsupported`; then the position
/// delete file in Avro or ORC with the least path among those that may apply to a data
/// file of the snapshot no deletion vector masks
/// (`Listed::applying_unsupported`; one that provably applies to none is
/// dropped), as `RefusalReason::DeleteKindUnsupported`; then the equality
/// delete with the least path among those above the least data sequence
/// number of the snapshot's data files, which may delete rows of that
/// file, as `RefusalReason::EqualityDeletes`. An equality delete at or below every
/// data file's sequence number applies to nothing and is dropped, as one
/// is when the snapshot has no data file.
///
/// A fault is raised at the first entry that shows it, so of a snapshot
/// with several, which one is reported can depend on the order of its
/// manifests; a refusal cannot.
pub(crate) async fn list_files(
    table: &str,
    md: &TableMetadata,
    snapshot: &Snapshot,
    layout: &TableLayout,
    storage: &Storage,
) -> Result<SnapshotFiles> {
    let list_bytes = storage.read_all(snapshot.manifest_list()).await?;
    let list = ManifestList::parse_with_version(&list_bytes, md.format_version())?;
    // Collected before streaming: the library returns an opaque
    // `impl IntoIterator`, whose iterator the compiler cannot prove `Send`,
    // and a load must be spawnable onto a multi-thread runtime.
    let files: Vec<ManifestFile> = list.consume_entries().into_iter().collect();
    let manifests: Vec<(ManifestFile, Manifest)> = stream::iter(files)
        .map(|file| async move {
            let bytes = storage.read_all(&file.manifest_path).await?;
            let manifest = Manifest::parse_avro(&bytes)?;
            Ok::<_, Error>((file, manifest))
        })
        .buffered(MANIFEST_READ_CONCURRENCY)
        .try_collect()
        .await?;
    let mut listed = Listed::default();
    let mut partitions: HashMap<(i32, &Struct), Partition> = HashMap::new();
    for (file, manifest) in &manifests {
        let spec = md
            .partition_spec_by_id(file.partition_spec_id)
            .ok_or_else(|| {
                Error::table_fault(
                    table,
                    TableFault::SpecMissing {
                        spec_id: file.partition_spec_id,
                    },
                )
            })?;
        let bucket_field = bucket_field(table, spec, layout.key_field_id)?;
        for entry in manifest.entries() {
            match entry.status() {
                ManifestStatus::Deleted => continue,
                ManifestStatus::Added | ManifestStatus::Existing => {}
            }
            let data_file = entry.data_file();
            let sequence = data_sequence(table, entry, file)?;
            let partition = partitions
                .entry((file.partition_spec_id, data_file.partition()))
                .or_insert_with(|| Partition {
                    spec_id: file.partition_spec_id,
                    values: Arc::new(data_file.partition().clone()),
                })
                .clone();
            match data_file.content_type() {
                DataContentType::Data => {
                    if let Some(format) = unsupported_format(data_file.file_format()) {
                        listed
                            .unsupported_data
                            .push((data_file.file_path().to_string(), format));
                        continue;
                    }
                    let (bucketing, bucket) = match bucket_field {
                        None => (None, UNBUCKETED),
                        Some(field) => (
                            Some(field.bucketing),
                            bucket_value(table, data_file, spec.spec_id(), field)?,
                        ),
                    };
                    listed.data.push(DataFileEntry {
                        path: data_file.file_path().to_string(),
                        bucket,
                        bucketing,
                        records: data_file.record_count(),
                        sequence,
                        added_by: added_snapshot(entry, file),
                        size: data_file.file_size_in_bytes(),
                        key_min: key_bound(data_file.lower_bounds(), layout.key_field_id),
                        key_max: key_bound(data_file.upper_bounds(), layout.key_field_id),
                        partition,
                    });
                }
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    classify_delete(table, data_file, sequence, partition, &mut listed)?;
                }
            }
        }
    }
    listed.judge(table)
}

/// A snapshot's live files as `list_files` reads them, before the delete
/// files it cannot serve are judged.
#[derive(Default)]
struct Listed {
    data: Vec<DataFileEntry>,
    deletes: Vec<DeleteFileEntry>,
    /// The data files in a format other than Parquet, by path.
    unsupported_data: Vec<(String, UnsupportedFormat)>,
    /// The position delete files icetray cannot read.
    unsupported: Vec<Unsupported>,
    /// Every equality delete, by path, with its data sequence number.
    equality: Vec<(String, SequenceNumber)>,
}

/// A position delete file icetray cannot read, with what decides whether
/// it may apply to a data file.
struct Unsupported {
    path: String,
    kind: UnsupportedDelete,
    sequence: SequenceNumber,
    referenced: Option<String>,
    partition: Partition,
}

impl Listed {
    /// The refusals `list_files` documents, else the files, the data files
    /// in order.
    fn judge(mut self, table: &str) -> Result<SnapshotFiles> {
        if let Some((path, format)) = std::mem::take(&mut self.unsupported_data)
            .into_iter()
            .min_by(|(a, _), (b, _)| a.cmp(b))
        {
            return Err(Error::refused(
                table,
                RefusalReason::DataFormatUnsupported { path, format },
            ));
        }
        if let Some(Unsupported { path, kind, .. }) = self.applying_unsupported() {
            return Err(Error::refused(
                table,
                RefusalReason::DeleteKindUnsupported { path, kind },
            ));
        }
        if let Some(oldest) = self.data.iter().map(|entry| entry.sequence).min()
            && let Some((path, _)) = self
                .equality
                .into_iter()
                .filter(|&(_, sequence)| sequence > oldest)
                .min()
        {
            return Err(Error::refused(
                table,
                RefusalReason::EqualityDeletes { path },
            ));
        }
        self.data.sort_by(entry_order);
        Ok(SnapshotFiles {
            data: self.data,
            deletes: self.deletes,
        })
    }

    /// Of the unreadable position deletes that may apply to a data file of
    /// the snapshot, the one with the least path. Only a data file no
    /// deletion vector masks (`Sorted::masks`) counts, since the spec
    /// ignores a file's position deletes where a vector applies to it. One
    /// that names a data file applies only to that file, when the snapshot
    /// lists it unmasked at a sequence number no greater than its own; one
    /// that names none, only to an unmasked file of its partition at such
    /// a number. Any other provably applies to nothing and is dropped, as
    /// a dangling or masked readable delete is never read.
    fn applying_unsupported(&mut self) -> Option<Unsupported> {
        if self.unsupported.is_empty() {
            return None;
        }
        let sorted = Sorted::of(&self.deletes);
        let unmasked: Vec<&DataFileEntry> = self
            .data
            .iter()
            .filter(|entry| !sorted.masks(&entry.path, entry.sequence))
            .collect();
        let by_path: HashMap<&str, SequenceNumber> = unmasked
            .iter()
            .map(|entry| (entry.path.as_str(), entry.sequence))
            .collect();
        let oldest = oldest_by_partition(
            unmasked
                .iter()
                .map(|entry| (&entry.partition, entry.sequence)),
        );
        let least = self
            .unsupported
            .iter()
            .enumerate()
            .filter(|(_, delete)| {
                let target = match &delete.referenced {
                    Some(referenced) => by_path.get(referenced.as_str()),
                    None => oldest.get(&delete.partition),
                };
                target.is_some_and(|&sequence| sequence <= delete.sequence)
            })
            .min_by_key(|(_, delete)| &delete.path)
            .map(|(at, _)| at)?;
        Some(self.unsupported.swap_remove(least))
    }
}

/// The format of a data file's entry as icetray refuses it, or `None` for
/// Parquet, the one format icetray reads rows from.
fn unsupported_format(format: DataFileFormat) -> Option<UnsupportedFormat> {
    match format {
        DataFileFormat::Parquet => None,
        DataFileFormat::Avro => Some(UnsupportedFormat::Avro),
        DataFileFormat::Orc => Some(UnsupportedFormat::Orc),
        DataFileFormat::Puffin => Some(UnsupportedFormat::Puffin),
    }
}

/// Files the live delete file `data_file` at `sequence` in `partition`
/// into `listed`: an equality delete for judging; a position delete file
/// by its format, a Parquet one as readable, an Avro or ORC one as
/// unsupported, and a Puffin one as a readable deletion vector, whose entry
/// must name its data file and its blob's offset and length, each
/// non-negative, or it is `TableFault::VectorUnplaced`.
fn classify_delete(
    table: &str,
    data_file: &DataFile,
    sequence: SequenceNumber,
    partition: Partition,
    listed: &mut Listed,
) -> Result<()> {
    let path = data_file.file_path().to_string();
    let kind = match (data_file.content_type(), data_file.file_format()) {
        (DataContentType::EqualityDeletes, _) => {
            listed.equality.push((path, sequence));
            return Ok(());
        }
        (_, DataFileFormat::Avro) => Err(UnsupportedDelete::AvroPositions),
        (_, DataFileFormat::Orc) => Err(UnsupportedDelete::OrcPositions),
        (_, DataFileFormat::Parquet) => Ok(DeleteKind::Positions),
        (_, DataFileFormat::Puffin) => {
            let placed = |value: Option<i64>| value.and_then(|value| u64::try_from(value).ok());
            match (
                data_file.referenced_data_file(),
                placed(data_file.content_offset()),
                placed(data_file.content_size_in_bytes()),
            ) {
                (Some(_), Some(offset), Some(length)) => Ok(DeleteKind::Vector { offset, length }),
                _unplaced => {
                    return Err(Error::table_fault(
                        table,
                        TableFault::VectorUnplaced { path },
                    ));
                }
            }
        }
    };
    let referenced = data_file.referenced_data_file();
    match kind {
        Err(kind) => listed.unsupported.push(Unsupported {
            path,
            kind,
            sequence,
            referenced,
            partition,
        }),
        Ok(kind) => listed.deletes.push(DeleteFileEntry {
            path,
            records: data_file.record_count(),
            size: data_file.file_size_in_bytes(),
            sequence,
            referenced,
            partition,
            kind,
        }),
    }
    Ok(())
}

/// The data sequence number of `entry` of `manifest`, or
/// `TableFault::SequenceNumberMissing` when `inherited_sequence` finds none.
/// `Manifest::parse_avro` reads entries as written, without the inheritance
/// iceberg-rust applies only on its own `FileIO` path, so it is applied here.
fn data_sequence(
    table: &str,
    entry: &ManifestEntry,
    manifest: &ManifestFile,
) -> Result<SequenceNumber> {
    inherited_sequence(
        entry.sequence_number(),
        entry.status(),
        manifest.sequence_number,
    )
    .ok_or_else(|| {
        Error::table_fault(
            table,
            TableFault::SequenceNumberMissing {
                path: entry.file_path().to_string(),
                manifest: manifest.manifest_path.clone(),
            },
        )
    })
}

/// The snapshot that added `entry` of `manifest`: the entry's own record of
/// it, or the one `manifest` records as having added its entries. Like the
/// sequence number (`data_sequence`), `Manifest::parse_avro` leaves an
/// entry's null as written, so the inheritance iceberg-rust applies only on
/// its own `FileIO` path is applied here.
fn added_snapshot(entry: &ManifestEntry, manifest: &ManifestFile) -> SnapshotId {
    added_snapshot_of(entry.snapshot_id(), manifest.added_snapshot_id)
}

/// Iceberg's inheritance of a snapshot id: an entry's `own`, when it
/// records one; else its manifest's, `manifest`. A manifest always records
/// one, so unlike the sequence number (`inherited_sequence`) this never
/// finds none.
fn added_snapshot_of(own: Option<i64>, manifest: i64) -> SnapshotId {
    SnapshotId(own.unwrap_or(manifest))
}

/// Iceberg's inheritance of a data sequence number: an entry's `own`, when
/// it records one; else its manifest's, `manifest`, when the entry was
/// added by the manifest's own commit (a writer leaves an added entry's
/// null, to be assigned at commit) or the manifest is a v1 one at
/// `INITIAL_SEQUENCE_NUMBER`; else none, since an existing or deleted entry
/// was added by an earlier commit whose sequence number it no longer
/// carries.
fn inherited_sequence(
    own: Option<i64>,
    status: ManifestStatus,
    manifest: i64,
) -> Option<SequenceNumber> {
    let inherits = match status {
        ManifestStatus::Added => true,
        ManifestStatus::Existing | ManifestStatus::Deleted => manifest == INITIAL_SEQUENCE_NUMBER,
    };
    own.or(inherits.then_some(manifest)).map(SequenceNumber)
}

/// The bucket in `data_file`'s partition tuple at `field`'s position: a
/// non-negative integer literal there, or `TableFault::BucketValueMissing`;
/// one at or past `field`'s bucket count, which no key hashes to, is
/// `TableFault::BucketValueOutOfRange`.
fn bucket_value(
    table: &str,
    data_file: &DataFile,
    spec_id: i32,
    field: BucketField,
) -> Result<u32> {
    let literal = data_file
        .partition()
        .fields()
        .get(field.position)
        .and_then(Option::as_ref);
    let bucket = match literal {
        Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => u32::try_from(*value).ok(),
        Some(
            Literal::Primitive(
                PrimitiveLiteral::Boolean(_)
                | PrimitiveLiteral::Long(_)
                | PrimitiveLiteral::Float(_)
                | PrimitiveLiteral::Double(_)
                | PrimitiveLiteral::String(_)
                | PrimitiveLiteral::Binary(_)
                | PrimitiveLiteral::Int128(_)
                | PrimitiveLiteral::UInt128(_)
                | PrimitiveLiteral::AboveMax
                | PrimitiveLiteral::BelowMin,
            )
            | Literal::Struct(_)
            | Literal::List(_)
            | Literal::Map(_),
        )
        | None => None,
    };
    let bucket = bucket.ok_or_else(|| {
        Error::table_fault(
            table,
            TableFault::BucketValueMissing {
                path: data_file.file_path().to_string(),
                spec_id,
            },
        )
    })?;
    let count = field.bucketing.count;
    if bucket >= count {
        return Err(Error::table_fault(
            table,
            TableFault::BucketValueOutOfRange {
                path: data_file.file_path().to_string(),
                spec_id,
                bucket,
                count,
            },
        ));
    }
    Ok(bucket)
}

/// The bound recorded for the key field, as a key; `None` when the
/// manifest recorded none or one that is not a key literal.
fn key_bound(bounds: &HashMap<i32, Datum>, key_field_id: i32) -> Option<Key> {
    bounds.get(&key_field_id).and_then(Key::from_datum)
}

/// The order of `list_files`'s data files.
fn entry_order(a: &DataFileEntry, b: &DataFileEntry) -> Ordering {
    a.bucketing
        .cmp(&b.bucketing)
        .then(a.bucket.cmp(&b.bucket))
        .then_with(|| a.key_min.cmp(&b.key_min))
        .then_with(|| a.path.cmp(&b.path))
}

/// Iceberg tables written with iceberg-rust's own writers, for the tests of
/// this module and of `generation`: metadata from `TableMetadataBuilder`,
/// manifests and manifest lists written through a filesystem `FileIO` into
/// a temporary directory, and read back through `Storage`'s `file://`
/// store. Nothing here is compiled outside tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        MAIN_BRANCH, ManifestContentType, ManifestListWriter, ManifestWriterBuilder, NestedField,
        Operation, PartitionSpec, PrimitiveType, Schema, Snapshot, SortOrder, Struct, Summary,
        TableMetadata, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
    };

    use super::{Bucketing, ColumnRead, TableLayout, ValueColumnLayout, ValueType};
    use crate::catalog::{METADATA_DIR, hint_location, version_file};
    use crate::key::{Key, KeyType};
    use crate::time;

    /// Each column `names` names read whole.
    pub(crate) fn whole<'a>(names: &[&'a str]) -> Vec<ColumnRead<'a>> {
        names.iter().map(|&name| ColumnRead::whole(name)).collect()
    }

    /// The key column of every fixture table.
    pub(crate) const KEY_COLUMN: &str = "key";
    /// The value column of every fixture table.
    pub(crate) const VALUE_COLUMN: &str = "value";
    /// The field id `TableMetadataBuilder::new` assigns the first column.
    pub(crate) const KEY_FIELD_ID: i32 = 1;
    /// The field id it assigns the second column.
    pub(crate) const VALUE_FIELD_ID: i32 = 2;
    /// The directory under a table root holding its data files.
    pub(crate) const DATA_DIR: &str = "data";
    /// The sequence number `ManifestWriter::add_file` writes as null, for the
    /// manifest list to assign: any negative one.
    const UNASSIGNED_SEQUENCE: i64 = -1;
    /// The time between the stamps of consecutive fixture snapshots: one
    /// millisecond, the least step Iceberg's timestamps can hold.
    const SNAPSHOT_STEP: Duration = Duration::from_millis(1);

    /// The timestamp of fixture snapshot `sequence` of `md`: `sequence`
    /// steps of `SNAPSHOT_STEP` after the metadata's last update, which the
    /// builder set to now, so it is later than that update and increases
    /// with the sequence, as the builder requires of a commit.
    pub(crate) fn snapshot_timestamp(md: &TableMetadata, sequence: i64) -> i64 {
        let steps = u32::try_from(sequence).unwrap();
        time::millis_after(md.last_updated_ms(), SNAPSHOT_STEP * steps)
    }

    /// A `file://` location for a temporary directory.
    pub(crate) fn file_root(dir: &std::path::Path) -> String {
        format!("file://{}", dir.display())
    }

    /// A schema of `KEY_COLUMN` of `key_type` and a nullable `VALUE_COLUMN`
    /// of `value_type`, fields `KEY_FIELD_ID` and `VALUE_FIELD_ID`.
    pub(crate) fn schema(key_type: Type, value_type: Type) -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(KEY_FIELD_ID, KEY_COLUMN, key_type).into(),
                NestedField::optional(VALUE_FIELD_ID, VALUE_COLUMN, value_type).into(),
            ])
            .build()
            .unwrap()
    }

    /// The layout of a table of `KEY_COLUMN` of `key_type` and a binary
    /// `VALUE_COLUMN` with no default, under `bucketing`: what a fixture
    /// whose data files are written without field ids is resolved against.
    pub(crate) fn layout(key_type: KeyType, bucketing: Option<Bucketing>) -> TableLayout {
        TableLayout {
            bucketing,
            key_column: KEY_COLUMN.to_string(),
            key_field_id: KEY_FIELD_ID,
            key_type,
            key_width: None,
            value_columns: vec![binary_column(VALUE_COLUMN, VALUE_FIELD_ID)],
            order: None,
        }
    }

    /// A binary value column named `name` at `field_id`, with no default.
    pub(crate) fn binary_column(name: &str, field_id: i32) -> ValueColumnLayout {
        ValueColumnLayout {
            name: name.to_string(),
            field_id,
            value_type: ValueType::Binary,
            default: None,
        }
    }

    /// The long-keyed, binary-valued schema most fixtures use.
    pub(crate) fn long_schema() -> Schema {
        schema(
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::Binary),
        )
    }

    /// A spec bucketing the key into `count` buckets, its field named by the
    /// count so two specs of one table never share a field name.
    pub(crate) fn bucket_spec(count: u32) -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .add_partition_field(
                KEY_FIELD_ID,
                format!("key_bucket_{count}"),
                Transform::Bucket(count),
            )
            .unwrap()
            .build()
    }

    /// A spec with no partition field.
    pub(crate) fn unpartitioned() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder().build()
    }

    /// Metadata for a table at `root` under `schema` and `spec`, with no
    /// snapshot yet; format version 2.
    pub(crate) fn metadata(
        root: &str,
        schema: Schema,
        spec: UnboundPartitionSpec,
    ) -> TableMetadata {
        metadata_of_version(root, schema, spec, FormatVersion::V2)
    }

    /// `metadata` at format `version`; `write_snapshot` writes v1 and v2.
    pub(crate) fn metadata_of_version(
        root: &str,
        schema: Schema,
        spec: UnboundPartitionSpec,
        version: FormatVersion,
    ) -> TableMetadata {
        TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::unsorted_order(),
            root.to_string(),
            version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    /// `md` with `snapshot` added and the main branch moved to it.
    pub(crate) fn with_snapshot(md: TableMetadata, snapshot: Snapshot) -> TableMetadata {
        md.into_builder(None)
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    /// What a fixture file's partition tuple holds.
    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Partition {
        /// A bucket under a bucket spec.
        Bucket(u32),
        /// A null where a bucket spec expects the bucket.
        NullBucket,
        /// The empty tuple of an unpartitioned spec.
        None,
    }

    /// The partition tuple `partition` describes.
    pub(crate) fn partition(partition: Partition) -> Struct {
        match partition {
            Partition::Bucket(bucket) => {
                Struct::from_iter([Some(Literal::int(i32::try_from(bucket).unwrap()))])
            }
            Partition::NullBucket => Struct::from_iter([None]),
            Partition::None => Struct::empty(),
        }
    }

    /// A key as the datum a manifest bound carries.
    pub(crate) fn datum(key: &Key) -> Datum {
        match key {
            Key::Long(value) => Datum::long(*value),
            Key::Str(text) => Datum::string(text),
            Key::Bytes(bytes) => Datum::binary(bytes.clone()),
            Key::Uuid(bytes) => Datum::uuid(uuid::Uuid::from_bytes(*bytes)),
        }
    }

    /// A data file of `content` at `path` under `spec_id`, with `records`
    /// rows, `size` bytes, and key bounds when given.
    pub(crate) fn data_file(
        path: &str,
        spec_id: i32,
        partition: Struct,
        records: u64,
        size: u64,
        bounds: Option<(&Key, &Key)>,
        content: DataContentType,
    ) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .partition_spec_id(spec_id)
            .record_count(records)
            .file_size_in_bytes(size);
        if let Some((min, max)) = bounds {
            builder
                .lower_bounds(HashMap::from([(KEY_FIELD_ID, datum(min))]))
                .upper_bounds(HashMap::from([(KEY_FIELD_ID, datum(max))]));
        }
        builder.build().unwrap()
    }

    /// How a manifest records one entry: its status and its data sequence
    /// number.
    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Written {
        /// Added by the snapshot, the sequence number left null for the
        /// manifest list to assign, as a writer leaves it.
        Added,
        /// Added by the snapshot, the sequence number written as given.
        AddedAt(i64),
        /// Carried from the commit at sequence number `sequence`, which
        /// snapshot `added_by` made.
        Existing {
            /// The sequence number the entry records.
            sequence: i64,
            /// The id of the snapshot that added the file.
            added_by: i64,
        },
        /// Removed by the snapshot; it was added at the given sequence
        /// number.
        Deleted(i64),
    }

    /// What a fixture delete file is.
    #[derive(Clone, Debug)]
    pub(crate) enum DeleteSpec {
        /// A position delete file in the given format.
        Positions(DataFileFormat),
        /// A position delete file in the given format whose entry names
        /// the given referenced data file.
        ScopedPositions(DataFileFormat, String),
        /// A deletion vector in a Puffin file, with the fields its manifest
        /// entry records as given.
        Vector {
            referenced: Option<String>,
            offset: Option<i64>,
            length: Option<i64>,
        },
        /// An equality delete on the key column.
        Equality,
    }

    /// The rows a fixture delete file entry records it deletes; nothing is
    /// written at the entry's path.
    pub(crate) const DELETE_RECORDS: u64 = 1;
    /// The bytes a fixture delete file entry records, under the same terms.
    pub(crate) const DELETE_BYTES: u64 = 1;

    /// A delete file entry at `path` under `spec_id` as `spec` describes
    /// it, of `DELETE_RECORDS` rows and `DELETE_BYTES` bytes; nothing is
    /// written there.
    pub(crate) fn delete_file(
        path: &str,
        spec_id: i32,
        partition: Struct,
        spec: DeleteSpec,
    ) -> DataFile {
        sized_delete_file(path, spec_id, partition, spec, DELETE_RECORDS, DELETE_BYTES)
    }

    /// `delete_file` recording `records` rows and `bytes` bytes, for a
    /// delete file written at `path`.
    pub(crate) fn sized_delete_file(
        path: &str,
        spec_id: i32,
        partition: Struct,
        spec: DeleteSpec,
        records: u64,
        bytes: u64,
    ) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .file_path(path.to_string())
            .partition(partition)
            .partition_spec_id(spec_id)
            .record_count(records)
            .file_size_in_bytes(bytes);
        match spec {
            DeleteSpec::Positions(format) => {
                builder
                    .content(DataContentType::PositionDeletes)
                    .file_format(format);
            }
            DeleteSpec::ScopedPositions(format, referenced) => {
                builder
                    .content(DataContentType::PositionDeletes)
                    .file_format(format)
                    .referenced_data_file(Some(referenced));
            }
            DeleteSpec::Vector {
                referenced,
                offset,
                length,
            } => {
                builder
                    .content(DataContentType::PositionDeletes)
                    .file_format(DataFileFormat::Puffin)
                    .referenced_data_file(referenced)
                    .content_offset(offset)
                    .content_size_in_bytes(length);
            }
            DeleteSpec::Equality => {
                builder
                    .content(DataContentType::EqualityDeletes)
                    .file_format(DataFileFormat::Parquet)
                    .equality_ids(Some(vec![KEY_FIELD_ID]));
            }
        }
        builder.build().unwrap()
    }

    /// One manifest to write: the spec its files are under, its content,
    /// and its entries as it records them.
    pub(crate) struct ManifestSpec {
        /// The partition spec the manifest's files are under.
        pub(crate) spec_id: i32,
        /// Whether it lists data files or delete files.
        pub(crate) content: ManifestContentType,
        /// Its entries, each with how the manifest records it.
        pub(crate) entries: Vec<(DataFile, Written)>,
    }

    /// Writes one manifest per `manifests` and the manifest list naming
    /// them under `md`'s root, and returns the snapshot that points at the
    /// list, ready for `with_snapshot`. `sequence` is the snapshot's, the
    /// one the manifest list records for each manifest.
    pub(crate) async fn write_snapshot(
        md: &TableMetadata,
        snapshot_id: i64,
        parent: Option<i64>,
        sequence: i64,
        schema_id: i32,
        manifests: Vec<ManifestSpec>,
    ) -> Snapshot {
        let file_io = FileIO::new_with_fs();
        let root = md.location();
        let schema = Arc::clone(md.schema_by_id(schema_id).unwrap());
        let mut files = Vec::with_capacity(manifests.len());
        for (index, manifest) in manifests.into_iter().enumerate() {
            let path = format!("{root}/{METADATA_DIR}/{snapshot_id}-m{index}.avro");
            let spec = PartitionSpec::clone(md.partition_spec_by_id(manifest.spec_id).unwrap());
            let builder = ManifestWriterBuilder::new(
                file_io.new_output(&path).unwrap(),
                Some(snapshot_id),
                Arc::clone(&schema),
                spec,
            );
            let mut writer = match (md.format_version(), manifest.content) {
                (FormatVersion::V1, ManifestContentType::Data) => builder.build_v1(),
                (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
                (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
                (FormatVersion::V1, ManifestContentType::Deletes) | (FormatVersion::V3, _) => {
                    unreachable!(
                        "no fixture writes {:?} manifests of a {:?} table",
                        manifest.content,
                        md.format_version()
                    )
                }
            };
            for (data_file, written) in manifest.entries {
                match written {
                    Written::Added => writer.add_file(data_file, UNASSIGNED_SEQUENCE).unwrap(),
                    Written::AddedAt(at) => writer.add_file(data_file, at).unwrap(),
                    Written::Existing { sequence, added_by } => writer
                        .add_existing_file(data_file, added_by, sequence, Some(sequence))
                        .unwrap(),
                    Written::Deleted(at) => {
                        writer.add_delete_file(data_file, at, Some(at)).unwrap();
                    }
                }
            }
            files.push(writer.write_manifest_file().await.unwrap());
        }
        let list_path = format!("{root}/{METADATA_DIR}/snap-{snapshot_id}.avro");
        let output = file_io
            .new_output(&list_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut list = match md.format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(output, snapshot_id, parent),
            FormatVersion::V2 => ManifestListWriter::v2(output, snapshot_id, parent, sequence),
            FormatVersion::V3 => unreachable!("no fixture writes a v3 manifest list"),
        };
        list.add_manifests(files.into_iter()).unwrap();
        list.close().await.unwrap();
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(snapshot_timestamp(md, sequence))
            .with_manifest_list(list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(schema_id)
            .build()
    }

    /// Writes `md` as metadata file `version` under its root and points the
    /// root's version hint at it; returns the file's location.
    pub(crate) async fn write_metadata(md: &TableMetadata, version: u64) -> String {
        let file_io = FileIO::new_with_fs();
        let location = version_file(md.location(), version);
        let json = serde_json::to_vec(md).unwrap();
        write_file(&file_io, &location, Bytes::from(json)).await;
        write_file(
            &file_io,
            &hint_location(md.location()),
            Bytes::from(version.to_string()),
        )
        .await;
        location
    }

    /// Writes `bytes` at `location` through `file_io`, creating directories
    /// on the way.
    pub(crate) async fn write_file(file_io: &FileIO, location: &str, bytes: Bytes) {
        file_io
            .new_output(location)
            .unwrap()
            .write(bytes)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{
        DataContentType, ManifestContentType, NestedField, PrimitiveType, Schema, Snapshot,
        TableMetadataBuilder, Type,
    };

    use super::testing::{
        self, DATA_DIR, DeleteSpec, KEY_COLUMN, KEY_FIELD_ID, VALUE_COLUMN, VALUE_FIELD_ID, Written,
    };
    use super::*;

    /// `reads` through `check_column_reads`, which these reads pass.
    fn checked<'r>(reads: &'r [ColumnRead<'r>]) -> CheckedReads<'r> {
        check_column_reads(TABLE, reads).unwrap()
    }
    use crate::bucket;
    use crate::config::{StorageConfig, ValueColumnConfig};
    use crate::generation::testing::{TABLE, refusal_of, table_fault_of};
    use crate::key::UUID_BYTES;

    /// The spec id of an unpartitioned fixture table's one spec.
    const UNPARTITIONED_SPEC: i32 = 0;
    /// A column name no fixture schema has.
    const ABSENT_COLUMN: &str = "absent";
    /// The second value column name of the evolved schema.
    const RENAMED_VALUE_COLUMN: &str = "payload";
    /// The field id of the evolved schema's new column.
    const RENAMED_VALUE_FIELD_ID: i32 = 3;
    /// A schema id no fixture metadata has.
    const ABSENT_SCHEMA_ID: i32 = 7;
    /// The bucket count of the first spec.
    const BUCKETS: u32 = 4;
    /// The bucket count of the evolved spec.
    const MORE_BUCKETS: u32 = 8;
    /// The ids of the two fixture snapshots, distinct from every sequence
    /// number so a test that confuses the two fails.
    const SNAPSHOT_A: i64 = 11;
    const SNAPSHOT_B: i64 = 22;
    /// Their sequence numbers, in commit order.
    const SEQUENCE_A: i64 = 1;
    const SEQUENCE_B: i64 = 2;
    /// The third fixture snapshot and its sequence number.
    const SNAPSHOT_C: i64 = 33;
    const SEQUENCE_C: i64 = 3;
    /// The snapshot id an entry records for itself, and the one its
    /// manifest records as having added it, distinct so a test that takes
    /// the wrong one fails.
    const ENTRY_SNAPSHOT: i64 = SNAPSHOT_A;
    const MANIFEST_SNAPSHOT: i64 = SNAPSHOT_B;
    /// An entry carried from the commit of snapshot `SNAPSHOT_A`.
    const EXISTING_FROM_A: Written = Written::Existing {
        sequence: SEQUENCE_A,
        added_by: SNAPSHOT_A,
    };
    /// The record count every fixture data file claims; nothing here reads
    /// a data file, so any count serves.
    const RECORDS: u64 = 1_000;
    /// The size every fixture data file claims, for the same reason.
    const SIZE: u64 = 4_096;
    /// Key bounds a fixture file claims, `LOW..=HIGH`.
    const LOW: i64 = 10;
    const HIGH: i64 = 20;
    /// Key bounds of a file below it, `LOWER..=HIGHER`, so the two ranges
    /// are disjoint.
    const LOWER: i64 = 5;
    const HIGHER: i64 = 7;
    /// Keys the routing tests hash.
    const SAMPLE_KEYS: [i64; 5] = [0, 1, 7, 1_000, -3];
    /// A snapshot id for the placeholder snapshots that derive takes.
    const PLACEHOLDER_SNAPSHOT: i64 = 1;
    /// The width of the `fixed` value column of the value-type test; any
    /// width is admitted, so one is enough.
    const FIXED_VALUE_BYTES: u64 = 8;

    fn storage() -> Storage {
        Storage::new(StorageConfig::default())
    }

    /// A snapshot recording `schema_id`, or none, for `derive`, which reads
    /// nothing else of it.
    fn snapshot_with_schema(schema_id: Option<i32>) -> Snapshot {
        let builder = Snapshot::builder()
            .with_snapshot_id(PLACEHOLDER_SNAPSHOT)
            .with_sequence_number(SEQUENCE_A)
            .with_timestamp_ms(0)
            .with_manifest_list("mem://w/db/kv/metadata/snap.avro")
            .with_summary(iceberg::spec::Summary {
                operation: iceberg::spec::Operation::Append,
                additional_properties: std::collections::HashMap::new(),
            });
        match schema_id {
            Some(schema_id) => builder.with_schema_id(schema_id).build(),
            None => builder.build(),
        }
    }

    fn derive(md: &TableMetadata, snapshot: &Snapshot) -> Result<TableLayout> {
        TableLayout::derive(
            TABLE,
            md,
            &UnreadDefaults::default(),
            snapshot,
            KEY_COLUMN,
            &[VALUE_COLUMN],
            None,
        )
    }

    /// The table fault `result` ended in, under `TABLE`.
    fn fault_of<T: std::fmt::Debug>(result: Result<T>) -> TableFault {
        table_fault_of(result, TABLE)
    }

    fn data_path(root: &str, name: &str) -> String {
        format!("{root}/{DATA_DIR}/{name}.parquet")
    }

    /// A live data file in `bucket` of `spec_id` with the given bounds.
    fn live(
        root: &str,
        name: &str,
        spec_id: i32,
        bucket: Option<u32>,
        bounds: Option<(i64, i64)>,
    ) -> (iceberg::spec::DataFile, Written) {
        let bounds = bounds.map(|(min, max)| (Key::Long(min), Key::Long(max)));
        (
            testing::data_file(
                &data_path(root, name),
                spec_id,
                testing::partition(
                    bucket.map_or(testing::Partition::None, testing::Partition::Bucket),
                ),
                RECORDS,
                SIZE,
                bounds.as_ref().map(|(min, max)| (min, max)),
                DataContentType::Data,
            ),
            Written::Added,
        )
    }

    /// `list_files`'s data files.
    async fn data_files(
        table: &str,
        md: &TableMetadata,
        snapshot: &Snapshot,
        layout: &TableLayout,
        storage: &Storage,
    ) -> Result<Vec<DataFileEntry>> {
        list_files(table, md, snapshot, layout, storage)
            .await
            .map(|files| files.data)
    }

    fn bucketing(count: u32) -> Bucketing {
        Bucketing {
            count,
            source_field_id: KEY_FIELD_ID,
        }
    }

    #[test]
    fn test_derive_admits_every_key_type_including_uuid_and_fixed() {
        let dir = tempfile::tempdir().unwrap();
        let fixed_width = crate::approx::count(UUID_BYTES);
        let cases = [
            (PrimitiveType::Long, KeyType::Long, None),
            (PrimitiveType::String, KeyType::String, None),
            (PrimitiveType::Binary, KeyType::Binary, None),
            (
                PrimitiveType::Fixed(fixed_width),
                KeyType::Binary,
                Some(fixed_width),
            ),
            (PrimitiveType::Uuid, KeyType::Uuid, None),
        ];
        for (primitive, expected, key_width) in cases {
            let md = testing::metadata(
                &testing::file_root(dir.path()),
                testing::schema(
                    Type::Primitive(primitive),
                    Type::Primitive(PrimitiveType::Binary),
                ),
                testing::bucket_spec(BUCKETS),
            );
            let layout = derive(&md, &snapshot_with_schema(Some(md.current_schema_id()))).unwrap();
            assert_eq!(
                layout,
                TableLayout {
                    bucketing: Some(bucketing(BUCKETS)),
                    key_column: KEY_COLUMN.to_string(),
                    key_field_id: KEY_FIELD_ID,
                    key_type: expected,
                    key_width,
                    value_columns: vec![testing::binary_column(VALUE_COLUMN, VALUE_FIELD_ID)],
                    order: None,
                }
            );
        }
    }

    #[test]
    fn test_derive_refuses_an_unsupported_key_type() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            testing::schema(
                Type::Primitive(PrimitiveType::Int),
                Type::Primitive(PrimitiveType::Binary),
            ),
            testing::unpartitioned(),
        );
        assert_eq!(
            refusal_of(derive(&md, &snapshot_with_schema(None))),
            RefusalReason::KeyTypeUnsupported
        );
    }

    #[test]
    fn test_derive_admits_binary_fixed_and_string_values_and_refuses_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        for admitted in [
            PrimitiveType::Binary,
            PrimitiveType::Fixed(FIXED_VALUE_BYTES),
            PrimitiveType::String,
        ] {
            let md = testing::metadata(
                &root,
                testing::schema(
                    Type::Primitive(PrimitiveType::Long),
                    Type::Primitive(admitted),
                ),
                testing::unpartitioned(),
            );
            assert!(derive(&md, &snapshot_with_schema(None)).is_ok());
        }
        let wider_than_parquet_stores = u64::try_from(i32::MAX).unwrap() + 1;
        for refused in [
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::Uuid),
            Type::Primitive(PrimitiveType::Double),
            Type::Primitive(PrimitiveType::Fixed(wider_than_parquet_stores)),
        ] {
            let md = testing::metadata(
                &root,
                testing::schema(Type::Primitive(PrimitiveType::Long), refused.clone()),
                testing::unpartitioned(),
            );
            assert_eq!(
                refusal_of(derive(&md, &snapshot_with_schema(None))),
                RefusalReason::ValueTypeUnsupported {
                    column: VALUE_COLUMN.to_string(),
                    got: refused.to_string(),
                    format: ValueFormat::Opaque,
                }
            );
        }
    }

    /// A schema of the key, the binary `VALUE_COLUMN`, and the string
    /// `RENAMED_VALUE_COLUMN`: a table of two value columns.
    fn two_value_schema(second: Type) -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    KEY_FIELD_ID,
                    KEY_COLUMN,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    VALUE_FIELD_ID,
                    VALUE_COLUMN,
                    Type::Primitive(PrimitiveType::Binary),
                )
                .into(),
                NestedField::optional(RENAMED_VALUE_FIELD_ID, RENAMED_VALUE_COLUMN, second).into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_derive_takes_every_value_column_in_the_order_given() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            two_value_schema(Type::Primitive(PrimitiveType::String)),
            testing::unpartitioned(),
        );
        let snapshot = snapshot_with_schema(None);
        let derive_columns = |columns: &[&str]| {
            TableLayout::derive(
                TABLE,
                &md,
                &UnreadDefaults::default(),
                &snapshot,
                KEY_COLUMN,
                columns,
                None,
            )
        };
        let layout = derive_columns(&[RENAMED_VALUE_COLUMN, VALUE_COLUMN]).unwrap();
        let columns: Vec<(&str, i32, ValueType)> = layout
            .value_columns
            .iter()
            .map(|column| (column.name.as_str(), column.field_id, column.value_type))
            .collect();
        assert_eq!(
            columns,
            [
                (
                    RENAMED_VALUE_COLUMN,
                    RENAMED_VALUE_FIELD_ID,
                    ValueType::String
                ),
                (VALUE_COLUMN, VALUE_FIELD_ID, ValueType::Binary),
            ]
        );
        assert!(derive_columns(&[]).unwrap().value_columns.is_empty());
        let unsupported = TableLayout::derive(
            TABLE,
            &testing::metadata(
                &testing::file_root(dir.path()),
                two_value_schema(Type::Primitive(PrimitiveType::Long)),
                testing::unpartitioned(),
            ),
            &UnreadDefaults::default(),
            &snapshot,
            KEY_COLUMN,
            &[VALUE_COLUMN, RENAMED_VALUE_COLUMN],
            None,
        );
        assert_eq!(
            refusal_of(unsupported),
            RefusalReason::ValueTypeUnsupported {
                column: RENAMED_VALUE_COLUMN.to_string(),
                got: Type::Primitive(PrimitiveType::Long).to_string(),
                format: ValueFormat::Opaque,
            }
        );
    }

    /// The order column is a top-level field found as a value column is,
    /// admitted when `OrderType::from_iceberg` takes its type and refused
    /// as `order_type_unsupported` from the schema otherwise (RFD 025); a
    /// key-value table derives none.
    #[test]
    fn test_derive_takes_an_order_column_of_an_order_type_and_refuses_another() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = snapshot_with_schema(None);
        let derive_ordered = |second: PrimitiveType, order_column: Option<&str>| {
            TableLayout::derive(
                TABLE,
                &testing::metadata(
                    &testing::file_root(dir.path()),
                    two_value_schema(Type::Primitive(second)),
                    testing::unpartitioned(),
                ),
                &UnreadDefaults::default(),
                &snapshot,
                KEY_COLUMN,
                &[VALUE_COLUMN],
                order_column,
            )
        };
        for (second, order_type) in [
            (PrimitiveType::Long, OrderType::Long),
            (PrimitiveType::Timestamptz, OrderType::TimestampTz),
            (PrimitiveType::String, OrderType::String),
        ] {
            assert_eq!(
                derive_ordered(second, Some(RENAMED_VALUE_COLUMN))
                    .unwrap()
                    .order,
                Some(OrderColumnLayout {
                    name: RENAMED_VALUE_COLUMN.to_string(),
                    field_id: RENAMED_VALUE_FIELD_ID,
                    order_type,
                })
            );
        }
        for second in [PrimitiveType::Double, PrimitiveType::Uuid] {
            assert_eq!(
                refusal_of(derive_ordered(second.clone(), Some(RENAMED_VALUE_COLUMN))),
                RefusalReason::OrderTypeUnsupported {
                    column: RENAMED_VALUE_COLUMN.to_string(),
                    got: second.to_string(),
                }
            );
        }
        assert_eq!(
            derive_ordered(PrimitiveType::Long, None).unwrap().order,
            None
        );
        assert_eq!(
            fault_of(derive_ordered(PrimitiveType::Long, Some("absent"))),
            TableFault::ColumnMissing {
                column: "absent".to_string(),
            }
        );
    }

    /// The entry of `TABLE` serving `columns`, the rest of it as a test
    /// leaves it.
    fn entry(columns: Vec<ValueColumnConfig>) -> TableConfig {
        TableConfig {
            name: TABLE.to_string(),
            source: crate::config::SourceConfig::Static {
                location: "mem://w/db/kv".to_string(),
            },
            r#ref: iceberg::spec::MAIN_BRANCH.to_string(),
            key_column: KEY_COLUMN.to_string(),
            value_columns: columns,
            order_column: None,
            max_scan_rows: None,
            strict: false,
            lookups: None,
            residency: crate::config::Residency::Full,
            resident_budget_bytes: None,
            row_cache: None,
        }
    }

    /// The layout of `select`'s tests: `RENAMED_VALUE_COLUMN` and
    /// `VALUE_COLUMN`, both binary, at positions 0 and 1, and the entry
    /// declaring the first protobuf.
    fn select_fixture() -> (TableLayout, TableConfig) {
        let layout = TableLayout {
            value_columns: vec![
                testing::binary_column(RENAMED_VALUE_COLUMN, RENAMED_VALUE_FIELD_ID),
                testing::binary_column(VALUE_COLUMN, VALUE_FIELD_ID),
            ],
            ..testing::layout(KeyType::Long, None)
        };
        let config = entry(vec![
            ValueColumnConfig::named(VALUE_COLUMN),
            ValueColumnConfig::of(RENAMED_VALUE_COLUMN, ValueFormat::Protobuf),
        ]);
        (layout, config)
    }

    #[test]
    fn test_select_names_columns_by_position_with_the_fields_each_keeps() {
        let (layout, config) = select_fixture();
        let fields = [9, 2];
        let selection = layout
            .select(
                &config,
                checked(&[
                    ColumnRead::whole(VALUE_COLUMN),
                    ColumnRead {
                        name: RENAMED_VALUE_COLUMN,
                        fields: &fields,
                    },
                ]),
            )
            .unwrap();
        assert_eq!(selection.positions, [1, 0]);
        assert_eq!(
            selection.fields,
            [None, FieldSet::new(&fields).unwrap()],
            "a protobuf column keeps the fields named, an empty list none"
        );
        assert!(selection.projects());
        let whole = layout
            .select(&config, checked(&[ColumnRead::whole(RENAMED_VALUE_COLUMN)]))
            .unwrap();
        assert_eq!(whole.fields, [None]);
        assert!(!whole.projects());
        assert_eq!(
            layout.select(&config, checked(&[])).unwrap().positions,
            Vec::<usize>::new()
        );
    }

    #[test]
    fn test_check_column_reads_refuses_the_request_only_faults_and_select_the_rest() {
        let (layout, config) = select_fixture();
        let one = [1];
        let field = |name, fields| ColumnRead { name, fields };
        // Each case's reads, the column the fault names, the fault, and
        // whether the reads alone hold it, whatever the table serves.
        let cases = [
            (
                vec![
                    ColumnRead::whole(VALUE_COLUMN),
                    ColumnRead::whole(ABSENT_COLUMN),
                ],
                ABSENT_COLUMN,
                ColumnRequestFault::NotServed,
                false,
            ),
            (
                vec![field(VALUE_COLUMN, &one[..])],
                VALUE_COLUMN,
                ColumnRequestFault::NotProtobuf,
                false,
            ),
            (
                vec![
                    ColumnRead::whole(VALUE_COLUMN),
                    field(RENAMED_VALUE_COLUMN, &one[..]),
                    field(VALUE_COLUMN, &[][..]),
                ],
                VALUE_COLUMN,
                ColumnRequestFault::Repeated,
                true,
            ),
            (
                vec![
                    ColumnRead::whole(ABSENT_COLUMN),
                    ColumnRead::whole(ABSENT_COLUMN),
                ],
                ABSENT_COLUMN,
                ColumnRequestFault::Repeated,
                true,
            ),
            (
                vec![field(VALUE_COLUMN, &[0][..])],
                VALUE_COLUMN,
                ColumnRequestFault::FieldOutOfRange { field: 0 },
                true,
            ),
            (
                vec![field(RENAMED_VALUE_COLUMN, &[1, 0][..])],
                RENAMED_VALUE_COLUMN,
                ColumnRequestFault::FieldOutOfRange { field: 0 },
                true,
            ),
            (
                vec![field(RENAMED_VALUE_COLUMN, &[MAX_FIELD_NUMBER + 1][..])],
                RENAMED_VALUE_COLUMN,
                ColumnRequestFault::FieldOutOfRange {
                    field: MAX_FIELD_NUMBER + 1,
                },
                true,
            ),
            (
                vec![field(RENAMED_VALUE_COLUMN, &[3, 1, 3][..])],
                RENAMED_VALUE_COLUMN,
                ColumnRequestFault::RepeatedField { field: 3 },
                true,
            ),
        ];
        for (reads, named, expected, request_only) in cases {
            let checked = check_column_reads(TABLE, &reads);
            let refusals = if request_only {
                vec![checked.map(drop), config.check_column_request(&reads)]
            } else {
                assert!(checked.is_ok(), "{reads:?} holds no fault of its own");
                vec![
                    layout.select(&config, checked.unwrap()).map(drop),
                    config.check_column_request(&reads),
                ]
            };
            for result in refusals {
                let Err(Error::ColumnRequest {
                    table,
                    column,
                    fault,
                }) = result
                else {
                    panic!("{reads:?} is refused");
                };
                assert_eq!(
                    (table.as_str(), column.as_str(), fault),
                    (TABLE, named, expected)
                );
            }
        }
    }

    #[test]
    fn test_select_takes_a_protobuf_entry_on_a_column_not_binary_as_opaque() {
        let (layout, config) = select_fixture();
        let layout = TableLayout {
            value_columns: layout
                .value_columns
                .into_iter()
                .map(|column| ValueColumnLayout {
                    value_type: ValueType::String,
                    ..column
                })
                .collect(),
            ..layout
        };
        let fields = [1];
        let Err(Error::ColumnRequest { column, fault, .. }) = layout.select(
            &config,
            checked(&[ColumnRead {
                name: RENAMED_VALUE_COLUMN,
                fields: &fields,
            }]),
        ) else {
            panic!("a field list on a string column is refused");
        };
        assert_eq!(
            (column.as_str(), fault),
            (RENAMED_VALUE_COLUMN, ColumnRequestFault::NotProtobuf)
        );
        let whole = layout
            .select(&config, checked(&[ColumnRead::whole(RENAMED_VALUE_COLUMN)]))
            .unwrap();
        assert_eq!(whole.fields, [None], "and read whole as opaque");
    }

    #[test]
    fn test_check_formats_refuses_protobuf_on_a_string_or_fixed_column() {
        let fixed_width = 16;
        let layout = |value_type| TableLayout {
            value_columns: vec![ValueColumnLayout {
                value_type,
                ..testing::binary_column(VALUE_COLUMN, VALUE_FIELD_ID)
            }],
            ..testing::layout(KeyType::Long, None)
        };
        let protobuf = entry(vec![ValueColumnConfig::of(
            VALUE_COLUMN,
            ValueFormat::Protobuf,
        )]);
        let opaque = entry(vec![ValueColumnConfig::named(VALUE_COLUMN)]);
        assert!(layout(ValueType::Binary).check_formats(&protobuf).is_ok());
        for value_type in [ValueType::String, ValueType::Fixed { width: fixed_width }] {
            assert!(layout(value_type).check_formats(&opaque).is_ok());
            assert_eq!(
                refusal_of(layout(value_type).check_formats(&protobuf)),
                RefusalReason::ValueTypeUnsupported {
                    column: VALUE_COLUMN.to_string(),
                    got: value_type.to_string(),
                    format: ValueFormat::Protobuf,
                },
                "{value_type}"
            );
        }
        assert_eq!(
            ValueType::Fixed { width: fixed_width }.to_string(),
            Type::Primitive(PrimitiveType::Fixed(fixed_width)).to_string(),
            "as Iceberg displays the type"
        );
    }

    #[test]
    fn test_derive_missing_column_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            testing::long_schema(),
            testing::unpartitioned(),
        );
        let snapshot = snapshot_with_schema(None);
        for (key_column, value_column) in
            [(ABSENT_COLUMN, VALUE_COLUMN), (KEY_COLUMN, ABSENT_COLUMN)]
        {
            assert_eq!(
                fault_of(TableLayout::derive(
                    TABLE,
                    &md,
                    &UnreadDefaults::default(),
                    &snapshot,
                    key_column,
                    &[value_column],
                    None
                )),
                TableFault::ColumnMissing {
                    column: ABSENT_COLUMN.to_string(),
                }
            );
        }
    }

    /// The evolved schema: the value column renamed, under a new field id.
    fn renamed_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    KEY_FIELD_ID,
                    KEY_COLUMN,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    RENAMED_VALUE_FIELD_ID,
                    RENAMED_VALUE_COLUMN,
                    Type::Primitive(PrimitiveType::Binary),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_derive_uses_the_snapshots_schema_not_the_current_one() {
        let dir = tempfile::tempdir().unwrap();
        let first = testing::metadata(
            &testing::file_root(dir.path()),
            testing::long_schema(),
            testing::unpartitioned(),
        );
        let first_schema_id = first.current_schema_id();
        let md = first
            .into_builder(None)
            .add_schema(renamed_schema())
            .unwrap()
            .set_current_schema(TableMetadataBuilder::LAST_ADDED)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let current_schema_id = md.current_schema_id();
        assert_ne!(current_schema_id, first_schema_id);
        assert!(md.current_schema().field_by_name(VALUE_COLUMN).is_none());

        // A snapshot written under the first schema still finds `value`.
        let old = derive(&md, &snapshot_with_schema(Some(first_schema_id))).unwrap();
        assert_eq!(old.value_columns[0].field_id, VALUE_FIELD_ID);
        // One written under the current schema does not.
        assert_eq!(
            fault_of(derive(&md, &snapshot_with_schema(Some(current_schema_id)))),
            TableFault::ColumnMissing {
                column: VALUE_COLUMN.to_string(),
            }
        );
        let renamed = TableLayout::derive(
            TABLE,
            &md,
            &UnreadDefaults::default(),
            &snapshot_with_schema(Some(current_schema_id)),
            KEY_COLUMN,
            &[RENAMED_VALUE_COLUMN],
            None,
        )
        .unwrap();
        assert_eq!(renamed.value_columns[0].field_id, RENAMED_VALUE_FIELD_ID);
        // A snapshot without a schema id uses the current schema.
        let by_default = TableLayout::derive(
            TABLE,
            &md,
            &UnreadDefaults::default(),
            &snapshot_with_schema(None),
            KEY_COLUMN,
            &[RENAMED_VALUE_COLUMN],
            None,
        )
        .unwrap();
        assert_eq!(by_default.value_columns[0].field_id, RENAMED_VALUE_FIELD_ID);
        assert_eq!(
            fault_of(derive(&md, &snapshot_with_schema(Some(ABSENT_SCHEMA_ID)))),
            TableFault::SchemaMissing {
                schema_id: ABSENT_SCHEMA_ID,
            }
        );
    }

    /// The schema of the nested-name test: the key, and a struct `outer`
    /// holding the binary `inner`, at field ids one, two, and three.
    fn nested_schema() -> Schema {
        let inner = NestedField::optional(
            RENAMED_VALUE_FIELD_ID,
            NESTED_INNER,
            Type::Primitive(PrimitiveType::Binary),
        );
        Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    KEY_FIELD_ID,
                    KEY_COLUMN,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    VALUE_FIELD_ID,
                    NESTED_OUTER,
                    Type::Struct(iceberg::spec::StructType::new(vec![inner.into()])),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    /// The struct field of `nested_schema` and the field inside it.
    const NESTED_OUTER: &str = "outer";
    const NESTED_INNER: &str = "inner";

    #[test]
    fn test_derive_a_nested_name_is_a_column_mismatch_and_an_absent_one_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            nested_schema(),
            testing::unpartitioned(),
        );
        let snapshot = snapshot_with_schema(None);
        let derive_value = |value_column: &str| {
            TableLayout::derive(
                TABLE,
                &md,
                &UnreadDefaults::default(),
                &snapshot,
                KEY_COLUMN,
                &[value_column],
                None,
            )
        };
        let dotted = format!("{NESTED_OUTER}.{NESTED_INNER}");
        assert_eq!(
            refusal_of(derive_value(&dotted)),
            RefusalReason::ColumnMismatch {
                column: dotted.clone(),
                path: None,
                mismatch: ColumnMismatch::NotTopLevel,
            }
        );
        let absent = format!("{NESTED_OUTER}.{ABSENT_COLUMN}");
        assert_eq!(
            fault_of(derive_value(&absent)),
            TableFault::ColumnMissing { column: absent }
        );
    }

    #[test]
    fn test_derive_takes_a_fixed_width_and_the_initial_default_and_refuses_an_unread_one() {
        let dir = tempfile::tempdir().unwrap();
        let default = vec![7_u8; crate::approx::length(FIXED_VALUE_BYTES)];
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    KEY_FIELD_ID,
                    KEY_COLUMN,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    VALUE_FIELD_ID,
                    VALUE_COLUMN,
                    Type::Primitive(PrimitiveType::Fixed(FIXED_VALUE_BYTES)),
                )
                .with_initial_default(Literal::fixed(default.clone()))
                .into(),
            ])
            .build()
            .unwrap();
        let md = testing::metadata_of_version(
            &testing::file_root(dir.path()),
            schema,
            testing::unpartitioned(),
            iceberg::spec::FormatVersion::V3,
        );
        let snapshot = snapshot_with_schema(None);
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(layout.value_columns[0].width(), Some(FIXED_VALUE_BYTES));
        assert_eq!(layout.value_columns[0].default, Some(Bytes::from(default)));
        let unread = UnreadDefaults::naming(md.current_schema_id(), VALUE_FIELD_ID);
        assert_eq!(
            refusal_of(TableLayout::derive(
                TABLE,
                &md,
                &unread,
                &snapshot,
                KEY_COLUMN,
                &[VALUE_COLUMN],
                None,
            )),
            RefusalReason::ColumnMismatch {
                column: VALUE_COLUMN.to_string(),
                path: None,
                mismatch: ColumnMismatch::UnreadableDefault,
            }
        );
    }

    #[test]
    fn test_derive_bucketing_present_and_absent() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let unbucketed = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let layout = derive(&unbucketed, &snapshot_with_schema(None)).unwrap();
        assert_eq!(layout.bucketing, None);
        let bucketed =
            testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let routed = derive(&bucketed, &snapshot_with_schema(None)).unwrap();
        assert_eq!(routed.bucketing, Some(bucketing(BUCKETS)));
        for key in SAMPLE_KEYS.map(Key::Long) {
            assert_eq!(
                bucketing(BUCKETS).bucket_of(&key),
                bucket::bucket(&key, BUCKETS)
            );
        }
    }

    /// A spec bucketing the key twice, by `BUCKETS` and then `MORE_BUCKETS`.
    fn twice_bucketed() -> iceberg::spec::UnboundPartitionSpec {
        iceberg::spec::UnboundPartitionSpec::builder()
            .add_partition_field(KEY_FIELD_ID, "first_bucket", Transform::Bucket(BUCKETS))
            .unwrap()
            .add_partition_field(
                KEY_FIELD_ID,
                "second_bucket",
                Transform::Bucket(MORE_BUCKETS),
            )
            .unwrap()
            .build()
    }

    #[tokio::test]
    async fn test_derive_and_list_files_take_the_first_bucket_field_on_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), twice_bucketed());
        let spec_id = md.default_partition_spec_id();
        let layout = derive(&md, &snapshot_with_schema(None)).unwrap();
        assert_eq!(layout.bucketing, Some(bucketing(BUCKETS)));
        // The partition tuple carries both buckets; the first is the route.
        let (first_bucket, second_bucket) = (2, 5);
        let path = data_path(&root, "twice");
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![(
                    testing::data_file(
                        &path,
                        spec_id,
                        iceberg::spec::Struct::from_iter([
                            Some(iceberg::spec::Literal::int(first_bucket)),
                            Some(iceberg::spec::Literal::int(second_bucket)),
                        ]),
                        RECORDS,
                        SIZE,
                        None,
                        DataContentType::Data,
                    ),
                    Written::Added,
                )],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let entries = data_files(TABLE, &md, &snapshot, &layout, &storage())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucketing, Some(bucketing(BUCKETS)));
        assert_eq!(entries[0].bucket, u32::try_from(first_bucket).unwrap());
    }

    #[test]
    fn test_derive_zero_buckets_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            testing::long_schema(),
            testing::bucket_spec(0),
        );
        assert_eq!(
            fault_of(derive(&md, &snapshot_with_schema(None))),
            TableFault::ZeroBuckets {
                spec_id: md.default_partition_spec_id(),
            }
        );
    }

    #[tokio::test]
    async fn test_list_files_skips_deleted_entries_and_orders_by_bucket_and_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let mut deleted = live(&root, "f3", spec_id, Some(0), Some((LOW, HIGH)));
        deleted.1 = Written::Deleted(SEQUENCE_A);
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "f2", spec_id, Some(3), Some((LOWER, HIGHER))),
                    live(&root, "f0", spec_id, Some(1), Some((LOW, HIGH))),
                    deleted,
                    live(&root, "f1", spec_id, Some(1), None),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        let storage = storage();
        let entries = data_files(TABLE, &md, &snapshot, &layout, &storage)
            .await
            .unwrap();
        assert_eq!(storage.stats().gets(), 2, "the list and one manifest");
        let entry = |name: &str, bucket: u32, bounds: Option<(i64, i64)>| DataFileEntry {
            path: data_path(&root, name),
            bucket,
            bucketing: Some(bucketing(BUCKETS)),
            records: RECORDS,
            sequence: SequenceNumber(SEQUENCE_A),
            added_by: SnapshotId(SNAPSHOT_A),
            size: SIZE,
            key_min: bounds.map(|(min, _)| Key::Long(min)),
            key_max: bounds.map(|(_, max)| Key::Long(max)),
            partition: Partition::new(
                spec_id,
                testing::partition(testing::Partition::Bucket(bucket)),
            ),
        };
        assert_eq!(
            entries,
            vec![
                entry("f1", 1, None),
                entry("f0", 1, Some((LOW, HIGH))),
                entry("f2", 3, Some((LOWER, HIGHER))),
            ]
        );
    }

    #[tokio::test]
    async fn test_list_files_unpartitioned_files_carry_no_bucketing() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let spec_id = md.default_partition_spec_id();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "b", spec_id, None, Some((LOW, HIGH))),
                    live(&root, "a", spec_id, None, Some((LOW, HIGH))),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        let entries = data_files(TABLE, &md, &snapshot, &layout, &storage())
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, [data_path(&root, "a"), data_path(&root, "b")]);
        for entry in &entries {
            assert_eq!((entry.bucketing, entry.bucket), (None, UNBUCKETED));
        }
    }

    #[tokio::test]
    async fn test_list_files_lists_position_deletes_and_vectors_from_delete_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let delete = |name: &str, spec: DeleteSpec, written: Written| {
            (
                testing::delete_file(
                    &data_path(&root, name),
                    spec_id,
                    testing::partition(testing::Partition::Bucket(1)),
                    spec,
                ),
                written,
            )
        };
        let (offset, length) = (4, 40);
        let f0 = data_path(&root, "f0");
        let vector = || DeleteSpec::Vector {
            referenced: Some(f0.clone()),
            offset: Some(offset),
            length: Some(length),
        };
        let with_deletes = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            None,
            SEQUENCE_B,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "f0", spec_id, Some(1), None)],
                },
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Deletes,
                    entries: vec![
                        delete("v0", vector(), Written::Added),
                        delete(
                            "d0",
                            DeleteSpec::Positions(DataFileFormat::Parquet),
                            EXISTING_FROM_A,
                        ),
                    ],
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, with_deletes.clone());
        let layout = derive(&md, &with_deletes).unwrap();
        let storage = storage();
        let files = list_files(TABLE, &md, &with_deletes, &layout, &storage)
            .await
            .unwrap();
        assert_eq!(storage.stats().gets(), 3, "the list and both manifests");
        assert_eq!(files.data.len(), 1);
        let mut deletes = files.deletes.clone();
        deletes.sort_by(|a, b| a.path.cmp(&b.path));
        let entry = |name: &str, sequence: i64, referenced: Option<String>, kind: DeleteKind| {
            DeleteFileEntry {
                path: data_path(&root, name),
                records: testing::DELETE_RECORDS,
                size: testing::DELETE_BYTES,
                sequence: SequenceNumber(sequence),
                referenced,
                partition: Partition::new(
                    spec_id,
                    testing::partition(testing::Partition::Bucket(1)),
                ),
                kind,
            }
        };
        assert_eq!(
            deletes,
            [
                entry("d0", SEQUENCE_A, None, DeleteKind::Positions),
                entry(
                    "v0",
                    SEQUENCE_B,
                    Some(f0.clone()),
                    DeleteKind::Vector {
                        offset: u64::try_from(offset).unwrap(),
                        length: u64::try_from(length).unwrap(),
                    }
                ),
            ],
            "an existing entry keeps its sequence number and an added one inherits the manifest's"
        );
    }

    /// The paths of the delete files `files` lists, sorted: nothing reads
    /// their order.
    fn delete_paths(files: &SnapshotFiles) -> Vec<&str> {
        let mut paths: Vec<&str> = files
            .deletes
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        paths.sort_unstable();
        paths
    }

    /// `md` with a snapshot `SNAPSHOT_B` at `SEQUENCE_B` listing `data` in
    /// a data manifest and then each of `delete_manifests` in a delete
    /// manifest of its own, all under `md`'s default spec, and the
    /// snapshot.
    async fn with_deletes(
        md: TableMetadata,
        data: Vec<(iceberg::spec::DataFile, Written)>,
        delete_manifests: Vec<Vec<(iceberg::spec::DataFile, Written)>>,
    ) -> (TableMetadata, Snapshot) {
        let spec_id = md.default_partition_spec_id();
        let data_manifest = testing::ManifestSpec {
            spec_id,
            content: ManifestContentType::Data,
            entries: data,
        };
        let manifests = std::iter::once(data_manifest)
            .chain(
                delete_manifests
                    .into_iter()
                    .map(|entries| testing::ManifestSpec {
                        spec_id,
                        content: ManifestContentType::Deletes,
                        entries,
                    }),
            )
            .collect();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            None,
            SEQUENCE_B,
            md.current_schema_id(),
            manifests,
        )
        .await;
        (testing::with_snapshot(md, snapshot.clone()), snapshot)
    }

    /// `list_files` of `with_deletes`'s snapshot with `deletes` in one
    /// delete manifest.
    async fn listed(
        root: &str,
        data: Vec<(iceberg::spec::DataFile, Written)>,
        deletes: Vec<(iceberg::spec::DataFile, Written)>,
    ) -> Result<SnapshotFiles> {
        listed_across(root, data, vec![deletes]).await
    }

    /// `list_files` of `with_deletes`'s snapshot of unpartitioned
    /// metadata.
    async fn listed_across(
        root: &str,
        data: Vec<(iceberg::spec::DataFile, Written)>,
        delete_manifests: Vec<Vec<(iceberg::spec::DataFile, Written)>>,
    ) -> Result<SnapshotFiles> {
        let md = testing::metadata(root, testing::long_schema(), testing::unpartitioned());
        listed_under(md, data, delete_manifests).await
    }

    /// `list_files` of `with_deletes`'s snapshot of `md`.
    async fn listed_under(
        md: TableMetadata,
        data: Vec<(iceberg::spec::DataFile, Written)>,
        delete_manifests: Vec<Vec<(iceberg::spec::DataFile, Written)>>,
    ) -> Result<SnapshotFiles> {
        let (md, snapshot) = with_deletes(md, data, delete_manifests).await;
        let layout = derive(&md, &snapshot).unwrap();
        list_files(TABLE, &md, &snapshot, &layout, &storage()).await
    }

    /// An unpartitioned delete file entry named `name`.
    fn unpartitioned_delete(root: &str, name: &str, spec: DeleteSpec) -> iceberg::spec::DataFile {
        testing::delete_file(
            &data_path(root, name),
            UNPARTITIONED_SPEC,
            testing::partition(testing::Partition::None),
            spec,
        )
    }

    /// An unpartitioned data file entry named `name`.
    fn unpartitioned_data(root: &str, name: &str) -> iceberg::spec::DataFile {
        live(root, name, UNPARTITIONED_SPEC, None, Some((LOW, HIGH))).0
    }

    #[tokio::test]
    async fn test_list_files_skips_delete_entries_that_are_themselves_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let data = vec![(unpartitioned_data(&root, "f0"), Written::Added)];
        let vector = DeleteSpec::Vector {
            referenced: Some(data_path(&root, "f0")),
            offset: Some(0),
            length: Some(1),
        };
        let deletes = vec![
            (
                unpartitioned_delete(&root, "v0", vector),
                Written::Deleted(SEQUENCE_A),
            ),
            (
                unpartitioned_delete(&root, "d0", DeleteSpec::Positions(DataFileFormat::Parquet)),
                Written::Deleted(SEQUENCE_A),
            ),
            (
                unpartitioned_delete(&root, "eq", DeleteSpec::Equality),
                Written::Deleted(SEQUENCE_A),
            ),
        ];
        let files = listed(&root, data, deletes).await.unwrap();
        assert_eq!(files.data.len(), 1);
        assert_eq!(files.deletes, []);
    }

    #[tokio::test]
    async fn test_list_files_refuses_an_equality_delete_above_the_oldest_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        // The older data file at A, a newer one and the delete at B: the
        // delete may remove rows of the older file.
        let data = vec![
            (unpartitioned_data(&root, "old"), EXISTING_FROM_A),
            (unpartitioned_data(&root, "new"), Written::Added),
        ];
        let deletes = vec![
            (
                unpartitioned_delete(&root, "eq1", DeleteSpec::Equality),
                Written::Added,
            ),
            (
                unpartitioned_delete(&root, "eq0", DeleteSpec::Equality),
                Written::Added,
            ),
        ];
        assert_eq!(
            refusal_of(listed(&root, data, deletes).await),
            RefusalReason::EqualityDeletes {
                path: data_path(&root, "eq0"),
            },
            "the least path of the live ones"
        );
    }

    #[tokio::test]
    async fn test_list_files_drops_an_equality_delete_no_data_file_is_older_than() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        // A compaction rewrote the data at B and left the delete at A, and
        // one more delete at B: neither is above any data file.
        let data = vec![(unpartitioned_data(&root, "compacted"), Written::Added)];
        let deletes = vec![
            (
                unpartitioned_delete(&root, "eq_old", DeleteSpec::Equality),
                EXISTING_FROM_A,
            ),
            (
                unpartitioned_delete(&root, "eq_same", DeleteSpec::Equality),
                Written::Added,
            ),
        ];
        let files = listed(&root, data, deletes.clone()).await.unwrap();
        assert_eq!(files.data.len(), 1);
        assert_eq!(files.deletes, [], "a dangling equality delete is dropped");
        // With no data file at all, an equality delete applies to nothing.
        let files = listed(&root, Vec::new(), deletes).await.unwrap();
        assert_eq!(
            files,
            SnapshotFiles {
                data: Vec::new(),
                deletes: Vec::new()
            }
        );
        // Beside a readable delete, the dangling one leaves the readable one
        // to decide.
        let data = vec![(unpartitioned_data(&root, "compacted"), Written::Added)];
        let deletes = vec![
            (
                unpartitioned_delete(&root, "eq_old", DeleteSpec::Equality),
                EXISTING_FROM_A,
            ),
            (
                unpartitioned_delete(&root, "pos", DeleteSpec::Positions(DataFileFormat::Parquet)),
                Written::Added,
            ),
        ];
        let files = listed(&root, data, deletes).await.unwrap();
        assert_eq!(delete_paths(&files), [data_path(&root, "pos")]);
    }

    #[tokio::test]
    async fn test_list_files_refuses_avro_and_orc_position_deletes_before_equality_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let data = || vec![(unpartitioned_data(&root, "old"), EXISTING_FROM_A)];
        let cases = [
            (DataFileFormat::Avro, UnsupportedDelete::AvroPositions),
            (DataFileFormat::Orc, UnsupportedDelete::OrcPositions),
        ];
        for (format, kind) in cases {
            let equality = vec![(
                unpartitioned_delete(&root, "a_live_equality", DeleteSpec::Equality),
                Written::Added,
            )];
            let positions = vec![
                (
                    unpartitioned_delete(&root, "z_positions", DeleteSpec::Positions(format)),
                    Written::Added,
                ),
                (
                    unpartitioned_delete(&root, "y_positions", DeleteSpec::Positions(format)),
                    Written::Added,
                ),
            ];
            let orders = [
                vec![equality.clone(), positions.clone()],
                vec![positions, equality],
            ];
            for delete_manifests in orders {
                assert_eq!(
                    refusal_of(listed_across(&root, data(), delete_manifests).await),
                    RefusalReason::DeleteKindUnsupported {
                        path: data_path(&root, "y_positions"),
                        kind: kind.clone(),
                    },
                    "{format:?} in either manifest order"
                );
            }
        }
    }

    /// An unpartitioned data file entry named `name` recording `format`;
    /// nothing is written there.
    fn unpartitioned_data_in(
        root: &str,
        name: &str,
        format: DataFileFormat,
    ) -> iceberg::spec::DataFile {
        iceberg::spec::DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(data_path(root, name))
            .file_format(format)
            .partition(testing::partition(testing::Partition::None))
            .partition_spec_id(UNPARTITIONED_SPEC)
            .record_count(RECORDS)
            .file_size_in_bytes(SIZE)
            .build()
            .unwrap()
    }

    /// A data file entry in ORC, Avro, or Puffin is refused from the
    /// manifests, the least such path named whatever the manifest order,
    /// ahead of an unreadable position delete and a live equality delete
    /// that would each refuse the snapshot too.
    #[tokio::test]
    async fn test_list_files_refuses_a_data_file_not_in_parquet_before_any_delete() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let cases = [
            (DataFileFormat::Orc, UnsupportedFormat::Orc),
            (DataFileFormat::Avro, UnsupportedFormat::Avro),
            (DataFileFormat::Puffin, UnsupportedFormat::Puffin),
        ];
        for (format, named) in cases {
            let foreign =
                |name: &str| (unpartitioned_data_in(&root, name, format), EXISTING_FROM_A);
            let parquet = (unpartitioned_data(&root, "parquet"), EXISTING_FROM_A);
            let deletes = vec![
                (
                    unpartitioned_delete(
                        &root,
                        "avro_positions",
                        DeleteSpec::Positions(DataFileFormat::Avro),
                    ),
                    Written::Added,
                ),
                (
                    unpartitioned_delete(&root, "equality", DeleteSpec::Equality),
                    Written::Added,
                ),
            ];
            let orders = [
                vec![foreign("z_foreign"), parquet.clone(), foreign("y_foreign")],
                vec![foreign("y_foreign"), foreign("z_foreign"), parquet],
            ];
            for data in orders {
                assert_eq!(
                    refusal_of(listed(&root, data, deletes.clone()).await),
                    RefusalReason::DataFormatUnsupported {
                        path: data_path(&root, "y_foreign"),
                        format: named,
                    },
                    "{format:?} in either order"
                );
            }
        }
    }

    /// An Avro or ORC position delete that provably applies to nothing is
    /// dropped, as a dangling readable delete is: one older than every
    /// data file, one naming a data file the snapshot does not list, and
    /// one naming a listed file newer than itself. One that may apply is
    /// refused.
    #[tokio::test]
    async fn test_list_files_drops_unreadable_position_deletes_that_apply_to_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let data = || {
            vec![(
                unpartitioned_data(&root, "new"),
                Written::AddedAt(SEQUENCE_B),
            )]
        };
        let avro = DataFileFormat::Avro;
        let scoped = |name: &str, referenced: &str, written| {
            let spec = DeleteSpec::ScopedPositions(avro, data_path(&root, referenced));
            (unpartitioned_delete(&root, name, spec), written)
        };
        let dangling = vec![
            (
                unpartitioned_delete(&root, "older", DeleteSpec::Positions(avro)),
                EXISTING_FROM_A,
            ),
            scoped("gone", "gone", Written::Added),
            scoped("before", "new", EXISTING_FROM_A),
        ];
        let files = listed(&root, data(), dangling.clone()).await.unwrap();
        assert!(files.deletes.is_empty());
        let mut applying = dangling;
        applying.push(scoped("same", "new", Written::Added));
        assert_eq!(
            refusal_of(listed(&root, data(), applying).await),
            RefusalReason::DeleteKindUnsupported {
                path: data_path(&root, "same"),
                kind: UnsupportedDelete::AvroPositions,
            }
        );
    }

    /// A deletion vector that applies to a data file masks the file's
    /// position deletes, so an Avro or ORC one naming only that file
    /// applies to nothing and is dropped, the vector kept. Where no vector
    /// applies to the file, one older than it or one naming another file,
    /// the position delete may apply and is refused.
    #[tokio::test]
    async fn test_list_files_drops_an_unreadable_position_delete_a_vector_masks() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let target = data_path(&root, "target");
        let vector = |referenced: &str, written| {
            let spec = DeleteSpec::Vector {
                referenced: Some(data_path(&root, referenced)),
                offset: Some(0),
                length: Some(1),
            };
            (unpartitioned_delete(&root, "vector", spec), written)
        };
        let deletes = |vector| {
            let spec = DeleteSpec::ScopedPositions(DataFileFormat::Avro, target.clone());
            vec![
                vector,
                (unpartitioned_delete(&root, "avro", spec), Written::Added),
            ]
        };
        let data = |written| {
            vec![
                (unpartitioned_data(&root, "target"), written),
                (unpartitioned_data(&root, "other"), Written::Added),
            ]
        };
        let masked = deletes(vector("target", Written::Added));
        let files = listed(&root, data(EXISTING_FROM_A), masked).await.unwrap();
        assert_eq!(delete_paths(&files), [data_path(&root, "vector")]);
        let refused = RefusalReason::DeleteKindUnsupported {
            path: data_path(&root, "avro"),
            kind: UnsupportedDelete::AvroPositions,
        };
        let older = deletes(vector("target", EXISTING_FROM_A));
        assert_eq!(
            refusal_of(listed(&root, data(Written::Added), older).await),
            refused,
            "a vector older than the file does not apply to it"
        );
        let elsewhere = deletes(vector("other", Written::Added));
        assert_eq!(
            refusal_of(listed(&root, data(EXISTING_FROM_A), elsewhere).await),
            refused,
            "a vector of another file does not mask this one"
        );
    }

    /// An Avro position delete that names no data file applies only where
    /// a data file of its partition at a sequence number it reaches is one
    /// no deletion vector masks. A masked file it reaches, an unmasked one
    /// newer than it, and an unmasked one of another partition leave it
    /// applying to nothing; an unmasked file of its partition it reaches
    /// gets it refused.
    #[tokio::test]
    async fn test_list_files_drops_an_unreadable_unscoped_delete_whose_partition_is_masked() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let (own, other) = (0, 1);
        let data = |name: &str, bucket: u32, written: Written| {
            (live(&root, name, spec_id, Some(bucket), None).0, written)
        };
        let delete = |name: &str, bucket: u32, spec: DeleteSpec, written: Written| {
            let partition = testing::partition(testing::Partition::Bucket(bucket));
            let entry = testing::delete_file(&data_path(&root, name), spec_id, partition, spec);
            (entry, written)
        };
        let vector = |name: &str, bucket: u32| {
            let spec = DeleteSpec::Vector {
                referenced: Some(data_path(&root, name)),
                offset: Some(0),
                length: Some(1),
            };
            delete(&format!("{name}_vector"), bucket, spec, Written::Added)
        };
        let avro = || {
            let spec = DeleteSpec::Positions(DataFileFormat::Avro);
            delete("avro", own, spec, EXISTING_FROM_A)
        };
        let mut files = vec![
            data("masked", own, EXISTING_FROM_A),
            data("newer", own, Written::Added),
            data("elsewhere", other, EXISTING_FROM_A),
        ];
        let deletes = vec![avro(), vector("masked", own)];
        let listed = listed_under(md.clone(), files.clone(), vec![deletes.clone()])
            .await
            .unwrap();
        assert_eq!(delete_paths(&listed), [data_path(&root, "masked_vector")]);
        files.push(data("unmasked", own, EXISTING_FROM_A));
        assert_eq!(
            refusal_of(listed_under(md, files, vec![deletes]).await),
            RefusalReason::DeleteKindUnsupported {
                path: data_path(&root, "avro"),
                kind: UnsupportedDelete::AvroPositions,
            }
        );
    }

    #[tokio::test]
    async fn test_list_files_vector_without_its_placement_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let referenced = Some(data_path(&root, "old"));
        let negative = -1;
        let unplaced = [
            (None, Some(0), Some(1)),
            (referenced.clone(), None, Some(1)),
            (referenced.clone(), Some(0), None),
            (referenced, Some(negative), Some(1)),
        ];
        for (referenced, offset, length) in unplaced {
            let deletes = vec![(
                unpartitioned_delete(
                    &root,
                    "vectors",
                    DeleteSpec::Vector {
                        referenced,
                        offset,
                        length,
                    },
                ),
                Written::Added,
            )];
            let data = vec![(unpartitioned_data(&root, "old"), Written::Added)];
            assert_eq!(
                fault_of(listed(&root, data, deletes).await),
                TableFault::VectorUnplaced {
                    path: data_path(&root, "vectors"),
                }
            );
        }
    }

    /// Metadata whose default spec buckets by `MORE_BUCKETS` after a first
    /// spec of `BUCKETS`, with both spec ids.
    fn evolved(root: &str) -> (TableMetadata, i32, i32) {
        let first = testing::metadata(root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let first_spec = first.default_partition_spec_id();
        let md = first
            .into_builder(None)
            .add_partition_spec(testing::bucket_spec(MORE_BUCKETS))
            .unwrap()
            .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let second_spec = md.default_partition_spec_id();
        assert_ne!(first_spec, second_spec);
        (md, first_spec, second_spec)
    }

    #[tokio::test]
    async fn test_list_files_evolved_spec_gives_each_manifests_bucketing() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let (md, first_spec, second_spec) = evolved(&root);
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id: second_spec,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "new", second_spec, Some(5), Some((LOW, HIGH)))],
                },
                testing::ManifestSpec {
                    spec_id: first_spec,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "old", first_spec, Some(2), Some((LOW, HIGH)))],
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(
            layout.bucketing,
            Some(bucketing(MORE_BUCKETS)),
            "the default spec's"
        );
        let entries = data_files(TABLE, &md, &snapshot, &layout, &storage())
            .await
            .unwrap();
        let routed: Vec<(&str, Option<Bucketing>, u32)> = entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.bucketing, entry.bucket))
            .collect();
        assert_eq!(
            routed,
            [
                (
                    data_path(&root, "old").as_str(),
                    Some(bucketing(BUCKETS)),
                    2
                ),
                (
                    data_path(&root, "new").as_str(),
                    Some(bucketing(MORE_BUCKETS)),
                    5
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_list_files_missing_spec_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let (two_specs, _, second_spec) = evolved(&root);
        let snapshot = testing::write_snapshot(
            &two_specs,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            two_specs.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id: second_spec,
                content: ManifestContentType::Data,
                entries: vec![live(&root, "new", second_spec, Some(5), None)],
            }],
        )
        .await;
        // Metadata that never saw the second spec lists the same manifests.
        let one_spec = testing::with_snapshot(
            testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS)),
            snapshot.clone(),
        );
        assert!(one_spec.partition_spec_by_id(second_spec).is_none());
        let layout = derive(&one_spec, &snapshot).unwrap();
        assert_eq!(
            fault_of(data_files(TABLE, &one_spec, &snapshot, &layout, &storage()).await),
            TableFault::SpecMissing {
                spec_id: second_spec,
            }
        );
    }

    #[tokio::test]
    async fn test_list_files_null_bucket_value_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let path = data_path(&root, "nobucket");
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![(
                    testing::data_file(
                        &path,
                        spec_id,
                        testing::partition(testing::Partition::NullBucket),
                        RECORDS,
                        SIZE,
                        None,
                        DataContentType::Data,
                    ),
                    Written::Added,
                )],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(
            fault_of(data_files(TABLE, &md, &snapshot, &layout, &storage()).await),
            TableFault::BucketValueMissing { path, spec_id }
        );
    }

    /// A bucket value at the spec's count names a bucket no key hashes to;
    /// admitted, the file would never be consulted and its keys answered
    /// absent.
    #[tokio::test]
    async fn test_list_files_bucket_value_at_or_past_the_count_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "f0", spec_id, Some(BUCKETS - 1), Some((LOW, HIGH))),
                    live(&root, "f1", spec_id, Some(BUCKETS), Some((LOW, HIGH))),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(
            fault_of(data_files(TABLE, &md, &snapshot, &layout, &storage()).await),
            TableFault::BucketValueOutOfRange {
                path: data_path(&root, "f1"),
                spec_id,
                bucket: BUCKETS,
                count: BUCKETS,
            }
        );
    }

    #[tokio::test]
    async fn test_list_files_order_is_the_same_for_shuffled_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let files = || {
            vec![
                live(&root, "f0", spec_id, Some(1), Some((LOW, HIGH))),
                live(&root, "f1", spec_id, Some(1), None),
                live(&root, "f2", spec_id, Some(3), Some((LOWER, HIGHER))),
                live(&root, "f3", spec_id, Some(0), Some((LOW, HIGH))),
            ]
        };
        let forward = files();
        // Written at the first snapshot's sequence number, so that the
        // listings differ in manifest order alone.
        let mut backward: Vec<_> = files()
            .into_iter()
            .map(|(file, _)| (file, Written::AddedAt(SEQUENCE_A)))
            .collect();
        backward.reverse();
        let (first_half, second_half) = backward.split_at(2);
        let in_order = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: forward,
            }],
        )
        .await;
        let shuffled = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            None,
            SEQUENCE_B,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: first_half.to_vec(),
                },
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: second_half.to_vec(),
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, in_order.clone());
        let md = testing::with_snapshot(md, shuffled.clone());
        let layout = derive(&md, &in_order).unwrap();
        let storage = storage();
        let a = data_files(TABLE, &md, &in_order, &layout, &storage)
            .await
            .unwrap();
        // The second listing's manifests were written by another snapshot,
        // whose id each added entry inherits; the rest is the same.
        let b: Vec<DataFileEntry> = data_files(TABLE, &md, &shuffled, &layout, &storage)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| {
                assert_eq!(entry.added_by, SnapshotId(SNAPSHOT_B));
                DataFileEntry {
                    added_by: SnapshotId(SNAPSHOT_A),
                    ..entry
                }
            })
            .collect();
        assert_eq!(a, b);
        assert_eq!(a.len(), 4);
        let order: Vec<(u32, Option<Key>)> = a
            .iter()
            .map(|entry| (entry.bucket, entry.key_min.clone()))
            .collect();
        assert_eq!(
            order,
            [
                (0, Some(Key::Long(LOW))),
                (1, None),
                (1, Some(Key::Long(LOW))),
                (3, Some(Key::Long(LOWER))),
            ]
        );
    }

    /// Every data file of `snapshot` with its data sequence number, in
    /// listing order.
    async fn sequences(md: &TableMetadata, snapshot: &Snapshot) -> Vec<(String, SequenceNumber)> {
        let layout = derive(md, snapshot).unwrap();
        data_files(TABLE, md, snapshot, &layout, &storage())
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.path, entry.sequence))
            .collect()
    }

    #[test]
    fn test_added_snapshot_inherits_the_manifest_s_when_the_entry_leaves_it_null() {
        assert_eq!(
            added_snapshot_of(None, MANIFEST_SNAPSHOT),
            SnapshotId(MANIFEST_SNAPSHOT)
        );
        assert_eq!(
            added_snapshot_of(Some(ENTRY_SNAPSHOT), MANIFEST_SNAPSHOT),
            SnapshotId(ENTRY_SNAPSHOT)
        );
    }

    /// Every data file of `snapshot` with the snapshot that added it, in
    /// listing order.
    async fn added_by(md: &TableMetadata, snapshot: &Snapshot) -> Vec<(String, SnapshotId)> {
        let layout = derive(md, snapshot).unwrap();
        data_files(TABLE, md, snapshot, &layout, &storage())
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.path, entry.added_by))
            .collect()
    }

    #[tokio::test]
    async fn test_list_files_records_the_snapshot_that_added_each_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let spec_id = md.default_partition_spec_id();
        let file =
            |name: &str, written: Written| (live(&root, name, spec_id, None, None).0, written);
        let commit = |md: TableMetadata,
                      id: i64,
                      parent: Option<i64>,
                      sequence: i64,
                      entries: Vec<(iceberg::spec::DataFile, Written)>| {
            let spec = testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries,
            };
            async move {
                let snapshot = testing::write_snapshot(
                    &md,
                    id,
                    parent,
                    sequence,
                    md.current_schema_id(),
                    vec![spec],
                )
                .await;
                (testing::with_snapshot(md, snapshot.clone()), snapshot)
            }
        };
        let carried = |sequence: i64, added_by: i64| Written::Existing { sequence, added_by };
        let (md, first) = commit(
            md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            vec![file("first", Written::Added)],
        )
        .await;
        assert_eq!(
            added_by(&md, &first).await,
            [(data_path(&root, "first"), SnapshotId(SNAPSHOT_A))],
            "an added entry leaves its snapshot to the manifest"
        );
        let (md, second) = commit(
            md,
            SNAPSHOT_B,
            Some(SNAPSHOT_A),
            SEQUENCE_B,
            vec![
                file("first", carried(SEQUENCE_A, SNAPSHOT_A)),
                file("second", Written::Added),
            ],
        )
        .await;
        let (md, third) = commit(
            md,
            SNAPSHOT_C,
            Some(SNAPSHOT_B),
            SEQUENCE_C,
            vec![
                file("first", carried(SEQUENCE_A, SNAPSHOT_A)),
                file("second", carried(SEQUENCE_B, SNAPSHOT_B)),
            ],
        )
        .await;
        assert_ne!(second.snapshot_id(), third.snapshot_id());
        assert_eq!(
            added_by(&md, &third).await,
            [
                (data_path(&root, "first"), SnapshotId(SNAPSHOT_A)),
                (data_path(&root, "second"), SnapshotId(SNAPSHOT_B)),
            ],
            "each file is attributed to its own append, not to the listing snapshot"
        );
    }

    #[tokio::test]
    async fn test_list_files_takes_an_explicit_sequence_number_and_inherits_a_null_added_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let spec_id = md.default_partition_spec_id();
        let file =
            |name: &str, written: Written| (live(&root, name, spec_id, None, None).0, written);
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            None,
            SEQUENCE_B,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    file("existing", EXISTING_FROM_A),
                    file("explicit", Written::AddedAt(SEQUENCE_A)),
                    file("inherited", Written::Added),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        assert_eq!(
            sequences(&md, &snapshot).await,
            [
                (data_path(&root, "existing"), SequenceNumber(SEQUENCE_A)),
                (data_path(&root, "explicit"), SequenceNumber(SEQUENCE_A)),
                (data_path(&root, "inherited"), SequenceNumber(SEQUENCE_B)),
            ]
        );
    }

    /// iceberg-rust parses every v1 manifest entry with the initial
    /// sequence number as its own, so this pins that icetray keeps such
    /// entries there. It does not reach `inherited_sequence`'s branch for a
    /// manifest at the initial sequence number, since no entry arrives
    /// without one; `test_data_sequence_existing_entry_without_one_is_a_table_fault`
    /// exercises that branch.
    #[tokio::test]
    async fn test_list_files_keeps_v1_entries_at_the_initial_sequence_number() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata_of_version(
            &root,
            testing::long_schema(),
            testing::unpartitioned(),
            iceberg::spec::FormatVersion::V1,
        );
        let spec_id = md.default_partition_spec_id();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            INITIAL_SEQUENCE_NUMBER,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "added", spec_id, None, None),
                    (
                        live(&root, "existing", spec_id, None, None).0,
                        Written::Existing {
                            sequence: INITIAL_SEQUENCE_NUMBER,
                            added_by: SNAPSHOT_A,
                        },
                    ),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let initial = SequenceNumber(INITIAL_SEQUENCE_NUMBER);
        assert_eq!(
            sequences(&md, &snapshot).await,
            [
                (data_path(&root, "added"), initial),
                (data_path(&root, "existing"), initial),
            ]
        );
    }

    #[test]
    fn test_inherited_sequence_follows_icebergs_rule() {
        let (own, manifest) = (SEQUENCE_A, SEQUENCE_B);
        let cases = [
            (Some(own), ManifestStatus::Added, manifest, Some(own)),
            (Some(own), ManifestStatus::Existing, manifest, Some(own)),
            (None, ManifestStatus::Added, manifest, Some(manifest)),
            (None, ManifestStatus::Existing, manifest, None),
            (None, ManifestStatus::Deleted, manifest, None),
            (
                None,
                ManifestStatus::Existing,
                INITIAL_SEQUENCE_NUMBER,
                Some(INITIAL_SEQUENCE_NUMBER),
            ),
        ];
        for (own, status, manifest, expected) in cases {
            assert_eq!(
                inherited_sequence(own, status, manifest),
                expected.map(SequenceNumber),
                "{own:?} {status:?} under {manifest}"
            );
        }
    }

    /// An existing entry with a null sequence number, which iceberg-rust's
    /// writer refuses to write, taken from a written manifest with the
    /// number cleared: a fault under a manifest past the initial sequence
    /// number, and the manifest's own, inherited, under one at it.
    #[tokio::test]
    async fn test_data_sequence_existing_entry_without_one_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let spec_id = md.default_partition_spec_id();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            None,
            SEQUENCE_B,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![(
                    live(&root, "existing", spec_id, None, None).0,
                    EXISTING_FROM_A,
                )],
            }],
        )
        .await;
        let storage = storage();
        let list = ManifestList::parse_with_version(
            &storage.read_all(snapshot.manifest_list()).await.unwrap(),
            md.format_version(),
        )
        .unwrap();
        let mut manifest = list.entries()[0].clone();
        let parsed =
            Manifest::parse_avro(&storage.read_all(&manifest.manifest_path).await.unwrap())
                .unwrap();
        let mut entry = ManifestEntry::clone(&parsed.entries()[0]);
        assert_eq!(entry.status(), ManifestStatus::Existing);
        entry.sequence_number = None;
        assert_eq!(
            fault_of(data_sequence(TABLE, &entry, &manifest)),
            TableFault::SequenceNumberMissing {
                path: data_path(&root, "existing"),
                manifest: manifest.manifest_path.clone(),
            }
        );
        manifest.sequence_number = INITIAL_SEQUENCE_NUMBER;
        assert_eq!(
            data_sequence(TABLE, &entry, &manifest).unwrap(),
            SequenceNumber(INITIAL_SEQUENCE_NUMBER)
        );
    }
}
