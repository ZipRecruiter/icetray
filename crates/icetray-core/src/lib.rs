//! Everything from bytes to answers for serving lookups by key, and ordered
//! Scans of a key's rows, over Apache Iceberg tables: keys, storage,
//! catalog, manifests, the resident index, the page cache, generations, the
//! doctor, and configuration. The binary and the testkit build on this
//! crate; it depends on neither.

pub mod approx;
mod aws_catalog;
mod blocking;
mod bucket;
mod cache;
pub mod catalog;
pub mod check;
mod column;
pub mod config;
pub mod contract;
pub mod deletes;
pub mod descriptors;
mod error;
pub mod generation;
mod index;
pub mod key;
mod level;
mod local;
mod manifest;
pub mod memory;
pub mod metric;
pub mod open;
pub mod order;
mod parquet_io;
mod projection;
mod row_cache;
mod scan;
mod shard;
mod snapshot;
mod storage;
mod table;
pub mod time;
pub mod varint;

pub use aws_catalog::{AwsCatalogError, AwsSource};
pub use bucket::bucket;
pub use cache::{CacheError, PageCache};
pub use catalog::{TableFault, TableSource};
pub use check::{
    Capping, ColumnCost, ColumnLevel, ColumnSummary, Examined, FileOrderReport, FileReport,
    Finding, FindingCode, LookupCost, OrderSummary, Penalty, ScanLimits, TableReport, TableSummary,
    Unwalked, ValueColumnReport, WriterProperty, diagnose, examine, judge,
};
pub use column::ColumnMismatch;
pub use config::{
    ColumnSetting, Config, ConfigError, ConfigViolation, DEFAULT_MAX_SCAN_ROWS, DEFAULT_RESIDENCY,
    HedgeConfig, LocalConfig, LogConfig, Residency, RewriteFault, RewriteRule, RewriteTextFault,
    S3Config, ServerConfig, SourceConfig, SourceSetting, StandardField, StorageConfig, TableConfig,
    ValueColumnConfig, ValueFormat, least_max_multi_scan_values,
};
pub use deletes::{BlobFraming, DeleteFault, DeleteSummary};
pub use error::{
    Budget, Error, ErrorKind, RefusalReason, Result, UnsupportedDelete, UnsupportedFormat,
};
pub use generation::{Generation, LoadedFile, Lookup, MultiLookup, Row, load_concurrency};
pub use index::{
    ChunkPage, DecodedPart, FileFault, FileIndex, HeaderField, OrderFences, PageHeaderFault,
    PageRef, Pair, PairPart, ResidentBytes, ResidentParts, WireType,
};
pub use key::{Key, KeyBytes, KeyType};
pub use level::{FALL_PER_MILLE, Level, PAGE_CACHE_SAMPLE_INTERVAL, PER_MILLE, RECOVER_PER_MILLE};
pub use local::{DataFileIdentity, DiskChunks, DiskLevel, LocalDir, LocalFault};
pub use manifest::{
    Bucketing, CheckedReads, ColumnRead, ColumnRequestFault, DataFileEntry, TableLayout,
    ValueColumnLayout, ValueType, check_column_reads,
};
pub use open::{Applied, Opened, Refused, Tables, open};
pub use order::{
    Direction, OrderColumnLayout, OrderFacts, OrderType, OrderValue, OrderVariant, ScanBounds,
};
pub use projection::{MAX_FIELD_NUMBER, Malformed, WIRE_TYPE_BITS, WIRE_TYPE_MASK, Wire};
pub use row_cache::RowCache;
pub use scan::{
    BoundSide, Fits, MultiScanLookup, Oversize, ScanLookup, ScanRequestFault, ScanRow, Scanned,
    check_scan_request, next_bound,
};
pub use snapshot::{SequenceNumber, SnapshotId};
pub use storage::{FILE_PREFIX, IoStats, LocationError, LocationFault, Storage};
pub use table::{
    CallsInFlight, FindingLog, ScanReceipt, Table, TableHandles, TableKind, TableState,
};
