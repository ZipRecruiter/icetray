//! Test infrastructure shared across the workspace: an in-process S3 server,
//! a stand-in for the AWS catalogs, the fixture writer and its delete file
//! writers, the ordered fixtures and their scan model (RFD 025), the
//! brute-force merge-on-read oracle, the deterministic simulation
//! transport, a store that counts GETs per object, one that counts the
//! rounds a read's GETs take, and one that logs each GET's range and
//! latency, shared assertions, the golden-table locator (RFD 014), the
//! truth file a check replays against a running server, the lookup
//! workloads the benchmark replays, and the memory headroom it checks.

pub mod aws_catalog;
pub mod counting;
pub mod delete_files;
pub mod error;
pub mod fixture;
pub mod get_log;
pub mod golden;
pub mod headroom;
pub mod keys;
pub mod latency;
pub mod layout;
pub mod merge_oracle;
pub mod ordered;
pub mod protobuf;
pub mod rounds;
pub mod s3;
pub mod sim;
pub mod truth;
pub mod verify;
pub mod workload;

pub use counting::CountingStore;
pub use error::{Result, TestkitError};
pub use fixture::{
    DeleteForm, Fixture, FixtureSpec, HandSnapshot, Nulls, Order, RawFile, memory_catalog_local,
    memory_catalog_s3, write_fixture,
};
pub use get_log::{GetLog, LoggedGet};
/// The catalog `memory_catalog_local` and `memory_catalog_s3` return, so a
/// caller without an `iceberg` dependency can hold it.
pub use iceberg::memory::MemoryCatalog;
pub use keys::{
    KeyShape, ValueKind, absent_key_from, absent_key_near, far_key, key_at, key_at_padded,
    key_at_shaped, scattered_key_at, text_value_for_len, value_for, value_for_len, value_of_kind,
    value_of_version,
};
pub use layout::{Bloom, Dictionary, Layout, Statistics};
pub use merge_oracle::{MergeOracle, RowCopy};
pub use ordered::{
    MINUTE_MICROS, ORDER_COLUMN, ORDER_FIELD_ID, OrderKind, Ordered, RowsPerKey, ScanRows,
    TestScan, follow, middle_half, order_value, row_value, scan_model, scanned_of,
};
pub use rounds::RoundsStore;
pub use s3::S3Fixture;
