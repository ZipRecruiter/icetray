//! The gRPC service over the generated contract (RFD 010): `Get`,
//! `MultiGet`, `Scan`, `MultiScan`, and `TableInfo` over the tables `open`
//! built, the wire key read under the table's key type, and core's errors
//! mapped to status codes by variant. Every call is counted under
//! `requests_total` by the code it answered around the service, not in it
//! (`counted::Counted`). `Get`, `MultiGet`, `Scan`, and `MultiScan` run
//! under the server's deadline for the method (RFD 013); a client's own
//! deadline is tonic's, which drops the call at it. Together they hold at
//! most `server.max_in_flight` slots, and a call that finds none is refused
//! at once. A `MultiGet` asks for at most `server.max_multi_get_values`
//! values, its keys times the columns it names, whatever fields each names
//! (RFD 022, RFD 024); a `MultiScan`'s bound, `server.max_multi_scan_values`,
//! and a Scan's other checks against the table are core's (RFD 025). A Scan
//! or `MultiScan` response is measured as the message that would be sent,
//! and one above `server.max_scan_bytes` is refused with no rows
//! (`scan_within_bound`, `multi_scan_within_bound`).
//!
//! `Get`, `MultiGet`, `Scan`, and `MultiScan` check a request in one order,
//! each step as far as it applies to the call, and answer the first fault
//! found:
//!
//! 1. The request on its own, whatever its table: a missing key, no
//!    direction or one the contract does not name, a bound with no
//!    variant, a limit of 0, a `start` at or above a `before` of its
//!    variant, a column named twice, and a field list with a number out of
//!    range or named twice (`INVALID_ARGUMENT`).
//! 2. The table's configuration, which needs no generation: first a table
//!    not served (`NOT_FOUND`), then a call of the wrong kind, a limit
//!    above `max_scan_rows`, whose fault names that bound, and a
//!    `MultiGet`'s or `MultiScan`'s values (`INVALID_ARGUMENT`).
//! 3. An in-flight slot (`RESOURCE_EXHAUSTED`).
//! 4. The table's readiness (`UNAVAILABLE`).
//! 5. The serving generation: a key of another variant than the key type
//!    or a uuid key that is not sixteen bytes, a bound of another than the
//!    order type's, and a column the table does not serve or a field list
//!    on one not protobuf (`INVALID_ARGUMENT`).
//!
//! So a request whose fault needs no generation is refused for it while
//! the table loads, never `UNAVAILABLE`, which a caller retries. Steps 1
//! and 2 run core's checks (`check_column_reads`, `check_scan_request`,
//! `Table::check_kind`, `Table::check_scan`, `Table::check_multi_scan`),
//! the ones its lookups run first, each against the table's entry as it
//! stands when it runs. So a request that fails them fails here, before it
//! takes a slot, except under a reload that lowers a table's
//! `max_scan_rows` between the two checks: core then refuses a limit this
//! module admitted, with the same code and still ahead of readiness, after
//! the call took its slot.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::Guard;
use bytes::Bytes;
use icetray_core::config::ServerConfig;
use icetray_core::key::UUID_BYTES;
use icetray_core::{
    BoundSide, CacheError, ColumnRead, Direction, Error, Fits, Key, KeyType, MultiScanLookup,
    Opened, OrderType, OrderValue, Oversize, Row, ScanBounds, ScanLookup, ScanReceipt, ScanRow,
    Scanned, Table, TableConfig, TableKind, TableLayout, Tables, ValueFormat, ValueType, approx,
    check_column_reads, check_scan_request, next_bound,
};
use icetray_proto::v1;
use icetray_proto::v1::icetray_server::Icetray;
use icetray_proto::v1::{
    ColumnValue, GetRequest, GetResponse, Hit, MultiGetRequest, MultiGetResponse, MultiScanRequest,
    MultiScanResponse, NullValue, ScanRequest, ScanResponse, TableInfoRequest, TableInfoResponse,
    column_value, order_value,
};
use prost::Message;
use prost::encoding::message;
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tonic::{Request, Response, Status};

use crate::error::{CallFault, WireKeyFault, WireScanFault};

/// The in-flight slots every call but `TableInfo` holds together, at most
/// `server.max_in_flight`: what admits a call, and what
/// `requests_in_flight` reads, so the gauge counts the slots the bound
/// holds and cannot drift from it. Clone shares the slots.
#[derive(Clone, Debug)]
pub struct Slots {
    /// Nothing closes it.
    semaphore: Arc<Semaphore>,
    /// The slots `semaphore` was built with.
    limit: usize,
}

impl Slots {
    /// `max_in_flight` slots, held to tokio's `Semaphore::MAX_PERMITS`,
    /// which is far above any count of calls one process can hold open.
    pub fn new(max_in_flight: usize) -> Self {
        let limit = max_in_flight.min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    /// A slot for one call, held until the call ends, its deadline
    /// included; with none free the call is refused at once, never queued.
    fn admit(&self) -> Result<SemaphorePermit<'_>, CallFault> {
        self.semaphore.try_acquire().map_err(|error| match error {
            // `Closed` is unreachable, since nothing closes the
            // semaphore; were it closed, no call could be admitted.
            TryAcquireError::NoPermits | TryAcquireError::Closed => {
                CallFault::Overloaded { limit: self.limit }
            }
        })
    }

    /// The slots there are: `server.max_in_flight` as the bound enforces it.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// The slots held now, one per call admitted and not yet ended.
    pub fn admitted(&self) -> usize {
        self.limit - self.semaphore.available_permits()
    }
}

/// The served tables and the bounds each call runs under. Clone: tonic
/// hands one clone to every connection, and they share the tables and the
/// in-flight slots.
#[derive(Clone)]
pub struct Service {
    /// The tables a call looks up by name, as the last reload left them
    /// (RFD 018).
    opened: Arc<Opened>,
    get_deadline: Duration,
    multi_get_deadline: Duration,
    scan_deadline: Duration,
    multi_scan_deadline: Duration,
    /// `Get`, `MultiGet`, `Scan`, and `MultiScan` together.
    slots: Slots,
    /// The most values one `MultiGet` may ask for.
    max_multi_get_values: usize,
}

impl Service {
    /// The service over the tables `opened` serves, by configured name,
    /// under `server`'s request bounds.
    pub fn new(opened: &Arc<Opened>, server: &ServerConfig) -> Self {
        Self {
            opened: Arc::clone(opened),
            get_deadline: server.get_deadline,
            multi_get_deadline: server.multi_get_deadline,
            scan_deadline: server.scan_deadline,
            multi_scan_deadline: server.multi_scan_deadline,
            slots: Slots::new(server.max_in_flight),
            max_multi_get_values: server.max_multi_get_values,
        }
    }

    /// The in-flight slots the service admits calls through.
    pub fn slots(&self) -> &Slots {
        &self.slots
    }

    /// The tables served as they stand, borrowed for one call
    /// (`Opened::serving_tables`): a call holds them to its end, so a
    /// reload that drops its table meanwhile does not cut it short. With
    /// the lookup's own generation guard, a call waiting on a read holds
    /// two of its thread's fast slots.
    fn serving(&self) -> Guard<Arc<Tables>> {
        self.opened.serving_tables()
    }
}

/// The table `tables` serves under `name`; a name not served is
/// `NOT_FOUND`.
fn table<'a>(tables: &'a Tables, name: &str) -> Result<&'a Arc<Table>, Status> {
    tables
        .get(name)
        .ok_or_else(|| Status::not_found(format!("table {name} is not served")))
}

/// The table's key type, which a call reads its keys off the wire under:
/// `UNAVAILABLE`, which a caller retries, for a table that has not loaded.
/// A call reads it only once its request has passed every check that needs
/// no generation and the call holds a slot, so a request with a fault one
/// of those checks finds is refused for it, never answered `UNAVAILABLE`.
fn ready_key_type(table: &Table) -> Result<KeyType, Status> {
    table.key_type().map_err(|error| status_from(&error))
}

/// A scan's response as measured against `server.max_scan_bytes`: sent, with
/// its size recorded, or refused as `Error::ResponseTooLarge` with every key
/// read counted an error (`ScanReceipt`).
fn settle<R>(
    receipt: ScanReceipt,
    measured: Result<(R, usize), Oversize>,
) -> Result<Response<R>, Status> {
    match measured {
        Ok((response, bytes)) => {
            receipt.sent(bytes);
            Ok(Response::new(response))
        }
        Err(oversize) => Err(status_from(&receipt.refused(oversize))),
    }
}

/// The variant a wire `Key` carries (RFD 010), the closed set the `oneof`
/// declares, for the fault naming one the table's key type does not take.
/// Display gives the field's name in the contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireVariant {
    /// `Key.long`.
    Long,
    /// `Key.str`.
    Str,
    /// `Key.bin`.
    Bin,
}

impl WireVariant {
    /// The variant `key` carries.
    pub fn of(key: &v1::key::Key) -> Self {
        match key {
            v1::key::Key::Long(_) => Self::Long,
            v1::key::Key::Str(_) => Self::Str,
            v1::key::Key::Bin(_) => Self::Bin,
        }
    }
}

impl fmt::Display for WireVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Long => "long",
            Self::Str => "str",
            Self::Bin => "bin",
        };
        f.write_str(name)
    }
}

/// The variant a wire key carries: a missing key or a `Key` with no
/// variant is `WireKeyFault::Missing`, a fault of the request whatever its
/// table.
pub fn key_variant(key: Option<v1::Key>) -> Result<v1::key::Key, WireKeyFault> {
    key.and_then(|key| key.key).ok_or(WireKeyFault::Missing)
}

/// A key's variant from the wire under the table's key type (RFD 010): the
/// variant must match the type, and `bin` on a uuid table must be
/// `UUID_BYTES` long. Every other pairing is `VariantMismatch`.
pub fn key_from_wire(variant: v1::key::Key, key_type: KeyType) -> Result<Key, WireKeyFault> {
    match (variant, key_type) {
        (v1::key::Key::Long(value), KeyType::Long) => Ok(Key::Long(value)),
        (v1::key::Key::Str(text), KeyType::String) => Ok(Key::Str(text)),
        (v1::key::Key::Bin(bytes), KeyType::Binary) => Ok(Key::Bytes(bytes)),
        (v1::key::Key::Bin(bytes), KeyType::Uuid) => <[u8; UUID_BYTES]>::try_from(bytes.as_slice())
            .map(Key::Uuid)
            .map_err(|_| WireKeyFault::UuidLength { bytes: bytes.len() }),
        (variant @ v1::key::Key::Long(_), KeyType::String | KeyType::Binary | KeyType::Uuid)
        | (variant @ v1::key::Key::Str(_), KeyType::Long | KeyType::Binary | KeyType::Uuid)
        | (variant @ v1::key::Key::Bin(_), KeyType::Long | KeyType::String) => {
            Err(WireKeyFault::VariantMismatch {
                expected: key_type,
                got: WireVariant::of(&variant),
            })
        }
    }
}

/// A batch's key variants, each as `key_variant` reads one; the first
/// fault ends the batch.
fn key_variants(keys: Vec<v1::Key>) -> Result<Vec<v1::key::Key>, WireKeyFault> {
    keys.into_iter().map(|key| key_variant(Some(key))).collect()
}

/// A batch's keys from the wire, each as `key_from_wire` reads one; the
/// first fault ends the batch.
fn keys_from_wire(
    variants: Vec<v1::key::Key>,
    key_type: KeyType,
) -> Result<Vec<Key>, WireKeyFault> {
    variants
        .into_iter()
        .map(|variant| key_from_wire(variant, key_type))
        .collect()
}

/// A Scan's direction from the wire: `Direction::try_from` of the number a
/// request carries, which names `NEWEST_FIRST` and `OLDEST_FIRST`. The server
/// has no default direction, so unspecified and a number the contract does
/// not name are `WireScanFault::Direction`.
pub fn direction_from_wire(number: i32) -> Result<Direction, WireScanFault> {
    match v1::Direction::try_from(number) {
        Ok(v1::Direction::NewestFirst) => Ok(Direction::NewestFirst),
        Ok(v1::Direction::OldestFirst) => Ok(Direction::OldestFirst),
        Ok(v1::Direction::Unspecified) | Err(_) => Err(WireScanFault::Direction { number }),
    }
}

/// A Scan's bound from the wire: `None` when the request sets none, the
/// order value when it sets a variant, and `WireScanFault::EmptyBound`
/// for a bound that is present with no variant. Whether the variant is the
/// one the table's order type takes is core's check (`ScanRequestFault`).
pub fn order_value_from_wire(
    bound: Option<v1::OrderValue>,
    which: BoundSide,
) -> Result<Option<OrderValue>, WireScanFault> {
    let Some(bound) = bound else {
        return Ok(None);
    };
    order_of_wire(bound.value)
        .map(Some)
        .ok_or(WireScanFault::EmptyBound { bound: which })
}

/// The order value a wire `OrderValue`'s variant carries, `None` for one
/// that sets no variant.
fn order_of_wire(value: Option<order_value::Value>) -> Option<OrderValue> {
    match value? {
        order_value::Value::Long(value) => Some(OrderValue::Long(value)),
        order_value::Value::Str(text) => Some(OrderValue::Str(text)),
        order_value::Value::Bin(bytes) => Some(OrderValue::Bytes(bytes)),
    }
}

/// What a request's direction, limit, and bounds ask of each key, read from
/// the wire.
fn scan_bounds_from_wire(
    direction: i32,
    limit: u32,
    start: Option<v1::OrderValue>,
    before: Option<v1::OrderValue>,
) -> Result<ScanBounds, WireScanFault> {
    Ok(ScanBounds {
        direction: direction_from_wire(direction)?,
        limit,
        start: order_value_from_wire(start, BoundSide::Start)?,
        before: order_value_from_wire(before, BoundSide::Before)?,
    })
}

/// The field number of `ScanResponse.rows` and of `MultiScanResponse.results`,
/// the repeated field each response is mostly made of.
const REPEATED_FIELD_TAG: u32 = 1;

/// The field number of `ScanResponse.next`.
const NEXT_FIELD_TAG: u32 = 2;

/// A Scan response read in `direction` over `bound` bytes, its refusal, or
/// the response and its size. The response is measured as the message that
/// would be sent; past the bound, `Oversize` says how many rows would fit:
/// the longest prefix whose response, as a Scan at that limit answers it
/// with its `next`, is within the bound.
fn scan_within_bound(
    lookup: ScanLookup,
    direction: Direction,
    bound: usize,
) -> Result<(ScanResponse, usize), Oversize> {
    let v1::ScanResult { rows, next } = scanned_wire(lookup.scanned);
    let response = ScanResponse {
        rows,
        next,
        snapshot_id: lookup.snapshot_id.0,
    };
    let bytes = response.encoded_len();
    if bytes <= bound {
        return Ok((response, bytes));
    }
    let fixed = ScanResponse {
        rows: Vec::new(),
        next: None,
        snapshot_id: response.snapshot_id,
    };
    // A Scan at a limit of `taken` rows, fewer than the response's,
    // answers them with the `next` that follows the last of them
    // (`next_bound`), since a row lies past it. At all of the response's
    // rows it answers the response's own. A prefix ending on a row past
    // which no bound lies has no `next` to give, and never counts as
    // fitting; core refuses a response holding such a row with one after
    // it (`Error::NoNextBound`), so none reaches here.
    let rows = &response.rows;
    let next_of = |taken: usize| {
        if taken == rows.len() {
            return Some(
                response
                    .next
                    .as_ref()
                    .map_or(0, |next| message::encoded_len(NEXT_FIELD_TAG, next)),
            );
        }
        let Some(last) = taken
            .checked_sub(1)
            .and_then(|last| rows[last].order.as_ref())
            .and_then(|order| order_of_wire(order.value.clone()))
        else {
            return Some(0);
        };
        let next = order_value_wire(next_bound(&last, direction)?);
        Some(message::encoded_len(NEXT_FIELD_TAG, &next))
    };
    Err(Oversize {
        bytes,
        bound,
        fits: Fits::Rows(fitting_prefix(rows, fixed.encoded_len(), bound, next_of)),
    })
}

/// A `MultiScan` response as `scan_within_bound` has a Scan's, `Oversize`
/// counting the leading keys' results that would fit.
fn multi_scan_within_bound(
    lookup: MultiScanLookup,
    bound: usize,
) -> Result<(MultiScanResponse, usize), Oversize> {
    let results = lookup.results.into_iter().map(scanned_wire).collect();
    let response = MultiScanResponse {
        results,
        snapshot_id: lookup.snapshot_id.0,
    };
    let bytes = response.encoded_len();
    if bytes <= bound {
        return Ok((response, bytes));
    }
    let fixed = MultiScanResponse {
        results: Vec::new(),
        snapshot_id: response.snapshot_id,
    };
    Err(Oversize {
        bytes,
        bound,
        fits: Fits::Keys(fitting_prefix(
            &response.results,
            fixed.encoded_len(),
            bound,
            |_| Some(0),
        )),
    })
}

/// The longest prefix of `items`, the repeated field of a response whose
/// other fields encode to `fixed` bytes, that fits in `bound` bytes, a
/// response of the first `n` of them carrying `tail(n)` bytes more; a
/// prefix whose tail is `None` does not fit. A shorter prefix need not fit
/// where a longer one does, since a tail can shrink as `n` grows.
fn fitting_prefix<M: Message>(
    items: &[M],
    fixed: usize,
    bound: usize,
    tail: impl Fn(usize) -> Option<usize>,
) -> usize {
    let mut size = fixed;
    let mut fits = 0;
    for (at, item) in items.iter().enumerate() {
        size += message::encoded_len(REPEATED_FIELD_TAG, item);
        if size > bound {
            break;
        }
        if tail(at + 1).is_some_and(|tail| size + tail <= bound) {
            fits = at + 1;
        }
    }
    fits
}

/// A wire scan fault as a status: every fault is the caller's, so
/// `INVALID_ARGUMENT`, with the fault's `Display` as the message.
impl From<WireScanFault> for Status {
    fn from(fault: WireScanFault) -> Self {
        Self::invalid_argument(fault.to_string())
    }
}

/// A wire key's fault as a status: every fault is the caller's, so
/// `INVALID_ARGUMENT`, with the fault's `Display` as the message.
impl From<WireKeyFault> for Status {
    fn from(fault: WireKeyFault) -> Self {
        Self::invalid_argument(fault.to_string())
    }
}

/// A call's own bound as a status: each fault's code, with the fault's
/// `Display` as the message.
impl From<CallFault> for Status {
    fn from(fault: CallFault) -> Self {
        let message = fault.to_string();
        match fault {
            CallFault::DeadlineExceeded { .. } => Self::deadline_exceeded(message),
            CallFault::Overloaded { .. } => Self::resource_exhausted(message),
            CallFault::TooManyValues { .. } => Self::invalid_argument(message),
        }
    }
}

/// The values a `MultiGet` of `keys` keys naming `columns` columns asks
/// for, its keys times its columns, a presence-only call counting one per
/// key (RFD 022), held to `limit`: `CallFault::TooManyValues` over it,
/// before the call takes a slot.
fn check_values(keys: usize, columns: usize, limit: usize) -> Result<(), CallFault> {
    if keys.saturating_mul(columns.max(1)) > limit {
        return Err(CallFault::TooManyValues {
            keys,
            columns,
            limit,
        });
    }
    Ok(())
}

/// Runs `work` under the server's deadline `after`; past it, `work` is
/// dropped and the call is `DEADLINE_EXCEEDED`. A client's `grpc-timeout`
/// is not read here: tonic's server layer drops the call at it, and the
/// client's library reports that expiry as its own.
async fn within<T>(
    after: Duration,
    work: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::time::timeout(after, work)
        .await
        .map_err(|_elapsed| CallFault::DeadlineExceeded { after })?
}

/// Core's error as a status (RFD 010): a key of the wrong type, a column
/// the table does not serve, named twice (RFD 022), or read with a field
/// list its format does not take (RFD 024), a malformed Scan or `MultiScan`
/// request, and a call of the wrong kind for the table (RFD 025), is
/// `INVALID_ARGUMENT`; a value a field list trims that is not the protobuf
/// its column declares is `FAILED_PRECONDITION`, which a retry meets again
/// (RFD 024), as is a Scan or `MultiScan` response above
/// `server.max_scan_bytes` (RFD 025); a page fetch that ran past
/// `server.get_deadline`, or a Scan's span GET past `server.scan_deadline`,
/// is `DEADLINE_EXCEEDED`, the code the call's own deadline answers: a
/// call's deadline and its fetch's pass moments apart, and the call answers
/// one code whichever passes first; a read the caller can retry (no
/// generation yet, an object-store, cache, or local-directory failure, a
/// catalog call that failed, a catalog or load that did not answer in time,
/// a load's key verification or a task of a `MultiGet` or `MultiScan`
/// canceled as the runtime shut down) is `UNAVAILABLE`; every other
/// failure, faulty table data among them, is `INTERNAL`. The message is
/// the error's `Display`.
pub fn status_from(error: &Error) -> Status {
    let message = error.to_string();
    match error {
        Error::KeyType { .. }
        | Error::ColumnRequest { .. }
        | Error::ScanRequest { .. }
        | Error::WrongTableKind { .. } => Status::invalid_argument(message),
        Error::MalformedValue { .. } | Error::ResponseTooLarge { .. } => {
            Status::failed_precondition(message)
        }
        Error::Cache(CacheError::FetchDeadline { .. }) => Status::deadline_exceeded(message),
        Error::NotReady { .. }
        | Error::ObjectStore(_)
        | Error::Cache(
            CacheError::Fetch(_) | CacheError::Backend(_) | CacheError::DescriptorLimit { .. },
        )
        | Error::Local { .. }
        | Error::CatalogTimeout { .. }
        | Error::Catalog { .. }
        | Error::LoadDeadline { .. }
        | Error::RefreshDeadline { .. }
        | Error::VerifyCanceled { .. }
        | Error::LookupCanceled { .. } => Status::unavailable(message),
        Error::Iceberg(_)
        | Error::Parquet(_)
        | Error::ObjectStorePath(_)
        | Error::Json(_)
        | Error::Config(_)
        | Error::Location { .. }
        | Error::Refused { .. }
        | Error::NoNextBound { .. }
        | Error::Table { .. }
        | Error::File { .. } => Status::internal(message),
    }
}

/// A lookup's outcome on the wire: the presence and, for a present key, one
/// value per column named, in order, each its bytes or `null`; none for an
/// absent key. A present value's bytes are the lookup's own, moved into the
/// message, not copied.
pub fn row_wire(row: Row) -> (v1::Presence, Vec<ColumnValue>) {
    match row {
        Row::Present(values) => (
            v1::Presence::Present,
            values.into_iter().map(column_value_wire).collect(),
        ),
        Row::Absent => (v1::Presence::Absent, Vec::new()),
    }
}

/// One column's value on the wire: its bytes, or `null` for a null, which
/// is not a value of zero length.
fn column_value_wire(value: Option<Bytes>) -> ColumnValue {
    let kind = match value {
        Some(bytes) => column_value::Kind::Value(bytes),
        None => column_value::Kind::NullValue(NullValue {}),
    };
    ColumnValue { kind: Some(kind) }
}

/// The table's value columns on the wire, in the layout's order, which is
/// by name, each with the format `entry`, the table's entry as it stands,
/// declares it in.
fn value_columns_wire(layout: &TableLayout, entry: &TableConfig) -> Vec<v1::ValueColumn> {
    let formats = entry.formats();
    layout
        .value_columns
        .iter()
        .map(|column| {
            let value_type = match column.value_type {
                ValueType::Binary => v1::ValueType::Binary,
                ValueType::Fixed { .. } => v1::ValueType::Fixed,
                ValueType::String => v1::ValueType::String,
            };
            let width = column.value_type.width().map_or(0, |width| {
                u32::try_from(width).expect(
                    "a layout admits a fixed column only as wide as a Parquet type length, an i32",
                )
            });
            let format = match formats(&column.name) {
                ValueFormat::Opaque => v1::ValueFormat::Opaque,
                ValueFormat::Protobuf => v1::ValueFormat::Protobuf,
            };
            v1::ValueColumn {
                name: column.name.clone(),
                value_type: i32::from(value_type),
                width,
                format: i32::from(format),
            }
        })
        .collect()
}

/// The column reads a request carries, as the core takes them.
fn reads(columns: &[v1::ColumnRead]) -> Vec<ColumnRead<'_>> {
    columns
        .iter()
        .map(|read| ColumnRead {
            name: &read.name,
            fields: &read.fields,
        })
        .collect()
}

/// An order value on the wire, in the variant the table's order type takes.
fn order_value_wire(value: OrderValue) -> v1::OrderValue {
    let value = match value {
        OrderValue::Long(value) => order_value::Value::Long(value),
        OrderValue::Str(text) => order_value::Value::Str(text),
        OrderValue::Bytes(bytes) => order_value::Value::Bin(bytes),
    };
    v1::OrderValue { value: Some(value) }
}

/// One Scan's answer on the wire: each row's order value and one value per
/// column, in the order the rows were read, and the next page's bound. The
/// values' bytes are the Scan's own, moved into the message, not copied.
fn scanned_wire(scanned: Scanned) -> v1::ScanResult {
    let rows = scanned
        .rows
        .into_iter()
        .map(|ScanRow { order, values }| v1::ScanRow {
            order: Some(order_value_wire(order)),
            values: values.into_iter().map(column_value_wire).collect(),
        })
        .collect();
    v1::ScanResult {
        rows,
        next: scanned.next.map(order_value_wire),
    }
}

/// The order column's type on the wire, and the width of a `fixed` one, 0
/// for every other type.
fn order_type_wire(order_type: OrderType) -> (v1::OrderType, u32) {
    match order_type {
        OrderType::Int => (v1::OrderType::Int, 0),
        OrderType::Long => (v1::OrderType::Long, 0),
        OrderType::Date => (v1::OrderType::Date, 0),
        OrderType::Timestamp => (v1::OrderType::Timestamp, 0),
        OrderType::TimestampTz => (v1::OrderType::Timestamptz, 0),
        OrderType::TimestampNs => (v1::OrderType::TimestampNs, 0),
        OrderType::TimestampTzNs => (v1::OrderType::TimestamptzNs, 0),
        OrderType::String => (v1::OrderType::String, 0),
        OrderType::Binary => (v1::OrderType::Binary, 0),
        OrderType::Fixed(width) => (v1::OrderType::Fixed, width),
    }
}

/// The table's key type on the wire.
fn key_type_wire(key_type: KeyType) -> v1::KeyType {
    match key_type {
        KeyType::Long => v1::KeyType::Long,
        KeyType::String => v1::KeyType::String,
        KeyType::Binary => v1::KeyType::Binary,
        KeyType::Uuid => v1::KeyType::Uuid,
    }
}

/// `Get` of `key` reading `columns` from `table`, whose request has been
/// checked.
async fn lookup(
    table: &Table,
    key: &Key,
    columns: &[ColumnRead<'_>],
) -> Result<Response<GetResponse>, Status> {
    let lookup = table
        .get(key, columns)
        .await
        .map_err(|error| status_from(&error))?;
    let (presence, values) = row_wire(lookup.row);
    Ok(Response::new(GetResponse {
        presence: i32::from(presence),
        values,
        snapshot_id: lookup.snapshot_id.0,
    }))
}

/// `MultiGet` of `keys` reading `columns` from `table`, whose request has
/// been checked.
async fn lookup_batch(
    table: &Table,
    keys: &[Key],
    columns: &[ColumnRead<'_>],
) -> Result<Response<MultiGetResponse>, Status> {
    let lookup = table
        .multi_get(keys, columns)
        .await
        .map_err(|error| status_from(&error))?;
    let results = lookup
        .rows
        .into_iter()
        .map(|row| {
            let (presence, values) = row_wire(row);
            Hit {
                presence: i32::from(presence),
                values,
            }
        })
        .collect();
    Ok(Response::new(MultiGetResponse {
        results,
        snapshot_id: lookup.snapshot_id.0,
    }))
}

/// `Scan` of `key` reading `columns` from `table` under `bounds`, whose
/// request has been checked.
async fn scan_rows(
    table: &Table,
    key: &Key,
    columns: &[ColumnRead<'_>],
    bounds: &ScanBounds,
) -> Result<Response<ScanResponse>, Status> {
    let (lookup, receipt) = table
        .scan_receipt(key, columns, bounds)
        .await
        .map_err(|error| status_from(&error))?;
    settle(
        receipt,
        scan_within_bound(lookup, bounds.direction, table.max_scan_bytes()),
    )
}

/// `MultiScan` of `keys` reading `columns` from `table` under `bounds`,
/// whose request has been checked.
async fn scan_batch(
    table: &Table,
    keys: &[Key],
    columns: &[ColumnRead<'_>],
    bounds: &ScanBounds,
) -> Result<Response<MultiScanResponse>, Status> {
    let (lookup, receipt) = table
        .multi_scan_receipt(keys, columns, bounds)
        .await
        .map_err(|error| status_from(&error))?;
    settle(
        receipt,
        multi_scan_within_bound(lookup, table.max_scan_bytes()),
    )
}

impl Service {
    /// `Get`, checked in the module's order, the checks against the
    /// generation in `Table::get`, inside its deadline.
    async fn answer_get(&self, request: GetRequest) -> Result<Response<GetResponse>, Status> {
        let GetRequest {
            table: name,
            key,
            columns,
        } = request;
        let key = key_variant(key)?;
        let columns = reads(&columns);
        check_column_reads(&name, &columns).map_err(|error| status_from(&error))?;
        let tables = self.serving();
        let table = table(&tables, &name)?;
        table
            .check_kind(TableKind::KeyValue)
            .map_err(|error| status_from(&error))?;
        let _slot = self.slots.admit()?;
        let key = key_from_wire(key, ready_key_type(table)?)?;
        within(self.get_deadline, lookup(table, &key, &columns)).await
    }

    /// `MultiGet`, checked as `Get` is, every key as `Get`'s, and held to
    /// `max_multi_get_values` (`check_values`) after its table's kind.
    async fn answer_multi_get(
        &self,
        request: MultiGetRequest,
    ) -> Result<Response<MultiGetResponse>, Status> {
        let MultiGetRequest {
            table: name,
            keys,
            columns,
        } = request;
        let keys = key_variants(keys)?;
        let columns = reads(&columns);
        check_column_reads(&name, &columns).map_err(|error| status_from(&error))?;
        let tables = self.serving();
        let table = table(&tables, &name)?;
        table
            .check_kind(TableKind::KeyValue)
            .map_err(|error| status_from(&error))?;
        check_values(keys.len(), columns.len(), self.max_multi_get_values)?;
        let _slot = self.slots.admit()?;
        let keys = keys_from_wire(keys, ready_key_type(table)?)?;
        within(
            self.multi_get_deadline,
            lookup_batch(table, &keys, &columns),
        )
        .await
    }

    /// `Scan`, checked in the module's order, its direction and bounds off
    /// the wire and `check_scan_request` before its table, `Table::check_scan`
    /// before its slot, and the checks against the generation in
    /// `Table::scan_receipt`, inside its deadline.
    async fn answer_scan(&self, request: ScanRequest) -> Result<Response<ScanResponse>, Status> {
        let ScanRequest {
            table: name,
            key,
            columns,
            direction,
            limit,
            start,
            before,
        } = request;
        let key = key_variant(key)?;
        let bounds = scan_bounds_from_wire(direction, limit, start, before)?;
        let columns = reads(&columns);
        check_scan_request(&name, &columns, &bounds).map_err(|error| status_from(&error))?;
        let tables = self.serving();
        let table = table(&tables, &name)?;
        table
            .check_scan(&bounds)
            .map_err(|error| status_from(&error))?;
        let _slot = self.slots.admit()?;
        let key = key_from_wire(key, ready_key_type(table)?)?;
        within(
            self.scan_deadline,
            scan_rows(table, &key, &columns, &bounds),
        )
        .await
    }

    /// `MultiScan`, checked as `Scan` is, every key as `Scan`'s, with
    /// `Table::check_multi_scan`, which holds its keys times its limit times
    /// its columns plus one to `max_multi_scan_values`, in place of
    /// `Table::check_scan`.
    async fn answer_multi_scan(
        &self,
        request: MultiScanRequest,
    ) -> Result<Response<MultiScanResponse>, Status> {
        let MultiScanRequest {
            table: name,
            keys,
            columns,
            direction,
            limit,
            start,
            before,
        } = request;
        let keys = key_variants(keys)?;
        let bounds = scan_bounds_from_wire(direction, limit, start, before)?;
        let columns = reads(&columns);
        check_scan_request(&name, &columns, &bounds).map_err(|error| status_from(&error))?;
        let tables = self.serving();
        let table = table(&tables, &name)?;
        table
            .check_multi_scan(keys.len(), columns.len(), &bounds)
            .map_err(|error| status_from(&error))?;
        let _slot = self.slots.admit()?;
        let keys = keys_from_wire(keys, ready_key_type(table)?)?;
        within(
            self.multi_scan_deadline,
            scan_batch(table, &keys, &columns, &bounds),
        )
        .await
    }

    /// `TableInfo`: the serving generation's description.
    fn answer_table_info(
        &self,
        request: TableInfoRequest,
    ) -> Result<Response<TableInfoResponse>, Status> {
        let tables = self.serving();
        let table = table(&tables, &request.table)?;
        let generation = table.serving().map_err(|error| status_from(&error))?;
        let entry = table.config();
        let limits = entry.scan_limits(table.max_scan_bytes(), table.max_multi_scan_values());
        let order = generation.layout.order.as_ref();
        let (order_type, order_width) = order.map_or((v1::OrderType::Unspecified, 0), |order| {
            order_type_wire(order.order_type)
        });
        Ok(Response::new(TableInfoResponse {
            table: request.table,
            snapshot_id: generation.snapshot_id.0,
            buckets: generation
                .layout
                .bucketing
                .map_or(0, |bucketing| bucketing.count),
            data_files: approx::count(generation.files.len()),
            records: generation.records,
            key_type: i32::from(key_type_wire(generation.layout.key_type)),
            index_bytes: approx::count(generation.index_bytes),
            value_columns: value_columns_wire(&generation.layout, &entry),
            order_column: order.map_or_else(String::new, |order| order.name.clone()),
            order_type: i32::from(order_type),
            order_width,
            max_scan_rows: limits.map_or(0, |limits| limits.max_scan_rows),
            max_multi_scan_values: limits
                .map_or(0, |limits| approx::count(limits.max_multi_scan_values)),
            max_scan_bytes: limits.map_or(0, |limits| approx::count(limits.max_scan_bytes)),
        }))
    }
}

#[tonic::async_trait]
impl Icetray for Service {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.answer_get(request.into_inner()).await
    }

    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        self.answer_multi_get(request.into_inner()).await
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        self.answer_scan(request.into_inner()).await
    }

    async fn multi_scan(
        &self,
        request: Request<MultiScanRequest>,
    ) -> Result<Response<MultiScanResponse>, Status> {
        self.answer_multi_scan(request.into_inner()).await
    }

    async fn table_info(
        &self,
        request: Request<TableInfoRequest>,
    ) -> Result<Response<TableInfoResponse>, Status> {
        self.answer_table_info(request.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use bytes::Bytes;
    use icetray_core::config::{ConfigError, ConfigViolation, StorageConfig};
    use icetray_core::{
        BoundSide, ColumnRequestFault, FileFault, LocalFault, Malformed, OrderVariant,
        RefusalReason, ScanRequestFault, SourceConfig, Storage, TableFault, TableKind,
        ValueColumnConfig,
    };
    use tonic::Code;

    use super::*;

    /// The table the sample errors name.
    const TABLE: &str = "kv";

    /// A `bin` key one byte short of a uuid.
    const SHORT_BIN_LEN: usize = UUID_BYTES - 1;

    /// The value of the sample long key.
    const LONG_VALUE: i64 = 42;

    /// The value of the sample string key.
    const STR_VALUE: &str = "user:42";

    fn wire(variant: v1::key::Key) -> v1::Key {
        v1::Key { key: Some(variant) }
    }

    #[test]
    fn test_key_variant_missing_key_or_variant_is_missing() {
        assert_eq!(key_variant(None), Err(WireKeyFault::Missing));
        assert_eq!(
            key_variant(Some(v1::Key { key: None })),
            Err(WireKeyFault::Missing)
        );
        let variant = v1::key::Key::Long(LONG_VALUE);
        assert_eq!(key_variant(Some(wire(variant.clone()))), Ok(variant));
    }

    #[test]
    fn test_key_variants_is_missing_for_any_key_without_a_variant() {
        let long = v1::key::Key::Long(LONG_VALUE);
        assert_eq!(
            key_variants(vec![wire(long.clone()), v1::Key { key: None }]),
            Err(WireKeyFault::Missing)
        );
        assert_eq!(
            key_variants(vec![wire(long.clone()), wire(long.clone())]),
            Ok(vec![long.clone(), long])
        );
    }

    #[test]
    fn test_key_from_wire_matching_variant_per_type() {
        assert_eq!(
            key_from_wire(v1::key::Key::Long(LONG_VALUE), KeyType::Long).unwrap(),
            Key::Long(LONG_VALUE)
        );
        assert_eq!(
            key_from_wire(v1::key::Key::Str(STR_VALUE.to_string()), KeyType::String).unwrap(),
            Key::Str(STR_VALUE.to_string())
        );
        let bytes = vec![0u8; SHORT_BIN_LEN];
        assert_eq!(
            key_from_wire(v1::key::Key::Bin(bytes.clone()), KeyType::Binary).unwrap(),
            Key::Bytes(bytes)
        );
        let uuid = [7u8; UUID_BYTES];
        assert_eq!(
            key_from_wire(v1::key::Key::Bin(uuid.to_vec()), KeyType::Uuid).unwrap(),
            Key::Uuid(uuid)
        );
    }

    #[test]
    fn test_key_from_wire_mismatched_variant_names_both_sides() {
        let variants = [
            v1::key::Key::Long(LONG_VALUE),
            v1::key::Key::Str(STR_VALUE.to_string()),
            v1::key::Key::Bin(vec![0u8; UUID_BYTES]),
        ];
        for key_type in KeyType::ALL {
            for variant in &variants {
                let matches = matches!(
                    (variant, key_type),
                    (v1::key::Key::Long(_), KeyType::Long)
                        | (v1::key::Key::Str(_), KeyType::String)
                        | (v1::key::Key::Bin(_), KeyType::Binary | KeyType::Uuid)
                );
                let result = key_from_wire(variant.clone(), key_type);
                if matches {
                    assert!(result.is_ok(), "{variant:?} on {key_type}");
                } else {
                    assert_eq!(
                        result,
                        Err(WireKeyFault::VariantMismatch {
                            expected: key_type,
                            got: WireVariant::of(variant),
                        }),
                        "{variant:?} on {key_type}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_key_from_wire_uuid_needs_exactly_sixteen_bytes() {
        for len in [0, SHORT_BIN_LEN, UUID_BYTES + 1] {
            assert_eq!(
                key_from_wire(v1::key::Key::Bin(vec![0u8; len]), KeyType::Uuid),
                Err(WireKeyFault::UuidLength { bytes: len }),
                "{len} bytes"
            );
        }
    }

    #[test]
    fn test_from_wire_key_fault_is_invalid_argument_per_variant() {
        let faults = [
            WireKeyFault::Missing,
            WireKeyFault::VariantMismatch {
                expected: KeyType::Long,
                got: WireVariant::Str,
            },
            WireKeyFault::UuidLength {
                bytes: SHORT_BIN_LEN,
            },
        ];
        for fault in faults {
            assert_eq!(Status::from(fault).code(), Code::InvalidArgument);
        }
    }

    /// A direction number the contract does not name.
    const UNKNOWN_DIRECTION: i32 = 99;

    /// A deadline for the sample fault.
    const SAMPLE_DEADLINE: Duration = Duration::from_millis(250);

    /// A limit for the sample faults.
    const SAMPLE_LIMIT: usize = 8;

    /// A row limit for the sample Scan faults.
    const SAMPLE_LIMIT_U32: u32 = 8;

    #[test]
    fn test_from_call_fault_code_per_variant() {
        let cases = [
            (
                CallFault::DeadlineExceeded {
                    after: SAMPLE_DEADLINE,
                },
                Code::DeadlineExceeded,
            ),
            (
                CallFault::Overloaded {
                    limit: SAMPLE_LIMIT,
                },
                Code::ResourceExhausted,
            ),
            (
                CallFault::TooManyValues {
                    keys: SAMPLE_LIMIT + 1,
                    columns: 1,
                    limit: SAMPLE_LIMIT,
                },
                Code::InvalidArgument,
            ),
        ];
        for (fault, code) in cases {
            assert_eq!(Status::from(fault).code(), code);
        }
    }

    /// A slot is held from `admit` to the drop of what it returns, and
    /// with every slot held the next call is refused.
    #[test]
    fn test_admit_holds_a_slot_until_dropped_and_refuses_past_the_limit() {
        let slots = Slots::new(SAMPLE_LIMIT);
        assert_eq!((slots.limit(), slots.admitted()), (SAMPLE_LIMIT, 0));
        let held: Vec<_> = (0..SAMPLE_LIMIT).map(|_| slots.admit().unwrap()).collect();
        assert_eq!(slots.admitted(), SAMPLE_LIMIT);
        assert!(matches!(
            slots.admit(),
            Err(CallFault::Overloaded {
                limit: SAMPLE_LIMIT
            })
        ));
        assert_eq!(slots.admitted(), SAMPLE_LIMIT, "a refusal holds no slot");
        drop(held);
        assert_eq!(slots.admitted(), 0);
    }

    #[test]
    fn test_new_holds_max_in_flight_to_the_semaphore_s_bound() {
        assert_eq!(Slots::new(usize::MAX).limit(), Semaphore::MAX_PERMITS);
    }

    #[test]
    fn test_of_names_every_wire_variant() {
        let cases = [
            (v1::key::Key::Long(LONG_VALUE), WireVariant::Long),
            (v1::key::Key::Str(STR_VALUE.to_string()), WireVariant::Str),
            (v1::key::Key::Bin(Vec::new()), WireVariant::Bin),
        ];
        for (key, variant) in cases {
            assert_eq!(WireVariant::of(&key), variant);
        }
    }

    /// One error of every variant the binary can build without the Iceberg,
    /// Parquet, or object-store crates, beside its code.
    fn status_cases() -> Vec<(Error, Code)> {
        vec![
            (
                Error::KeyType {
                    table: TABLE.to_string(),
                    expected: KeyType::Long,
                    got: KeyType::String,
                },
                Code::InvalidArgument,
            ),
            (
                Error::NotReady {
                    table: TABLE.to_string(),
                },
                Code::Unavailable,
            ),
            (
                Error::Cache(CacheError::Fetch(Arc::new(Error::NotReady {
                    table: TABLE.to_string(),
                }))),
                Code::Unavailable,
            ),
            (
                Error::Cache(CacheError::FetchDeadline {
                    path: "mem://f0.parquet".to_string(),
                    offset: 0,
                    after: SAMPLE_DEADLINE,
                }),
                Code::DeadlineExceeded,
            ),
            (
                Error::Local {
                    path: PathBuf::from("/var/lib/icetray"),
                    fault: LocalFault::Io(std::io::Error::other("sample")),
                },
                Code::Unavailable,
            ),
            (
                Error::Json(serde_json::from_str::<u8>("sample").unwrap_err()),
                Code::Internal,
            ),
            (
                Error::Config(ConfigError::Invalid(ConfigViolation::EmptyTableName {
                    index: 0,
                })),
                Code::Internal,
            ),
            (
                Error::Refused {
                    table: TABLE.to_string(),
                    reason: RefusalReason::KeyTypeUnsupported,
                },
                Code::Internal,
            ),
            (
                Error::Table {
                    table: TABLE.to_string(),
                    fault: TableFault::MetadataLocationUnknown,
                },
                Code::Internal,
            ),
            (
                Error::File {
                    path: "mem://f0.parquet".to_string(),
                    fault: FileFault::NoRows,
                },
                Code::Internal,
            ),
            (
                Error::CatalogTimeout {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                Code::Unavailable,
            ),
            (
                Error::LoadDeadline {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                Code::Unavailable,
            ),
            (
                Error::RefreshDeadline {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                Code::Unavailable,
            ),
        ]
        .into_iter()
        .chain(column_cases())
        .chain(scan_cases())
        .chain(canceled_cases())
        .collect()
    }

    /// Every fault of a Scan request core finds, and a call of the wrong
    /// kind, beside its code.
    fn scan_cases() -> Vec<(Error, Code)> {
        let (limit, max) = (SAMPLE_LIMIT_U32 + 1, SAMPLE_LIMIT_U32);
        [
            ScanRequestFault::LimitZero,
            ScanRequestFault::LimitAboveMax { limit, max },
            ScanRequestFault::BoundVariant {
                bound: BoundSide::Start,
                expected: OrderVariant::Long,
                got: OrderVariant::Str,
            },
            ScanRequestFault::EmptyWindow,
            ScanRequestFault::TooManyValues {
                keys: SAMPLE_LIMIT,
                limit,
                columns: 1,
                values: SAMPLE_LIMIT * 2,
                max: SAMPLE_LIMIT,
            },
        ]
        .into_iter()
        .map(|fault| {
            (
                Error::ScanRequest {
                    table: TABLE.to_string(),
                    fault,
                },
                Code::InvalidArgument,
            )
        })
        .chain([TableKind::KeyValue, TableKind::Ordered].map(|kind| {
            (
                Error::WrongTableKind {
                    table: TABLE.to_string(),
                    kind,
                },
                Code::InvalidArgument,
            )
        }))
        .chain(
            [Fits::Rows(SAMPLE_LIMIT), Fits::Keys(SAMPLE_LIMIT)].map(|fits| {
                (
                    Error::ResponseTooLarge {
                        table: TABLE.to_string(),
                        oversize: Oversize {
                            bytes: SAMPLE_LIMIT + 1,
                            bound: SAMPLE_LIMIT,
                            fits,
                        },
                    },
                    Code::FailedPrecondition,
                )
            }),
        )
        .chain([(
            Error::NoNextBound {
                table: TABLE.to_string(),
                key: Box::new(Key::Long(LONG_VALUE)),
                order: Box::new(OrderValue::Long(i64::MAX)),
            },
            Code::Internal,
        )])
        .collect()
    }

    /// Every fault of a column a lookup names, beside its code, and a
    /// malformed value a projection met.
    fn column_cases() -> Vec<(Error, Code)> {
        let field = 7;
        [
            ColumnRequestFault::NotServed,
            ColumnRequestFault::Repeated,
            ColumnRequestFault::NotProtobuf,
            ColumnRequestFault::FieldOutOfRange { field: 0 },
            ColumnRequestFault::RepeatedField { field },
        ]
        .into_iter()
        .map(|fault| {
            (
                Error::ColumnRequest {
                    table: TABLE.to_string(),
                    column: "value".to_string(),
                    fault,
                },
                Code::InvalidArgument,
            )
        })
        .chain([(
            Error::MalformedValue {
                table: TABLE.to_string(),
                column: "value".to_string(),
                key: Box::new(Key::Long(LONG_VALUE)),
                order: None,
                fault: Malformed::WireType {
                    at: 0,
                    wire_type: 7,
                },
            },
            Code::FailedPrecondition,
        )])
        .collect()
    }

    /// The variants a canceled task ends in, beside their code.
    fn canceled_cases() -> [(Error, Code); 2] {
        [
            (
                Error::VerifyCanceled {
                    path: "mem://f0.parquet".to_string(),
                    source: canceled_task(),
                },
                Code::Unavailable,
            ),
            (
                Error::LookupCanceled {
                    table: TABLE.to_string(),
                    source: canceled_task(),
                },
                Code::Unavailable,
            ),
        ]
    }

    /// The outcome of a task aborted before it ran: the `JoinError` a
    /// canceled task leaves.
    fn canceled_task() -> tokio::task::JoinError {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let task = tokio::spawn(std::future::pending::<()>());
                task.abort();
                task.await.unwrap_err()
            })
    }

    #[test]
    fn test_status_from_code_per_variant() {
        for (error, code) in status_cases() {
            assert_eq!(status_from(&error).code(), code, "{error}");
        }
    }

    #[tokio::test]
    async fn test_status_from_object_store_failure_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let missing = format!("file://{}/missing", dir.path().display());
        let error = Storage::new(StorageConfig::default())
            .read_all(&missing)
            .await
            .expect_err("a missing file");
        assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
        assert_eq!(status_from(&error).code(), Code::Unavailable);
    }

    #[test]
    fn test_row_wire_carries_each_value_in_order_null_apart_from_zero_length() {
        let bytes = Bytes::from_static(STR_VALUE.as_bytes());
        let (present, wire) = row_wire(Row::Present(vec![
            None,
            Some(bytes.clone()),
            Some(Bytes::new()),
        ]));
        assert_eq!(present, v1::Presence::Present);
        assert_eq!(
            wire,
            [
                ColumnValue {
                    kind: Some(column_value::Kind::NullValue(NullValue {})),
                },
                ColumnValue {
                    kind: Some(column_value::Kind::Value(bytes.clone())),
                },
                ColumnValue {
                    kind: Some(column_value::Kind::Value(Bytes::new())),
                },
            ]
        );
        let Some(column_value::Kind::Value(carried)) = &wire[1].kind else {
            panic!("the second value is bytes");
        };
        assert_eq!(
            carried.as_ptr(),
            bytes.as_ptr(),
            "the message carries the lookup's bytes, not a copy"
        );
        assert_eq!(
            row_wire(Row::Present(Vec::new())),
            (v1::Presence::Present, Vec::new()),
            "a presence-only answer carries no value"
        );
        assert_eq!(row_wire(Row::Absent), (v1::Presence::Absent, Vec::new()));
    }

    #[test]
    fn test_check_values_counts_keys_times_columns_and_a_presence_only_key_as_one() {
        let columns_named = 2;
        let at_limit = SAMPLE_LIMIT / columns_named;
        assert!(check_values(at_limit, columns_named, SAMPLE_LIMIT).is_ok());
        assert!(matches!(
            check_values(at_limit + 1, columns_named, SAMPLE_LIMIT),
            Err(CallFault::TooManyValues {
                keys,
                columns: 2,
                limit: SAMPLE_LIMIT,
            }) if keys == at_limit + 1
        ));
        assert!(check_values(SAMPLE_LIMIT, 0, SAMPLE_LIMIT).is_ok());
        assert!(matches!(
            check_values(SAMPLE_LIMIT + 1, 0, SAMPLE_LIMIT),
            Err(CallFault::TooManyValues { columns: 0, .. })
        ));
    }

    #[test]
    fn test_value_columns_wire_names_each_type_a_fixed_width_and_each_format() {
        let fixed_width = 16;
        let column = |name: &str, value_type| icetray_core::ValueColumnLayout {
            name: name.to_string(),
            field_id: 0,
            value_type,
            default: None,
        };
        let layout = TableLayout {
            bucketing: None,
            key_column: "key".to_string(),
            key_field_id: 0,
            key_type: KeyType::Long,
            key_width: None,
            value_columns: vec![
                column("a", ValueType::Binary),
                column("b", ValueType::Fixed { width: fixed_width }),
                column("c", ValueType::String),
            ],
            order: None,
        };
        let entry = TableConfig {
            name: TABLE.to_string(),
            source: SourceConfig::Static {
                location: "mem://w/db/kv".to_string(),
            },
            r#ref: "main".to_string(),
            key_column: "key".to_string(),
            value_columns: vec![
                ValueColumnConfig::named("c"),
                ValueColumnConfig::of("a", ValueFormat::Protobuf),
                ValueColumnConfig::named("b"),
            ],
            order_column: None,
            max_scan_rows: None,
            strict: false,
            lookups: None,
            residency: icetray_core::Residency::Full,
            resident_budget_bytes: None,
            row_cache: None,
        };
        let wire: Vec<(String, i32, u32, i32)> = value_columns_wire(&layout, &entry)
            .into_iter()
            .map(|column| (column.name, column.value_type, column.width, column.format))
            .collect();
        let opaque = i32::from(v1::ValueFormat::Opaque);
        assert_eq!(
            wire,
            [
                (
                    "a".to_string(),
                    i32::from(v1::ValueType::Binary),
                    0,
                    i32::from(v1::ValueFormat::Protobuf)
                ),
                (
                    "b".to_string(),
                    i32::from(v1::ValueType::Fixed),
                    u32::try_from(fixed_width).unwrap(),
                    opaque
                ),
                ("c".to_string(), i32::from(v1::ValueType::String), 0, opaque),
            ]
        );
    }

    #[test]
    fn test_key_type_wire_per_type() {
        let cases = [
            (KeyType::Long, v1::KeyType::Long),
            (KeyType::String, v1::KeyType::String),
            (KeyType::Binary, v1::KeyType::Binary),
            (KeyType::Uuid, v1::KeyType::Uuid),
        ];
        assert_eq!(cases.map(|(key_type, _)| key_type), KeyType::ALL);
        for (key_type, wire) in cases {
            assert_eq!(key_type_wire(key_type), wire);
        }
    }

    #[test]
    fn test_direction_from_wire_names_both_directions_and_refuses_every_other_number() {
        assert_eq!(
            direction_from_wire(i32::from(v1::Direction::NewestFirst)),
            Ok(Direction::NewestFirst)
        );
        assert_eq!(
            direction_from_wire(i32::from(v1::Direction::OldestFirst)),
            Ok(Direction::OldestFirst)
        );
        for number in [i32::from(v1::Direction::Unspecified), UNKNOWN_DIRECTION, -1] {
            assert_eq!(
                direction_from_wire(number),
                Err(WireScanFault::Direction { number })
            );
        }
    }

    #[test]
    fn test_order_value_from_wire_carries_each_variant_and_refuses_a_bound_with_none() {
        let cases = [
            (
                order_value::Value::Long(LONG_VALUE),
                OrderValue::Long(LONG_VALUE),
            ),
            (
                order_value::Value::Str(STR_VALUE.to_string()),
                OrderValue::Str(STR_VALUE.to_string()),
            ),
            (
                order_value::Value::Bin(vec![0u8; SHORT_BIN_LEN]),
                OrderValue::Bytes(vec![0u8; SHORT_BIN_LEN]),
            ),
        ];
        for (wire, order) in cases {
            let bound = v1::OrderValue { value: Some(wire) };
            assert_eq!(
                order_value_from_wire(Some(bound), BoundSide::Start),
                Ok(Some(order))
            );
        }
        assert_eq!(order_value_from_wire(None, BoundSide::Before), Ok(None));
        for which in [BoundSide::Start, BoundSide::Before] {
            assert_eq!(
                order_value_from_wire(Some(v1::OrderValue { value: None }), which),
                Err(WireScanFault::EmptyBound { bound: which })
            );
        }
    }

    #[test]
    fn test_from_wire_scan_fault_is_invalid_argument_per_variant() {
        let faults = [
            WireScanFault::Direction { number: 0 },
            WireScanFault::EmptyBound {
                bound: BoundSide::Start,
            },
        ];
        for fault in faults {
            assert_eq!(Status::from(fault).code(), Code::InvalidArgument);
        }
    }

    #[test]
    fn test_order_value_wire_round_trips_each_variant() {
        let values = [
            OrderValue::Long(LONG_VALUE),
            OrderValue::Str(STR_VALUE.to_string()),
            OrderValue::Bytes(vec![0u8; SHORT_BIN_LEN]),
        ];
        for value in values {
            assert_eq!(
                order_value_from_wire(Some(order_value_wire(value.clone())), BoundSide::Start),
                Ok(Some(value))
            );
        }
    }

    #[test]
    fn test_scanned_wire_keeps_each_row_in_order_with_its_values_and_next() {
        let bytes = Bytes::from_static(STR_VALUE.as_bytes());
        let scanned = Scanned {
            rows: vec![
                ScanRow {
                    order: OrderValue::Long(LONG_VALUE),
                    values: vec![Some(bytes.clone()), None],
                },
                ScanRow {
                    order: OrderValue::Long(LONG_VALUE - 1),
                    values: vec![Some(Bytes::new()), None],
                },
            ],
            next: Some(OrderValue::Long(LONG_VALUE - 1)),
        };
        let v1::ScanResult { rows, next } = scanned_wire(scanned);
        assert_eq!(
            order_value_from_wire(next, BoundSide::Before),
            Ok(Some(OrderValue::Long(LONG_VALUE - 1)))
        );
        let orders: Vec<_> = rows
            .iter()
            .map(|row| order_value_from_wire(row.order.clone(), BoundSide::Start))
            .collect();
        assert_eq!(
            orders,
            [
                Ok(Some(OrderValue::Long(LONG_VALUE))),
                Ok(Some(OrderValue::Long(LONG_VALUE - 1)))
            ]
        );
        assert_eq!(
            rows[0].values,
            [
                ColumnValue {
                    kind: Some(column_value::Kind::Value(bytes)),
                },
                ColumnValue {
                    kind: Some(column_value::Kind::NullValue(NullValue {})),
                },
            ]
        );
        assert_eq!(
            rows[1].values[0],
            ColumnValue {
                kind: Some(column_value::Kind::Value(Bytes::new())),
            },
            "a value of zero length is not a null"
        );
    }

    #[test]
    fn test_order_type_wire_maps_every_order_type_and_the_fixed_width() {
        let width = u32::try_from(SHORT_BIN_LEN).unwrap();
        let cases = [
            (OrderType::Int, v1::OrderType::Int, 0),
            (OrderType::Long, v1::OrderType::Long, 0),
            (OrderType::Date, v1::OrderType::Date, 0),
            (OrderType::Timestamp, v1::OrderType::Timestamp, 0),
            (OrderType::TimestampTz, v1::OrderType::Timestamptz, 0),
            (OrderType::TimestampNs, v1::OrderType::TimestampNs, 0),
            (OrderType::TimestampTzNs, v1::OrderType::TimestamptzNs, 0),
            (OrderType::String, v1::OrderType::String, 0),
            (OrderType::Binary, v1::OrderType::Binary, 0),
            (OrderType::Fixed(width), v1::OrderType::Fixed, width),
        ];
        for (order_type, wire, wire_width) in cases {
            assert_eq!(
                order_type_wire(order_type),
                (wire, wire_width),
                "{order_type}"
            );
        }
    }

    /// Value lengths of the sample rows, chosen to cross the lengths at
    /// which a varint grows a byte: 127 and 128, and 16,383 and 16,384.
    const SAMPLE_VALUE_LENS: [usize; 6] = [10, 127, 128, 16_383, 16_384, 300];

    /// The sample snapshot id: nine varint bytes, as an Iceberg id is.
    const SAMPLE_SNAPSHOT: i64 = 0x3fff_ffff_ffff_fff1;

    /// A Scan answer of `SAMPLE_VALUE_LENS`' rows, the first `rows` of
    /// them, each with a long order value and one value, and `next`.
    fn sample_scanned(rows: usize, next: Option<i64>) -> Scanned {
        Scanned {
            rows: SAMPLE_VALUE_LENS[..rows]
                .iter()
                .zip(0_i64..)
                .map(|(&len, order)| ScanRow {
                    order: OrderValue::Long(order * SAMPLE_ORDER_STEP),
                    values: vec![Some(Bytes::from(vec![7u8; len]))],
                })
                .collect(),
            next: next.map(OrderValue::Long),
        }
    }

    /// The step between the sample rows' order values: wide enough that
    /// their varints grow from one byte to two.
    const SAMPLE_ORDER_STEP: i64 = 100;

    /// The `ScanResponse` the sample answer is sent as.
    fn sample_response(rows: usize, next: Option<i64>) -> ScanResponse {
        let v1::ScanResult { rows, next } = scanned_wire(sample_scanned(rows, next));
        ScanResponse {
            rows,
            next,
            snapshot_id: SAMPLE_SNAPSHOT,
        }
    }

    fn sample_lookup(rows: usize, next: Option<i64>) -> ScanLookup {
        ScanLookup {
            scanned: sample_scanned(rows, next),
            snapshot_id: icetray_core::SnapshotId(SAMPLE_SNAPSHOT),
        }
    }

    /// The response a Scan read in `direction` at a limit of `taken` rows
    /// sends, where one at a larger limit sent `response`, whose rows carry
    /// long order values: its first `taken` rows, and the next page's bound
    /// after the last of them, newest first its order value and oldest
    /// first one more, or at all of the rows the response's own.
    fn at_limit(response: &ScanResponse, taken: usize, direction: Direction) -> ScanResponse {
        let next = if taken == response.rows.len() {
            response.next.clone()
        } else {
            taken.checked_sub(1).map(|last| {
                let Some(v1::OrderValue {
                    value: Some(order_value::Value::Long(order)),
                }) = response.rows[last].order
                else {
                    panic!("a sample row has a long order value");
                };
                order_value_wire(OrderValue::Long(match direction {
                    Direction::NewestFirst => order,
                    Direction::OldestFirst => order + 1,
                }))
            })
        };
        ScanResponse {
            rows: response.rows[..taken].to_vec(),
            next,
            snapshot_id: response.snapshot_id,
        }
    }

    /// A response exactly at the bound is sent with its size, and one byte
    /// over is refused naming the size, the bound, and the rows that fit.
    #[test]
    fn test_scan_within_bound_sends_at_the_bound_and_refuses_one_byte_over() {
        let rows = SAMPLE_VALUE_LENS.len();
        let direction = Direction::NewestFirst;
        let size = sample_response(rows, None).encoded_len();
        let (response, bytes) =
            scan_within_bound(sample_lookup(rows, None), direction, size).unwrap();
        assert_eq!(bytes, size);
        assert_eq!(response, sample_response(rows, None));

        let refused =
            scan_within_bound(sample_lookup(rows, None), direction, size - 1).unwrap_err();
        let fits = fits_rows(&sample_response(rows, None), direction, size - 1);
        assert_eq!(
            refused,
            Oversize {
                bytes: size,
                bound: size - 1,
                fits: Fits::Rows(fits),
            }
        );
        assert_eq!(fits, rows - 1, "only the last row does not fit");
    }

    /// The most leading rows of `response`, sent as a Scan in `direction`
    /// at that limit sends them, with its `next`, whose encoded size is
    /// within `bound`, found by encoding each prefix.
    fn fits_rows(response: &ScanResponse, direction: Direction, bound: usize) -> usize {
        (0..=response.rows.len())
            .filter(|&rows| at_limit(response, rows, direction).encoded_len() <= bound)
            .max()
            .unwrap_or(0)
    }

    /// For a bound at, one under, and one over each prefix's size, sent with
    /// the `next` a Scan at that limit sends in either direction, with and
    /// without rows past the response, the rows reported to fit are the
    /// longest prefix whose encoding is within it, none when not even the
    /// empty response is.
    #[test]
    fn test_scan_within_bound_counts_the_longest_prefix_that_fits() {
        let rows = SAMPLE_VALUE_LENS.len();
        let past = i64::try_from(rows).unwrap() * SAMPLE_ORDER_STEP;
        for direction in [Direction::OldestFirst, Direction::NewestFirst] {
            for next in [None, Some(past)] {
                let response = sample_response(rows, next);
                let mut reported = std::collections::BTreeSet::new();
                for prefix in 0..=rows {
                    let size = at_limit(&response, prefix, direction).encoded_len();
                    for bound in [size - 1, size, size + 1] {
                        if bound >= response.encoded_len() {
                            continue;
                        }
                        let Err(oversize) =
                            scan_within_bound(sample_lookup(rows, next), direction, bound)
                        else {
                            panic!("bound {bound} is below the response's size");
                        };
                        let expected = fits_rows(&response, direction, bound);
                        assert_eq!(
                            oversize.fits,
                            Fits::Rows(expected),
                            "{direction:?} {next:?} bound {bound}"
                        );
                        reported.insert(expected);
                    }
                }
                assert_eq!(
                    reported,
                    (0..rows).collect(),
                    "{direction:?} {next:?}: every count from none to all but one is reported"
                );
            }
        }
    }

    /// The length of the long order value of
    /// `test_scan_within_bound_counts_a_prefix_past_a_shorter_one_that_does_not_fit`:
    /// long enough that a `next` naming it outweighs a row and a short one.
    const LONG_ORDER_CHARS: usize = 1_000;

    /// Newest first, a `next` is the last row's order value, so a prefix
    /// ending on a long one can be over the bound where a longer prefix,
    /// ending on a short one, is within it: the count is of the longest
    /// prefix that fits, not the first that does not.
    #[test]
    fn test_scan_within_bound_counts_a_prefix_past_a_shorter_one_that_does_not_fit() {
        let long_order = "c".repeat(LONG_ORDER_CHARS);
        let orders = [long_order.as_str(), "b", "a"];
        let order = |at: usize| OrderValue::Str(orders[at].to_string());
        let scanned = Scanned {
            rows: (0..orders.len())
                .map(|at| ScanRow {
                    order: order(at),
                    values: Vec::new(),
                })
                .collect(),
            next: Some(order(orders.len() - 1)),
        };
        let wire = scanned_wire(scanned.clone());
        let prefix = |taken: usize| ScanResponse {
            rows: wire.rows[..taken].to_vec(),
            next: Some(order_value_wire(order(taken - 1))),
            snapshot_id: SAMPLE_SNAPSHOT,
        };
        let (one, two) = (prefix(1).encoded_len(), prefix(2).encoded_len());
        assert!(
            two < one,
            "two rows ending on a short order value are the smaller"
        );
        let lookup = ScanLookup {
            scanned,
            snapshot_id: icetray_core::SnapshotId(SAMPLE_SNAPSHOT),
        };
        let refused = scan_within_bound(lookup, Direction::NewestFirst, two).unwrap_err();
        assert_eq!(refused.fits, Fits::Rows(2));
    }

    /// A `MultiScan` response reports the leading results that fit, a key
    /// listed twice counting at each position, and is sent at the bound.
    #[test]
    fn test_multi_scan_within_bound_counts_the_leading_results_that_fit() {
        let results = [2, 0, 3, 2];
        let past =
            |rows: usize| (rows > 0).then(|| i64::try_from(rows).unwrap() * SAMPLE_ORDER_STEP);
        let lookup = || MultiScanLookup {
            results: results
                .iter()
                .map(|&rows| sample_scanned(rows, past(rows)))
                .collect(),
            snapshot_id: icetray_core::SnapshotId(SAMPLE_SNAPSHOT),
        };
        let response = |keys: usize| MultiScanResponse {
            results: results[..keys]
                .iter()
                .map(|&rows| scanned_wire(sample_scanned(rows, past(rows))))
                .collect(),
            snapshot_id: SAMPLE_SNAPSHOT,
        };
        let size = response(results.len()).encoded_len();
        let (sent, bytes) = multi_scan_within_bound(lookup(), size).unwrap();
        assert_eq!((sent, bytes), (response(results.len()), size));

        for keys in 0..results.len() {
            let fits = response(keys).encoded_len();
            let over = response(keys + 1).encoded_len();
            for bound in [fits, over - 1] {
                let refused = multi_scan_within_bound(lookup(), bound).unwrap_err();
                assert_eq!(
                    refused,
                    Oversize {
                        bytes: size,
                        bound,
                        fits: Fits::Keys(keys),
                    },
                    "bound {bound}"
                );
            }
        }
    }
}
