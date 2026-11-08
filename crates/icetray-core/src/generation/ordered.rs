//! An ordered table's uniqueness across files (RFD 025, Refresh): a key
//! and order value together live in at most one file. A load checks only
//! where a repeated live pair can occur: among the files of an overlap
//! group (`overlap_groups`) that holds a file that is new, or reused with
//! deletes that shrank (RFD 021). Such a checked file meets another when
//! one key can fall in both (RFD 005's rule across buckets and bucketings)
//! and their key ranges and order ranges both meet (`meetings`). Of two
//! files that meet, the order pages that can hold a pair both hold are
//! chosen by the order fences, a page's pairs lying from its fence up to
//! the next page's (`chosen_pages`), and one k-way merge by key and order
//! value per group runs over the chosen pages, every group's at once, each
//! page read at most once: a file's current row group whenever its merge
//! reaches it, and later row groups ahead of need, in the order each merge
//! reaches them, while the bytes every merge holds and has in flight stay
//! within `MERGE_READ_AHEAD_BYTES`. The refusal is the first group's in
//! group order that refuses, as merging the groups one at a time would
//! give. A group where no file meets a checked one, as an append of later
//! rows does, is merged with nothing and reads nothing.

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool};

use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::{FuturesUnordered, StreamExt as _};
use tokio::task::JoinError;

use super::{LoadedFile, Member, Span, buckets_may_share_a_key, load_concurrency};
use crate::deletes::FileDeletes;
use crate::error::{Error, RefusalReason, Result};
use crate::index::{DeletedRows as _, OrderFences, Pair, PairChunks, PairPart, PairRead, Pairs};
use crate::order::OrderValue;
use crate::{blocking, shard};

/// One file as the check across files sees it: its span as the overlap
/// sweep sees it (`Span`), and its least and greatest order value. A
/// span's position in its slice stands for the file's.
#[derive(Clone, Copy, Debug)]
pub(super) struct OrderedSpan<'a> {
    pub(super) span: Span<'a>,
    pub(super) orders: (&'a OrderValue, &'a OrderValue),
}

impl<'a> OrderedSpan<'a> {
    /// The span of a file of an ordered table, from its key range and the
    /// order facts its verification recorded.
    pub(super) fn of(file: &'a LoadedFile) -> Self {
        let facts = file
            .index
            .order_facts()
            .expect("every file of an ordered table records its order facts");
        Self {
            span: Span::of(file),
            orders: (&facts.min, &facts.max),
        }
    }

    /// The rows both `self` and `other` can hold: the key range and the
    /// order range they share, as fences compare them.
    fn shared(self, other: Self) -> Shared<'a> {
        let (ours, theirs) = (self.span, other.span);
        Shared {
            keys: (
                PairPart::of_key(ours.first.max(theirs.first)),
                PairPart::of_key(ours.last.min(theirs.last)),
            ),
            orders: (
                PairPart::of_order(self.orders.0.max(other.orders.0)),
                PairPart::of_order(self.orders.1.min(other.orders.1)),
            ),
        }
    }
}

/// A closed key range and a closed order range two files that meet share:
/// where a pair both hold lies.
#[derive(Clone, Copy)]
struct Shared<'a> {
    keys: (PairPart<'a>, PairPart<'a>),
    orders: (PairPart<'a>, PairPart<'a>),
}

/// The bytes of chosen pages within which a load's merges read row groups
/// ahead of need, 64 MiB, one budget for every overlap group the load
/// merges at once: a row group is read ahead only while the bytes the
/// merges hold and have in flight, that row group's included, stay within
/// this. A file's current row group is read whatever it takes, so a file
/// of row groups larger than this still merges, one row group at a time,
/// and the load's check across files holds at most this beyond one row
/// group per file.
pub(super) const MERGE_READ_AHEAD_BYTES: u64 = 64 * 1024 * 1024;

/// What a load's check across files did, or one merge of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RowCheck {
    /// The overlap groups whose check merged files.
    pub(super) merges: usize,
    /// The comparisons the check made: of keys and order values while
    /// sweeping the groups, sorts and searches included; of two files'
    /// buckets; of order pages' pairs while choosing pages; and of two
    /// cursors' pairs while merging.
    pub(super) compared: u64,
    /// The most bytes of chosen pages the merges held and had in flight at
    /// once, together (`ReadAhead::taken`).
    pub(super) held: u64,
}

/// The members of a sweep whose order ranges are open, by their closed
/// ranges of order ranks (`order_ranks`): a segment tree over the ranks
/// whose every node lists the open members whose range it covers in the
/// range's canonical decomposition, and the open members by the rank their
/// range starts at. A member whose range holds a rank is listed at exactly
/// one node on that rank's path to the root, so `meeting` finds each open
/// member whose range meets a range once, at the cost of the path, the
/// members it reports, and closed members it passes over, each of which
/// it drops from the node it found them at.
struct OpenRanges {
    /// The tree's leaves, a power of two at or above the ranks.
    leaves: usize,
    nodes: Vec<Vec<usize>>,
    starts: BTreeSet<(usize, usize)>,
    open: Vec<bool>,
    start_of: Vec<usize>,
}

impl OpenRanges {
    /// No member open, over `ranks` ranks and `members` members.
    fn new(ranks: usize, members: usize) -> Self {
        let leaves = ranks.next_power_of_two();
        Self {
            leaves,
            nodes: vec![Vec::new(); 2 * leaves],
            starts: BTreeSet::new(),
            open: vec![false; members],
            start_of: vec![0; members],
        }
    }

    /// Opens `member`, whose order range is the ranks `range` spans.
    fn open(&mut self, member: usize, range: (usize, usize)) {
        self.open[member] = true;
        self.start_of[member] = range.0;
        self.starts.insert((range.0, member));
        let (mut low, mut high) = (range.0 + self.leaves, range.1 + self.leaves + 1);
        while low < high {
            if low % 2 == 1 {
                self.nodes[low].push(member);
                low += 1;
            }
            if high % 2 == 1 {
                high -= 1;
                self.nodes[high].push(member);
            }
            low /= 2;
            high /= 2;
        }
    }

    /// Closes `member`, if open.
    fn close(&mut self, member: usize) {
        if std::mem::take(&mut self.open[member]) {
            self.starts.remove(&(self.start_of[member], member));
        }
    }

    /// Adds to `met` every open member whose range meets the ranks `range`
    /// spans: those whose range holds its start, and those whose range
    /// starts past its start and at or before its end.
    fn meeting(&mut self, range: (usize, usize), met: &mut Vec<usize>) {
        let mut node = range.0 + self.leaves;
        while node > 0 {
            let open = &self.open;
            self.nodes[node].retain(|&member| open[member]);
            met.extend_from_slice(&self.nodes[node]);
            node /= 2;
        }
        if range.0 < range.1 {
            met.extend(
                self.starts
                    .range((range.0 + 1, 0)..=(range.1, usize::MAX))
                    .map(|&(_, member)| member),
            );
        }
    }
}

/// Per member of `orders`, its closed order range as ranks among the
/// distinct order values the ranges end at, and how many there are: one
/// sort of the ends, which takes a pass where they already ascend, as an
/// appended bucket's do, and one pass over them. Every comparison is
/// counted in `compared`.
fn order_ranks(
    orders: &[(&OrderValue, &OrderValue)],
    compared: &mut u64,
) -> (Vec<(usize, usize)>, usize) {
    let mut ends: Vec<(usize, bool, &OrderValue)> = orders
        .iter()
        .enumerate()
        .flat_map(|(member, &(low, high))| [(member, false, low), (member, true, high)])
        .collect();
    ends.sort_by(|a, b| {
        *compared += 1;
        a.2.cmp(b.2)
    });
    let mut ranks = vec![(0, 0); orders.len()];
    let mut rank = 0;
    let mut previous: Option<&OrderValue> = None;
    for &(member, high, value) in &ends {
        if let Some(previous) = previous {
            *compared += 1;
            if previous != value {
                rank += 1;
            }
        }
        previous = Some(value);
        if high {
            ranks[member].1 = rank;
        } else {
            ranks[member].0 = rank;
        }
    }
    (ranks, rank + 1)
}

/// Per member of `group`, positions in `spans` of the members of one
/// overlap group, the members it meets where one of the two is checked, by
/// position in the group: one key can fall in both their buckets
/// (`buckets_may_share_a_key`) and their key ranges and order ranges both
/// meet, ends included. `checked` marks the checked files by position in
/// `spans`. One sweep of the group by first key keeps the members whose key
/// range is still open, those whose last key is not below the next
/// member's first, by order range (`OpenRanges`); each member finds the
/// open members its order range meets, the open checked members alone when
/// it is not checked, and keeps those a key can share a bucket with. The
/// sweep costs a few sorts, a logarithmic step per member, and a step per
/// pair of members whose key and order ranges both meet, whichever
/// dimension the group's files overlap in. Every comparison is counted in
/// `compared`.
fn meetings(
    spans: &[OrderedSpan<'_>],
    checked: &[bool],
    group: &[usize],
    compared: &mut u64,
) -> Vec<Vec<usize>> {
    let span = |member: usize| spans[group[member]];
    let orders: Vec<_> = (0..group.len()).map(|member| span(member).orders).collect();
    let (ranks, distinct) = order_ranks(&orders, compared);
    let mut by_first: Vec<usize> = (0..group.len()).collect();
    by_first.sort_by(|&a, &b| {
        *compared += 1;
        span(a).span.first.cmp(span(b).span.first)
    });
    let mut by_last: Vec<usize> = (0..group.len()).collect();
    by_last.sort_by(|&a, &b| {
        *compared += 1;
        span(a).span.last.cmp(span(b).span.last)
    });
    let mut every = OpenRanges::new(distinct, group.len());
    let mut of_checked = OpenRanges::new(distinct, group.len());
    let mut found = vec![Vec::new(); group.len()];
    let mut met = Vec::new();
    let mut closing = 0;
    for &member in &by_first {
        let ours = span(member);
        while let Some(&next) = by_last.get(closing) {
            *compared += 1;
            if span(next).span.last >= ours.span.first {
                break;
            }
            every.close(next);
            of_checked.close(next);
            closing += 1;
        }
        let is_checked = checked[group[member]];
        let open = if is_checked {
            &mut every
        } else {
            &mut of_checked
        };
        open.meeting(ranks[member], &mut met);
        for theirs in met.drain(..) {
            *compared += 1;
            let other = span(theirs);
            if buckets_may_share_a_key(ours.span.place(), other.span.place()) {
                found[member].push(theirs);
                found[theirs].push(member);
            }
        }
        every.open(member, ranks[member]);
        if is_checked {
            of_checked.open(member, ranks[member]);
        }
    }
    found
}

/// The top of an order page's pairs: below the next page's fence, or, on
/// a file's last page, at or below its last key with its greatest order
/// value.
#[derive(Clone, Copy, Debug)]
enum Top<'a> {
    Below(Pair<'a>),
    AtMost(Pair<'a>),
}

impl<'a> Top<'a> {
    /// The pair the top is set by.
    fn pair(self) -> Pair<'a> {
        match self {
            Self::Below(pair) | Self::AtMost(pair) => pair,
        }
    }

    /// Whether `pair` lies under the top.
    fn covers(self, pair: Pair<'_>) -> bool {
        match self {
            Self::Below(top) => pair < top,
            Self::AtMost(top) => pair <= top,
        }
    }

    /// The lower of `self` and `other`, the one a pair below both lies
    /// under.
    fn lower(self, other: Self) -> Self {
        match self.pair().cmp(&other.pair()) {
            Ordering::Less => self,
            Ordering::Greater => other,
            Ordering::Equal => match (self, other) {
                (Self::AtMost(_), Self::Below(_)) => other,
                (Self::Below(_), Self::AtMost(_) | Self::Below(_))
                | (Self::AtMost(_), Self::AtMost(_)) => self,
            },
        }
    }
}

/// Whether a pair from `low` up to `top` can lie in `shared`: within one
/// key, the key in the shared keys and the order values from `low`'s up to
/// the top's meeting the shared orders; across keys, `low` at or below the
/// shared range's greatest pair and its least under the top. Each is what
/// a pair in both the range and `shared` requires, so a range for which it
/// fails holds no such pair.
fn may_hold(low: Pair<'_>, top: Top<'_>, shared: Shared<'_>) -> bool {
    let (keys, orders) = (shared.keys, shared.orders);
    let key = top.pair().key;
    if low.key == key {
        keys.0 <= key
            && key <= keys.1
            && low.order <= orders.1
            && top.covers(Pair {
                key,
                order: orders.0,
            })
    } else {
        low <= Pair {
            key: keys.1,
            order: orders.1,
        } && top.covers(Pair {
            key: keys.0,
            order: orders.0,
        })
    }
}

/// One order page of a member of a group as `chosen_pages` sweeps it: its
/// pairs from `low` up to `top`, and whether it is chosen so far.
struct PageRange<'a> {
    row_group: usize,
    page: usize,
    low: Pair<'a>,
    top: Top<'a>,
    chosen: bool,
}

/// A walk over one file's order pages in pair order: the row group and
/// page it stands on, its fences, and the top of its last page.
struct PageWalk<'a> {
    fences: &'a OrderFences,
    last: Pair<'a>,
    row_group: usize,
    page: usize,
}

impl<'a> PageWalk<'a> {
    /// The walk from the first order page of the file `fences` and `last`,
    /// its last key with its greatest order value, describe; `None` for a
    /// file without one.
    fn first(fences: &'a OrderFences, last: Pair<'a>) -> Option<Self> {
        Self {
            fences,
            last,
            row_group: 0,
            page: 0,
        }
        .on_a_page()
    }

    /// The walk from the first page at or after where it stands.
    fn on_a_page(mut self) -> Option<Self> {
        while self.row_group < self.fences.row_groups()
            && self.page >= self.fences.pages(self.row_group)
        {
            self.row_group += 1;
            self.page = 0;
        }
        (self.row_group < self.fences.row_groups()).then_some(self)
    }

    /// The walk from the page after the one it stands on.
    fn next(mut self) -> Option<Self> {
        self.page += 1;
        self.on_a_page()
    }

    /// The page it stands on.
    fn range(&self) -> PageRange<'a> {
        let (low, next) = self.fences.page_pairs(self.row_group, self.page);
        PageRange {
            row_group: self.row_group,
            page: self.page,
            low,
            top: next.map_or(Top::AtMost(self.last), Top::Below),
            chosen: false,
        }
    }
}

/// A member's next page in `chosen_pages`' sweep, in a max-heap that pops
/// the least fence first, the lesser member first among equal ones, every
/// comparison counted in `compared`.
struct Next<'a, 'c> {
    low: Pair<'a>,
    member: usize,
    compared: &'c Cell<u64>,
}

impl PartialEq for Next<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Next<'_, '_> {}

impl PartialOrd for Next<'_, '_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Next<'_, '_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compared.set(self.compared.get() + 1);
        other
            .low
            .cmp(&self.low)
            .then_with(|| other.member.cmp(&self.member))
    }
}

/// One row group of a file and the order pages of it a merge reads,
/// ascending, with the step of `chosen_pages`' sweep that passed the last
/// of them: the sweep takes pages by fence, in the order the merge takes
/// pairs, so the steps that passed two files' row groups order the points
/// where the merge runs dry of them.
#[derive(Clone, Debug)]
struct GroupPages {
    row_group: usize,
    pages: Vec<usize>,
    passed: usize,
}

/// `page` after `groups`, the pages chosen of one file so far in file
/// order, when it is chosen, the sweep passing it at step `passed`.
fn keep(groups: &mut Vec<GroupPages>, page: &PageRange<'_>, passed: usize) {
    if !page.chosen {
        return;
    }
    match groups.last_mut() {
        Some(group) if group.row_group == page.row_group => {
            group.pages.push(page.page);
            group.passed = passed;
        }
        Some(_) | None => groups.push(GroupPages {
            row_group: page.row_group,
            pages: vec![page.page],
            passed,
        }),
    }
}

/// Per member of `group` that `meetings` found `counterparts` for, the
/// order pages that can hold a pair one of its counterparts holds, by the
/// fences: of two pages, one of each file, whose pairs' ranges meet, both
/// are chosen when a pair both files' ranges share can lie in the range
/// they meet in (`may_hold`). One k-way merge of those members' pages by
/// fence, each member's pages already in pair order (`PageWalk`), keeps
/// each member's latest page, the one whose range holds the merge's pair
/// when any does, since a file's pages tile its pairs; each page is tested
/// against the latest page of each counterpart, and a counterpart's later
/// pages test it in turn. The cost is a heap step per page and a step per
/// page per counterpart, and what it holds besides the pages chosen is a
/// page per member. Each member chosen with its file's position in `files`
/// and its chosen pages per row group, in file order.
fn chosen_pages(
    spans: &[OrderedSpan<'_>],
    files: &[Arc<LoadedFile>],
    group: &[usize],
    counterparts: &[Vec<usize>],
    compared: &mut u64,
) -> Vec<(usize, Vec<GroupPages>)> {
    let counted = Cell::new(0);
    let mut walks: Vec<Option<PageWalk<'_>>> = Vec::with_capacity(group.len());
    let mut heap = BinaryHeap::new();
    for (member, theirs) in counterparts.iter().enumerate() {
        let walk = if theirs.is_empty() {
            None
        } else {
            let fences = files[group[member]]
                .index
                .order_fences()
                .expect("every file of an ordered table records its order fences");
            let span = spans[group[member]];
            PageWalk::first(fences, Pair::of(span.span.last, span.orders.1))
        };
        if let Some(walk) = &walk {
            heap.push(Next {
                low: walk.range().low,
                member,
                compared: &counted,
            });
        }
        walks.push(walk);
    }
    let mut latest: Vec<Option<PageRange<'_>>> = (0..group.len()).map(|_| None).collect();
    let mut reads: Vec<Vec<GroupPages>> = vec![Vec::new(); group.len()];
    let mut step = 0;
    while let Some(Next { member, .. }) = heap.pop() {
        step += 1;
        let walk = walks[member]
            .take()
            .expect("a member in the heap stands on a page");
        let mut ours = walk.range();
        if let Some(before) = latest[member].take() {
            keep(&mut reads[member], &before, step);
        }
        for &theirs in &counterparts[member] {
            let Some(page) = latest[theirs].as_mut() else {
                continue;
            };
            *compared += 1;
            if !page.top.covers(ours.low) {
                continue;
            }
            let shared = spans[group[member]].shared(spans[group[theirs]]);
            if may_hold(ours.low, ours.top.lower(page.top), shared) {
                ours.chosen = true;
                page.chosen = true;
            }
        }
        latest[member] = Some(ours);
        walks[member] = walk.next();
        if let Some(walk) = &walks[member] {
            heap.push(Next {
                low: walk.range().low,
                member,
                compared: &counted,
            });
        }
    }
    drop(heap);
    *compared += counted.get();
    for (member, page) in latest.into_iter().enumerate() {
        if let Some(page) = page {
            keep(&mut reads[member], &page, step);
        }
    }
    reads
        .into_iter()
        .enumerate()
        .filter(|(_, groups)| !groups.is_empty())
        .map(|(member, groups)| (group[member], groups))
        .collect()
}

/// Checks every overlap group of `groups` (`overlap_groups`) that holds a
/// checked file of `files`, sorted as `members` is: one new, or reused with
/// deletes that shrank (RFD 021). Per group, the sweep (`meetings`) finds
/// which files meet, one of each two checked; the order pages that can
/// hold a pair two of them share are chosen without I/O (`chosen_pages`);
/// then one merge per group (`refuse_duplicate_rows`), every group's at
/// once, reads each chosen page at most once, reading ahead within
/// `read_ahead` bytes across all of them, and refuses a live pair two files
/// share where one is checked; `deletes` are the rows each file's deletes
/// remove. A group where no page is chosen reads nothing.
pub(super) async fn verify_rows(
    table: &str,
    members: &[Member],
    files: &[Arc<LoadedFile>],
    groups: &[Vec<usize>],
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
    read_ahead: u64,
) -> Result<RowCheck> {
    let GroupMerges {
        checked,
        merges,
        compared,
    } = GroupMerges::of(members, files, groups);
    let mut check =
        refuse_duplicate_rows(table, files, &checked, merges, deletes, read_ahead).await?;
    check.compared += compared;
    Ok(check)
}

/// What `verify_rows` merges: which files of `files` it checks, the chosen
/// pages of each overlap group that merges, in group order, and the
/// comparisons choosing them took.
struct GroupMerges {
    checked: Vec<bool>,
    merges: Vec<Vec<(usize, Vec<GroupPages>)>>,
    compared: u64,
}

impl GroupMerges {
    /// The merges of `groups`, `members` saying which of `files` are
    /// checked: per group that holds a checked file, the sweep and then the
    /// chosen pages, a group where no page is chosen left out.
    fn of(members: &[Member], files: &[Arc<LoadedFile>], groups: &[Vec<usize>]) -> Self {
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(|file| OrderedSpan::of(file)).collect();
        let checked: Vec<bool> = members
            .iter()
            .map(|member| !member.reused || member.shrank)
            .collect();
        let mut compared = 0;
        let mut merges = Vec::new();
        for group in groups {
            if !group.iter().any(|&at| checked[at]) {
                continue;
            }
            let counterparts = meetings(&spans, &checked, group, &mut compared);
            let reads = chosen_pages(&spans, files, group, &counterparts, &mut compared);
            if !reads.is_empty() {
                merges.push(reads);
            }
        }
        Self {
            checked,
            merges,
            compared,
        }
    }
}

/// One row group of a cursor's file as read: the cursor, the row group's
/// place among the cursor's, and its chosen pages.
type Arrival = (usize, usize, PairChunks);

/// One read of a row group of a merge as it ended: its cursor among every
/// merge's cursors, the row group's place among the cursor's, its bytes
/// (`PairRead::bytes`), and what it read, `None` where its merge ended
/// first and aborted it.
struct Landed {
    cursor: usize,
    place: usize,
    bytes: u64,
    read: Option<Result<PairChunks>>,
}

/// A read of one row group of a merge (`FileIndex::pair_chunks`).
type RowGroupRead = BoxFuture<'static, Landed>;

/// A step of a merge on the blocking pool as it returns: the merge, and its
/// state with why the step stopped, or the pool's refusal to run it.
type Stepped = (
    usize,
    std::result::Result<(MergeState, Result<Progress>), LostStep>,
);

/// A step the blocking pool dropped unstarted as the runtime shut down,
/// and with it the state it owned, which no retire hands back.
struct LostStep {
    /// The pool's refusal.
    canceled: JoinError,
    /// The bytes the state owned with the row groups handed to it
    /// (`MergeState::owned`), known when the step was spawned.
    owned: u64,
}

/// Merges the chosen pages each of `merges` names of `files`, one merge
/// per overlap group, in group order (`MergeState::resume`), the rows
/// `deletes` removes not live, every merge at once: each steps on the
/// blocking pool over owned data while the others step and reads go on, at
/// most one step per worker of the runtime at a time (`shard::workers`),
/// the merge first in group order first. The merges share one read-ahead
/// budget and the load's slots (`ReadAhead`): a cursor's next row group is
/// read once the cursor reaches it, whatever it takes; beyond those, row
/// groups are read ahead of need while the bytes every merge holds and has
/// in flight stay within `read_ahead`, at most `load_concurrency()` reads
/// at once with a slot left for each cursor still to read. A step stops
/// for the async side when a cursor waits on a row group not yet read,
/// when it has dropped the bytes the next read ahead needs, or when its
/// merge ends.
///
/// The outcome is the one merging the groups one at a time gives: the
/// failure of the first merge in group order that fails, once every merge
/// before it has passed, else every merge's pass. Once a merge fails, every
/// merge after it stops, its outcome no longer able to change the load's:
/// its step ends before its next pair, its reads under way are aborted, and
/// what it holds is dropped. A step under way when the outcome is known
/// runs on the blocking pool after this returns, up to its next stop
/// check, and holds its row groups until then. A read that fails, ahead of
/// need or not, fails its merge at once, even where the merge would have
/// refused before it reached that row group. A canceled step is
/// `Error::VerifyCanceled` for its merge's first checked file.
async fn refuse_duplicate_rows(
    table: &str,
    files: &[Arc<LoadedFile>],
    checked: &[bool],
    merges: Vec<Vec<(usize, Vec<GroupPages>)>>,
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
    read_ahead: u64,
) -> Result<RowCheck> {
    let mut ahead = ReadAhead::new(files, &merges, read_ahead, load_concurrency());
    Merges::new(table, files, checked, merges, deletes)
        .drive(&mut ahead)
        .await
}

/// Where a merge of `refuse_duplicate_rows` stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// It steps, or waits for its next step.
    Running,
    /// It failed or stopped.
    Ended,
    /// It passed, and so ended.
    Passed,
}

/// The merges of `refuse_duplicate_rows`, in group order, between their
/// steps: what each holds while it waits, where each stands, and the first
/// that failed.
struct Merges {
    /// Each merge's state between steps: `None` while it steps on the
    /// blocking pool, and once it has ended and dropped what it held.
    states: Vec<Option<MergeState>>,
    /// Each merge's `MergeState::stop`, which a step under way reads.
    stops: Vec<Arc<AtomicBool>>,
    /// Where each merge stands, which `ReadAhead` reads when a read for it
    /// lands.
    outcomes: Vec<Outcome>,
    /// The location of each merge's first checked file, which names a
    /// canceled step of it.
    firsts: Vec<String>,
    /// The first merge in group order known to have failed, and how.
    failed: Option<(usize, Error)>,
    /// The comparisons of the merges that passed.
    compared: u64,
    /// The steps under way on the blocking pool.
    steps: FuturesUnordered<BoxFuture<'static, Stepped>>,
}

impl Merges {
    /// The merges of `merges`, each the chosen pages of `files` one group's
    /// merge reads, every cursor waiting on its first row group.
    fn new(
        table: &str,
        files: &[Arc<LoadedFile>],
        checked: &[bool],
        merges: Vec<Vec<(usize, Vec<GroupPages>)>>,
        deletes: &Arc<[Option<Arc<FileDeletes>>]>,
    ) -> Self {
        let firsts = merges
            .iter()
            .map(|reads| {
                reads
                    .iter()
                    .find(|(at, _)| checked[*at])
                    .map(|(at, _)| files[*at].index.handle().location.clone())
                    .expect("a merge reads pages of a checked file")
            })
            .collect();
        let states: Vec<MergeState> = merges
            .into_iter()
            .map(|reads| {
                let cursors = reads
                    .into_iter()
                    .map(|(at, groups)| {
                        PairCursor::new(at, checked[at], Arc::clone(&files[at]), groups.len())
                    })
                    .collect();
                MergeState::new(table, cursors, Arc::clone(deletes))
            })
            .collect();
        Self {
            stops: states.iter().map(|state| Arc::clone(&state.stop)).collect(),
            outcomes: vec![Outcome::Running; states.len()],
            states: states.into_iter().map(Some).collect(),
            firsts,
            failed: None,
            compared: 0,
            steps: FuturesUnordered::new(),
        }
    }

    /// Runs the merges, reading through `ahead`, until the outcome is known
    /// (`settled`): starts the reads they want and the steps that can go
    /// on, then takes the next step to return or read to land.
    async fn drive(&mut self, ahead: &mut ReadAhead) -> Result<RowCheck> {
        let most_steps = shard::workers();
        loop {
            if let Some(outcome) = self.settled() {
                return outcome.map(|compared| RowCheck {
                    merges: self.states.len(),
                    compared,
                    held: ahead.most_taken,
                });
            }
            self.hand_over(ahead);
            ahead.start();
            let room = ahead.room();
            while self.steps.len() < most_steps {
                let Some((merge, mut state)) = self.next_step(ahead) else {
                    break;
                };
                let batch = std::mem::take(&mut ahead.landed[merge]);
                let owned = state.owned(&batch);
                self.steps.push(Box::pin(async move {
                    let stepped = blocking::on_blocking_pool(move || {
                        let progress = state.resume(batch, room);
                        (state, progress)
                    })
                    .await
                    .map_err(|canceled| LostStep { canceled, owned });
                    (merge, stepped)
                }));
            }
            if self.steps.is_empty() {
                let one = ahead
                    .in_flight
                    .next()
                    .await
                    .expect("a row group a merge waits on is being read");
                self.land(one, ahead);
                continue;
            }
            tokio::select! {
                biased;
                Some((merge, stepped)) = self.steps.next() => self.stepped(merge, stepped, ahead),
                Some(one) = ahead.in_flight.next() => self.land(one, ahead),
            }
        }
    }

    /// The outcome once it is known, the comparisons of every merge when
    /// all pass: the first failure in group order once every merge before
    /// it has passed.
    fn settled(&mut self) -> Option<Result<u64>> {
        let before = self
            .failed
            .as_ref()
            .map_or(self.outcomes.len(), |(at, _)| *at);
        if !self.outcomes[..before]
            .iter()
            .all(|&outcome| outcome == Outcome::Passed)
        {
            return None;
        }
        Some(match self.failed.take() {
            Some((_, error)) => Err(error),
            None => Ok(self.compared),
        })
    }

    /// Hands `ahead` the row groups each merge between steps came to wait
    /// on, and the bytes it dropped.
    fn hand_over(&mut self, ahead: &mut ReadAhead) {
        for (merge, state) in self.states.iter_mut().enumerate() {
            if let Some(state) = state {
                let first = ahead.merges[merge].start;
                ahead.want(
                    state
                        .wants
                        .drain(..)
                        .map(|(cursor, place)| (first + cursor, place)),
                );
                ahead.release(std::mem::take(&mut state.dropped));
            }
        }
    }

    /// The first merge in group order, with its state, that waits between
    /// steps and can go on: no cursor of it waits, or a read of it landed.
    fn next_step(&mut self, ahead: &ReadAhead) -> Option<(usize, MergeState)> {
        let merge = self.states.iter().enumerate().position(|(merge, state)| {
            state.as_ref().is_some_and(|state| {
                self.outcomes[merge] == Outcome::Running
                    && (state.waiting == 0 || !ahead.landed[merge].is_empty())
            })
        })?;
        let state = self.states[merge]
            .take()
            .expect("the merge found waits between steps");
        Some((merge, state))
    }

    /// Takes `one` as landed (`ReadAhead::land`), failing its merge when it
    /// failed.
    fn land(&mut self, one: Landed, ahead: &mut ReadAhead) {
        if let Some((merge, error)) = ahead.land(one, &self.outcomes) {
            self.fail(merge, error, ahead);
        }
    }

    /// Takes a step of `merge` as it returned: the merge waits for its
    /// next step, passes, or fails. A merge that ended while it stepped
    /// drops what it holds, and a lost step hands back what its state
    /// owned.
    fn stepped(
        &mut self,
        merge: usize,
        stepped: std::result::Result<(MergeState, Result<Progress>), LostStep>,
        ahead: &mut ReadAhead,
    ) {
        let (state, progress) = match stepped {
            Ok(stepped) => stepped,
            Err(LostStep { canceled, owned }) => {
                ahead.release(owned);
                if self.outcomes[merge] == Outcome::Running {
                    let error = Error::verify_canceled(&self.firsts[merge], canceled);
                    self.fail(merge, error, ahead);
                }
                return;
            }
        };
        self.states[merge] = Some(state);
        if self.outcomes[merge] != Outcome::Running {
            self.retire(merge, ahead);
            return;
        }
        match progress {
            Ok(Progress::Waiting | Progress::Room) => {}
            Ok(Progress::Done) => {
                if let Some(state) = &self.states[merge] {
                    self.compared += state.heads.compared;
                }
                self.end(merge, ahead);
                self.outcomes[merge] = Outcome::Passed;
            }
            Ok(Progress::Stopped) => self.end(merge, ahead),
            Err(error) => self.fail(merge, error, ahead),
        }
    }

    /// `merge` failed with `error`: unless a merge before it failed first,
    /// its failure is the outcome once every merge before it passes, and it
    /// and every merge after it end.
    fn fail(&mut self, merge: usize, error: Error, ahead: &mut ReadAhead) {
        if self.failed.as_ref().is_some_and(|(at, _)| *at < merge) {
            return;
        }
        self.failed = Some((merge, error));
        for later in merge..self.outcomes.len() {
            if self.outcomes[later] == Outcome::Running {
                self.end(later, ahead);
            }
        }
    }

    /// Ends `merge`: a step of it under way stops before its next pair,
    /// `ahead` reads nothing more for it (`ReadAhead::end`), and what it
    /// holds is dropped, now when it waits between steps, else once its
    /// step returns.
    fn end(&mut self, merge: usize, ahead: &mut ReadAhead) {
        self.outcomes[merge] = Outcome::Ended;
        self.stops[merge].store(true, atomic::Ordering::Relaxed);
        ahead.end(merge);
        self.retire(merge, ahead);
    }

    /// Drops the state of the ended `merge` when it waits between steps,
    /// handing `ahead` the bytes it held and dropped.
    fn retire(&mut self, merge: usize, ahead: &mut ReadAhead) {
        if let Some(state) = self.states[merge].take() {
            ahead.release(state.dropped + state.held());
        }
    }
}

/// When the merges of a load read each row group of their cursors, under
/// one budget and one set of slots: a cursor's row groups in file order,
/// the one a cursor waits on first, whatever it takes, and the others
/// ahead of need while the bytes every merge holds and has in flight, with
/// the row group's, stay within `budget`, and while a slot of
/// `concurrency` stays free for each cursor still to read, so that a
/// wanted read never queues behind reads ahead. Row groups are read ahead
/// in the order each merge reaches them across its files: a row group is
/// needed once its cursor runs dry of the one before, so they go by the
/// sweep step that passed the one before (`GroupPages::passed`), the
/// merges' row groups of one step in group order, and one that does not
/// fit waits, with every one after it, until the merges drop enough bytes
/// (`room`). The cursors of every merge are numbered together, in group
/// order and each merge's in its own.
struct ReadAhead {
    /// Each cursor's file.
    files: Vec<Arc<LoadedFile>>,
    /// Each merge's cursors.
    merges: Vec<Range<usize>>,
    /// Each cursor's merge.
    merge_of: Vec<usize>,
    /// Each cursor's row groups not yet read, in file order.
    unread: Vec<VecDeque<PairRead>>,
    /// How many of each cursor's row groups are read or being read.
    issued: Vec<usize>,
    /// Row groups cursors wait on, each a cursor and the row group's place
    /// among its own, read before any ahead.
    wanted: VecDeque<(usize, usize)>,
    /// The row groups after each cursor's first, as `wanted` names them, in
    /// the order the merges reach them.
    order: VecDeque<(usize, usize)>,
    /// The cursors with a row group not yet read or being read: each may
    /// come to want one, so reads ahead leave a slot of `concurrency` for
    /// each, and a wanted read never queues behind reads ahead.
    unfinished: usize,
    /// The reads under way, at most `concurrency`.
    in_flight: FuturesUnordered<RowGroupRead>,
    /// Per merge, what aborts each of its reads started.
    aborts: Vec<Vec<AbortHandle>>,
    concurrency: usize,
    /// Per merge, the reads done since it last took them, each by its
    /// cursor among the merge's own.
    landed: Vec<Vec<Arrival>>,
    /// The bytes of every row group read or being read that no merge has
    /// dropped (`PairRead::bytes`), and the most at once. A merge reports
    /// what it dropped when a step ends, and what it held when it ends, so
    /// the count stays at or above what is held and in flight. A cursor's
    /// decoded run is not counted, and it outlives what is: its byte-array
    /// keys and order values are slices of the pages they decoded from, the
    /// fetched bytes or the buffers a compressed page was decompressed into,
    /// so a run keeps those alive, and a cursor's last run of a row group
    /// stays after the row group's pages are dropped, until its next fill
    /// decodes over it or the merge ends.
    taken: u64,
    most_taken: u64,
    budget: u64,
}

impl ReadAhead {
    /// The reads of the chosen pages each of `merges` names of `files`, for
    /// the cursors in the order the merges and each merge's reads list
    /// them, reading ahead within `budget` bytes, at most `concurrency` at
    /// once.
    fn new(
        files: &[Arc<LoadedFile>],
        merges: &[Vec<(usize, Vec<GroupPages>)>],
        budget: u64,
        concurrency: usize,
    ) -> Self {
        let reads: Vec<&(usize, Vec<GroupPages>)> = merges.iter().flatten().collect();
        let mut order: Vec<(usize, usize, usize)> = reads
            .iter()
            .enumerate()
            .flat_map(|(cursor, (_, groups))| {
                groups
                    .windows(2)
                    .enumerate()
                    .map(move |(before, pair)| (pair[0].passed, cursor, before + 1))
            })
            .collect();
        order.sort_unstable();
        let mut ranges = Vec::with_capacity(merges.len());
        for merge in merges {
            let start = ranges.last().map_or(0, |range: &Range<usize>| range.end);
            ranges.push(start..start + merge.len());
        }
        Self {
            files: reads
                .iter()
                .map(|(at, _)| Arc::clone(&files[*at]))
                .collect(),
            merges: ranges,
            merge_of: merges
                .iter()
                .enumerate()
                .flat_map(|(merge, reads)| std::iter::repeat_n(merge, reads.len()))
                .collect(),
            unread: reads
                .iter()
                .map(|(at, groups)| {
                    groups
                        .iter()
                        .map(|group| files[*at].index.pair_read(group.row_group, &group.pages))
                        .collect()
                })
                .collect(),
            issued: vec![0; reads.len()],
            wanted: VecDeque::new(),
            order: order
                .into_iter()
                .map(|(_, cursor, place)| (cursor, place))
                .collect(),
            unfinished: reads
                .iter()
                .filter(|(_, groups)| !groups.is_empty())
                .count(),
            in_flight: FuturesUnordered::new(),
            aborts: merges.iter().map(|_| Vec::new()).collect(),
            concurrency,
            landed: merges.iter().map(|_| Vec::new()).collect(),
            taken: 0,
            most_taken: 0,
            budget,
        }
    }

    /// Notes the row groups `waits` names, each a cursor and the row
    /// group's place among its own, as wanted, but for those already read
    /// or being read.
    fn want(&mut self, waits: impl IntoIterator<Item = (usize, usize)>) {
        for (cursor, place) in waits {
            if self.is_next_unread(cursor, place) {
                self.wanted.push_back((cursor, place));
            }
        }
    }

    /// Notes `dropped` bytes of row groups no merge holds any more.
    fn release(&mut self, dropped: u64) {
        self.taken -= dropped;
    }

    /// Starts the reads `next` gives into `in_flight`, each abortable by
    /// its merge's `end`.
    fn start(&mut self) {
        while let Some((cursor, place, read)) = self.next(self.in_flight.len(), self.concurrency) {
            let file = Arc::clone(&self.files[cursor]);
            let bytes = read.bytes();
            let (abort, registration) = AbortHandle::new_pair();
            self.aborts[self.merge_of[cursor]].push(abort);
            let chunks = Abortable::new(
                async move { file.index.pair_chunks(read).await },
                registration,
            );
            self.in_flight.push(Box::pin(async move {
                Landed {
                    cursor,
                    place,
                    bytes,
                    // intentional: `Aborted` says only that the read's merge ended
                    read: chunks.await.ok(),
                }
            }));
        }
    }

    /// Takes `landed` as landed and starts the reads the slot it frees
    /// lets start, while merges step, each merge standing as `outcomes`
    /// says. A read for a merge that has ended is dropped, its bytes with
    /// it; a read that failed fails its merge, and is that merge with its
    /// error.
    fn land(&mut self, landed: Landed, outcomes: &[Outcome]) -> Option<(usize, Error)> {
        let merge = self.merge_of[landed.cursor];
        let running = outcomes[merge] == Outcome::Running;
        let failed = match landed.read {
            Some(Ok(chunks)) if running => {
                let cursor = landed.cursor - self.merges[merge].start;
                self.landed[merge].push((cursor, landed.place, chunks));
                None
            }
            Some(Err(error)) if running => {
                self.release(landed.bytes);
                Some((merge, error))
            }
            Some(_) | None => {
                self.release(landed.bytes);
                None
            }
        };
        self.start();
        failed
    }

    /// Reads nothing more for `merge`: its reads under way are aborted, its
    /// row groups not yet read are not read, and its reads landed and not
    /// yet taken are dropped.
    fn end(&mut self, merge: usize) {
        for abort in self.aborts[merge].drain(..) {
            abort.abort();
        }
        for cursor in self.merges[merge].clone() {
            if !self.unread[cursor].is_empty() {
                self.unread[cursor].clear();
                self.unfinished -= 1;
            }
        }
        let landed: u64 = self.landed[merge]
            .drain(..)
            .map(|(_, _, chunks)| chunks.read_bytes())
            .sum();
        self.release(landed);
    }

    /// The next row group to read, its cursor, and its place among the
    /// cursor's, with `in_flight` reads of `concurrency` under way: a
    /// wanted one while a slot is free, else the next ahead when it fits
    /// the budget and leaves a slot for each unfinished cursor.
    fn next(&mut self, in_flight: usize, concurrency: usize) -> Option<(usize, usize, PairRead)> {
        if in_flight >= concurrency {
            return None;
        }
        while let Some((cursor, place)) = self.wanted.pop_front() {
            if self.is_next_unread(cursor, place) {
                return Some(self.issue(cursor));
            }
        }
        if in_flight + self.unfinished >= concurrency {
            return None;
        }
        let cursor = self.ahead()?;
        let bytes = self.unread[cursor]
            .front()
            .expect("a row group to read ahead is unread")
            .bytes();
        if self.taken + bytes > self.budget {
            return None;
        }
        self.order.pop_front();
        Some(self.issue(cursor))
    }

    /// Whether the row group at `place` among `cursor`'s is the cursor's
    /// next to read: none of its merge's ended, and it neither read nor
    /// being read.
    fn is_next_unread(&self, cursor: usize, place: usize) -> bool {
        self.issued[cursor] == place && !self.unread[cursor].is_empty()
    }

    /// The cursor of the next row group to read ahead, passing over those
    /// already read or being read as wanted ones, and those of merges that
    /// ended. Each cursor's row groups are in order by place, and its first
    /// is wanted from the start, so the next one ahead is the cursor's next
    /// unread.
    fn ahead(&mut self) -> Option<usize> {
        while let Some(&(cursor, place)) = self.order.front() {
            if self.is_next_unread(cursor, place) {
                return Some(cursor);
            }
            self.order.pop_front();
        }
        None
    }

    /// Takes the next unread row group of `cursor` as read.
    fn issue(&mut self, cursor: usize) -> (usize, usize, PairRead) {
        let read = self.unread[cursor]
            .pop_front()
            .expect("a row group to read is unread");
        if self.unread[cursor].is_empty() {
            self.unfinished -= 1;
        }
        let place = self.issued[cursor];
        self.issued[cursor] += 1;
        self.taken += read.bytes();
        self.most_taken = self.most_taken.max(self.taken);
        (cursor, place, read)
    }

    /// The bytes the merges must drop before the next row group to read
    /// ahead fits the budget, when the budget alone holds it back.
    fn room(&mut self) -> Option<u64> {
        let cursor = self.ahead()?;
        let needed = self.taken + self.unread[cursor].front()?.bytes();
        (needed > self.budget).then(|| needed - self.budget)
    }
}

/// Where a cursor stands after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Head {
    /// On a live pair.
    Live,
    /// Its row group's pages are spent and another row group is to read.
    Spent,
    /// Exhausted.
    Done,
}

/// A cursor over the live pairs of one file's chosen pages, in file order,
/// one row group's pages current and one run of them decoded at a time,
/// passing over deleted rows, with the row groups read ahead of it.
struct PairCursor {
    /// The file's position in the generation's files.
    at: usize,
    /// Whether the load checks the file (`verify_rows`).
    checked: bool,
    file: Arc<LoadedFile>,
    /// Its row groups by place, in file order: each one read and not yet
    /// current.
    ahead: Vec<Option<PairChunks>>,
    /// The place of the row group to make current next.
    next: usize,
    /// Whether it waits on that row group, not yet read.
    waits: bool,
    /// The row group being read, until its pages are spent.
    chunks: Option<PairChunks>,
    /// The next run of `chunks` to decode.
    span: usize,
    pairs: Pairs,
    /// The head's index in `pairs`.
    offset: usize,
}

impl PairCursor {
    /// A cursor of the file `file` at `at` over `row_groups` row groups,
    /// waiting on the first.
    fn new(at: usize, checked: bool, file: Arc<LoadedFile>, row_groups: usize) -> Self {
        Self {
            at,
            checked,
            file,
            ahead: (0..row_groups).map(|_| None).collect(),
            next: 0,
            waits: true,
            chunks: None,
            span: 0,
            pairs: Pairs::empty(),
            offset: 0,
        }
    }

    /// The pair under the cursor against the one under `other`.
    fn cmp_head(&self, other: &Self) -> Ordering {
        self.pairs.cmp_at(self.offset, &other.pairs, other.offset)
    }

    /// Moves onto the first live pair of `chunks`, the next row group's
    /// pages.
    fn resume(&mut self, chunks: PairChunks, deletes: &[Option<Arc<FileDeletes>>]) -> Result<Head> {
        self.next += 1;
        self.chunks = Some(chunks);
        self.span = 0;
        let head = self.fill()?;
        self.past_deleted(head, deletes)
    }

    /// Moves to the next live pair.
    fn advance(&mut self, deletes: &[Option<Arc<FileDeletes>>]) -> Result<Head> {
        let head = self.step()?;
        self.past_deleted(head, deletes)
    }

    /// Steps over deleted rows from the head while it is on a pair.
    fn past_deleted(
        &mut self,
        mut head: Head,
        deletes: &[Option<Arc<FileDeletes>>],
    ) -> Result<Head> {
        while head == Head::Live
            && deletes[self.at]
                .as_deref()
                .deleted(self.pairs.position(self.offset))
        {
            head = self.step()?;
        }
        Ok(head)
    }

    /// Moves to the next pair, deleted or not.
    fn step(&mut self) -> Result<Head> {
        self.offset += 1;
        if self.offset < self.pairs.len() {
            return Ok(Head::Live);
        }
        self.fill()
    }

    /// Decodes the next run of the row group held that holds a pair.
    fn fill(&mut self) -> Result<Head> {
        if let Some(chunks) = &self.chunks {
            while self.span < chunks.len() {
                self.pairs = self.file.index.decode_pairs(chunks, self.span)?;
                self.span += 1;
                self.offset = 0;
                if !self.pairs.is_empty() {
                    return Ok(Head::Live);
                }
            }
        }
        Ok(if self.next == self.ahead.len() {
            Head::Done
        } else {
            Head::Spent
        })
    }
}

/// Cursors in a binary min-heap by head, counting the comparisons of two
/// heads it makes.
struct Heads {
    cursors: Vec<PairCursor>,
    /// Indices into `cursors`, a heap by head.
    heap: Vec<usize>,
    compared: u64,
}

impl Heads {
    /// The head of cursor `a` against that of cursor `b`.
    fn cmp(&mut self, a: usize, b: usize) -> Ordering {
        self.compared += 1;
        self.cursors[a].cmp_head(&self.cursors[b])
    }

    /// The cursor with the least head, left in the heap.
    fn peek(&self) -> Option<usize> {
        self.heap.first().copied()
    }

    /// Puts `cursor` in the heap by its head.
    fn push(&mut self, cursor: usize) {
        self.heap.push(cursor);
        let mut at = self.heap.len() - 1;
        while at > 0 {
            let parent = (at - 1) / 2;
            if self.cmp(self.heap[at], self.heap[parent]) != Ordering::Less {
                break;
            }
            self.heap.swap(at, parent);
            at = parent;
        }
    }

    /// Takes the cursor with the least head out of the heap.
    fn pop(&mut self) -> Option<usize> {
        if self.heap.is_empty() {
            return None;
        }
        let least = self.heap.swap_remove(0);
        let mut at = 0;
        loop {
            let left = 2 * at + 1;
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let mut lesser = left;
            if right < self.heap.len()
                && self.cmp(self.heap[right], self.heap[left]) == Ordering::Less
            {
                lesser = right;
            }
            if self.cmp(self.heap[lesser], self.heap[at]) != Ordering::Less {
                break;
            }
            self.heap.swap(at, lesser);
            at = lesser;
        }
        Some(least)
    }
}

/// Why a step of a merge stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Progress {
    /// A cursor waits on a row group not yet read.
    Waiting,
    /// It dropped the bytes the next row group to read ahead needs.
    Room,
    /// It ended.
    Done,
    /// It was stopped (`MergeState::stop`).
    Stopped,
}

/// One merge between its reads, owned, so that each step can move to the
/// blocking pool: the cursors, how many of them wait on a row group not yet
/// read and which row groups they came to wait on, the checked cursors not
/// yet exhausted, the bytes of chosen pages dropped, and whether it is to
/// stop.
struct MergeState {
    table: String,
    heads: Heads,
    waiting: usize,
    /// The row groups cursors came to wait on since `ReadAhead` last took
    /// them, each a cursor and the row group's place among its own.
    wants: Vec<(usize, usize)>,
    checked: usize,
    deletes: Arc<[Option<Arc<FileDeletes>>]>,
    /// The bytes of row groups dropped since `ReadAhead` last took them.
    dropped: u64,
    /// Set when the merge's outcome can no longer change the load's: a
    /// step under way stops before its next pair.
    stop: Arc<AtomicBool>,
}

impl MergeState {
    /// A merge of `cursors`, every one waiting on its first row group.
    fn new(
        table: &str,
        cursors: Vec<PairCursor>,
        deletes: Arc<[Option<Arc<FileDeletes>>]>,
    ) -> Self {
        let checked = cursors.iter().filter(|cursor| cursor.checked).count();
        Self {
            table: table.to_string(),
            waiting: cursors.len(),
            wants: (0..cursors.len()).map(|cursor| (cursor, 0)).collect(),
            heads: Heads {
                heap: Vec::with_capacity(cursors.len()),
                cursors,
                compared: 0,
            },
            checked,
            deletes,
            dropped: 0,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The bytes of the row groups its cursors hold, current or read ahead.
    fn held(&self) -> u64 {
        self.heads
            .cursors
            .iter()
            .flat_map(|cursor| cursor.chunks.iter().chain(cursor.ahead.iter().flatten()))
            .map(PairChunks::read_bytes)
            .sum()
    }

    /// The bytes it owns with `arrived` handed to it: those of the row
    /// groups its cursors hold, of those it dropped and `ReadAhead` has not
    /// yet taken, and of `arrived`. A step that returns, failed or not,
    /// holds or has dropped every one of them.
    fn owned(&self, arrived: &[Arrival]) -> u64 {
        self.dropped
            + self.held()
            + arrived
                .iter()
                .map(|(_, _, chunks)| chunks.read_bytes())
                .sum::<u64>()
    }

    /// Hands each row group of `arrived` to its cursor, then merges until a
    /// cursor waits, until it has dropped `room` bytes when `room` is set,
    /// or until the merge ends. A row group that fails to decode fails the
    /// step, and those after it, never handed to a cursor, count as
    /// dropped.
    fn resume(&mut self, arrived: Vec<Arrival>, room: Option<u64>) -> Result<Progress> {
        let mut arrived = arrived.into_iter();
        while let Some((cursor, place, chunks)) = arrived.next() {
            if let Err(error) = self.arrive(cursor, place, chunks) {
                self.dropped += arrived
                    .map(|(_, _, chunks)| chunks.read_bytes())
                    .sum::<u64>();
                return Err(error);
            }
        }
        self.merge(room)
    }

    /// Hands `chunks`, row group `place` of `cursor`, to the cursor: moves
    /// the cursor onto it where the cursor waits on it, and holds it ahead
    /// otherwise.
    fn arrive(&mut self, cursor: usize, place: usize, chunks: PairChunks) -> Result<()> {
        let waiting = &mut self.heads.cursors[cursor];
        if waiting.waits && waiting.next == place {
            waiting.waits = false;
            self.waiting -= 1;
            let head = waiting.resume(chunks, &self.deletes)?;
            self.place(cursor, head)
        } else {
            waiting.ahead[place] = Some(chunks);
            Ok(())
        }
    }

    /// Drops the row group `cursor` holds current.
    fn drop_current(&mut self, cursor: usize) {
        if let Some(chunks) = self.heads.cursors[cursor].chunks.take() {
            self.dropped += chunks.read_bytes();
        }
    }

    /// Puts `cursor` back by where it stands: in the heap on a live pair;
    /// once its row group's pages are spent, they are dropped and it moves
    /// onto its next row group when that is read, and waits on it
    /// otherwise.
    fn place(&mut self, cursor: usize, mut head: Head) -> Result<()> {
        loop {
            match head {
                Head::Live => {
                    self.heads.push(cursor);
                    return Ok(());
                }
                Head::Done => {
                    self.drop_current(cursor);
                    if self.heads.cursors[cursor].checked {
                        self.checked -= 1;
                    }
                    return Ok(());
                }
                Head::Spent => {
                    self.drop_current(cursor);
                    let spent = &mut self.heads.cursors[cursor];
                    let Some(chunks) = spent.ahead[spent.next].take() else {
                        spent.waits = true;
                        self.waiting += 1;
                        self.wants.push((cursor, spent.next));
                        return Ok(());
                    };
                    head = spent.resume(chunks, &self.deletes)?;
                }
            }
        }
    }

    /// Merges the live pairs of the cursors by key and order value, and
    /// refuses the first pair two of them hold where one is checked, as
    /// `RefusalReason::DuplicateRows` naming the latest copy in file order
    /// (RFD 025, Uniqueness); a deleted row is not live (RFD 021). Every
    /// file is sorted by key and order value and free of repeated pairs on
    /// its own (verification checked), and each cursor reads a subsequence
    /// of its file's rows, so equal heads are exactly a pair in several
    /// files. Two files neither of which is checked were proven unique by
    /// the generation that last checked them, so a tie between them alone
    /// passes. The merge ends once every checked cursor is exhausted; until
    /// then it stops once a cursor waits on a row group not yet read,
    /// before any pair past it is taken, once it has dropped `room` bytes
    /// when `room` is set, and once it is to stop.
    fn merge(&mut self, room: Option<u64>) -> Result<Progress> {
        let mut tied = Vec::new();
        loop {
            if self.stop.load(atomic::Ordering::Relaxed) {
                return Ok(Progress::Stopped);
            }
            if self.checked == 0 {
                return Ok(Progress::Done);
            }
            if self.waiting > 0 {
                return Ok(Progress::Waiting);
            }
            if room.is_some_and(|room| self.dropped >= room) {
                return Ok(Progress::Room);
            }
            let heads = &mut self.heads;
            let least = heads
                .pop()
                .expect("a checked cursor not exhausted and not waiting is in the heap");
            tied.push(least);
            while let Some(next) = heads.peek() {
                if heads.cmp(next, least) != Ordering::Equal {
                    break;
                }
                tied.extend(heads.pop());
            }
            if tied.len() > 1 && tied.iter().any(|&cursor| heads.cursors[cursor].checked) {
                let later = tied
                    .iter()
                    .copied()
                    .max_by_key(|&cursor| heads.cursors[cursor].at)
                    .expect("a tie holds two cursors");
                let head = &heads.cursors[least];
                return Err(Error::refused(
                    &self.table,
                    RefusalReason::DuplicateRows {
                        path: heads.cursors[later].file.entry.path.clone(),
                        key: head.pairs.key(head.offset),
                        order: Box::new(head.pairs.order(head.offset)),
                    },
                ));
            }
            for cursor in tied.drain(..) {
                let head = self.heads.cursors[cursor].advance(&self.deletes)?;
                self.place(cursor, head)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::testing::{KeyColumn, TABLE, bucketing, refusal_of};
    use super::super::{Span, overlap_groups, routes};
    use super::*;
    use crate::approx;
    use crate::config::{Residency, ServerConfig};
    use crate::index::testing::{self as index_testing, long_pair};
    use crate::key::{Key, KeyType};
    use crate::manifest::{self, DataFileEntry};
    use crate::order::OrderType;
    use crate::snapshot::{SequenceNumber, SnapshotId};
    use crate::storage::IoStats;
    use crate::storage::testing::Gauged;

    /// Whether two closed ranges meet; ends that touch do.
    fn meet<T: Ord + ?Sized>(a: (&T, &T), b: (&T, &T)) -> bool {
        a.0 <= b.1 && b.0 <= a.1
    }

    /// Whether the files `ours` and `theirs` span can share a live pair: one
    /// key can fall in both their buckets (`buckets_may_share_a_key`) and their
    /// key ranges and order ranges both meet, ends included.
    fn is_counterpart(ours: OrderedSpan<'_>, theirs: OrderedSpan<'_>) -> bool {
        buckets_may_share_a_key(ours.span.place(), theirs.span.place())
            && meet(keys(ours), keys(theirs))
            && meet(ours.orders, theirs.orders)
    }

    /// The key range of the file `span` spans.
    fn keys(span: OrderedSpan<'_>) -> (&Key, &Key) {
        (span.span.first, span.span.last)
    }

    /// Bucket counts of a table before and after a spec evolution: one key
    /// falls in buckets congruent modulo two under both.
    const TWO: u32 = 2;
    const FOUR: u32 = 4;

    /// Where a file is filed: its bucketing's count and its bucket, or
    /// unbucketed. Ordered as file order orders bucketings and buckets.
    type Place = Option<(u32, u32)>;

    /// One file of a fixture: its place, its key range, its order range.
    type Described = (Place, (Key, Key), (OrderValue, OrderValue));

    fn span(described: &Described) -> OrderedSpan<'_> {
        let (place, keys, orders) = described;
        OrderedSpan {
            span: Span {
                bucketing: place.map(|(count, _)| bucketing(count)),
                bucket: place.map_or(0, |(_, bucket)| bucket),
                first: &keys.0,
                last: &keys.1,
                added_by: SnapshotId(0),
            },
            orders: (&orders.0, &orders.1),
        }
    }

    fn file(place: Place, keys: (i64, i64), orders: (i64, i64)) -> Described {
        (
            place,
            (Key::Long(keys.0), Key::Long(keys.1)),
            (OrderValue::Long(orders.0), OrderValue::Long(orders.1)),
        )
    }

    /// Every file's counterparts by a scan of every other file: the oracle
    /// the sweep within overlap groups is checked against.
    fn by_full_scan(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        (0..spans.len())
            .map(|at| {
                (0..spans.len())
                    .filter(|&other| other != at && is_counterpart(spans[at], spans[other]))
                    .collect()
            })
            .collect()
    }

    /// The overlap groups of `spans`, which are in file order, as
    /// `assemble` finds them.
    fn groups_of(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        let swept: Vec<Span<'_>> = spans.iter().map(|span| span.span).collect();
        overlap_groups(&swept, &routes(&swept))
    }

    /// What the sweep of every group of `spans` finds with the files
    /// `checked` marks checked, by file position: per file, its
    /// counterparts where one of the two is checked, ascending.
    fn swept(spans: &[OrderedSpan<'_>], checked: &[bool]) -> Vec<Vec<usize>> {
        let mut found = vec![Vec::new(); spans.len()];
        let mut compared = 0;
        for group in groups_of(spans) {
            let met = meetings(spans, checked, &group, &mut compared);
            for (member, &at) in group.iter().enumerate() {
                found[at] = met[member].iter().map(|&theirs| group[theirs]).collect();
                found[at].sort_unstable();
            }
        }
        found
    }

    /// The sweep finds exactly the oracle's counterparts of every file,
    /// less those where neither file is checked: with each file in turn
    /// the one unchecked file, with every file checked, and with every
    /// other file checked.
    fn assert_sweep_is_the_full_scan(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        let oracle = by_full_scan(spans);
        let mut patterns: Vec<Vec<bool>> = (0..spans.len())
            .map(|at| (0..spans.len()).map(|other| other != at).collect())
            .collect();
        patterns.push(vec![true; spans.len()]);
        patterns.push((0..spans.len()).map(|at| at % 2 == 0).collect());
        for checked in &patterns {
            let found = swept(spans, checked);
            for at in 0..spans.len() {
                let expected: Vec<usize> = oracle[at]
                    .iter()
                    .copied()
                    .filter(|&theirs| checked[at] || checked[theirs])
                    .collect();
                assert_eq!(found[at], expected, "the counterparts of {at}");
            }
        }
        oracle
    }

    #[test]
    fn test_meetings_meet_by_key_and_order_range_ends_included_in_buckets_that_share() {
        let files = [
            file(None, (0, 10), (0, 5)),
            file(Some((TWO, 0)), (0, 10), (0, 5)),
            file(Some((TWO, 0)), (0, 10), (6, 9)),
            file(Some((TWO, 0)), (10, 20), (5, 9)),
            file(Some((TWO, 0)), (11, 20), (0, 5)),
            file(Some((TWO, 1)), (0, 10), (0, 5)),
            file(Some((FOUR, 2)), (0, 10), (0, 5)),
            file(Some((FOUR, 3)), (0, 10), (0, 5)),
        ];
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(span).collect();
        let found = assert_sweep_is_the_full_scan(&spans);
        assert_eq!(
            found[1],
            [0, 3, 6],
            "ends that touch meet; an order range or key range apart, or a bucket no key shares, does not"
        );
        assert_eq!(
            found[0],
            [1, 3, 5, 6, 7],
            "an unbucketed file shares every bucket"
        );
    }

    /// Files of a fixture spread over several groups.
    const SPREAD_FILES: u64 = 120;

    /// A multiplier and an increment of a linear congruential sequence,
    /// which spreads the fixture's places and ranges without a seed.
    const MIX: u64 = 6_364_136_223_846_793_005;
    const STEP: u64 = 1_442_695_040_888_963_407;

    /// The span of key and order values the spread fixture's ranges fall in,
    /// and the widest range it draws.
    const SPACE: u64 = 1_000;
    const WIDEST: u64 = 60;

    #[test]
    fn test_meetings_within_overlap_groups_are_those_of_a_full_scan() {
        let mut state = 0_u64;
        let mut draw = |below: u64| {
            state = state.wrapping_mul(MIX).wrapping_add(STEP);
            (state >> 33) % below
        };
        let range = |draw: &mut dyn FnMut(u64) -> u64| {
            let low = draw(SPACE);
            let high = low + draw(WIDEST);
            (i64::try_from(low).unwrap(), i64::try_from(high).unwrap())
        };
        let places: [Place; 7] = [
            None,
            Some((TWO, 0)),
            Some((TWO, 1)),
            Some((FOUR, 0)),
            Some((FOUR, 1)),
            Some((FOUR, 2)),
            Some((FOUR, 3)),
        ];
        let mut files: Vec<Described> = (0..SPREAD_FILES)
            .map(|_| {
                let place = places[usize::try_from(draw(approx::count(places.len()))).unwrap()];
                let keys = range(&mut draw);
                file(place, keys, range(&mut draw))
            })
            .collect();
        files.sort_by(|a, b| (a.0, &a.1.0).cmp(&(b.0, &b.1.0)));
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(span).collect();
        let oracle = assert_sweep_is_the_full_scan(&spans);
        assert!(groups_of(&spans).len() > 1, "several overlap groups");
        let found: usize = oracle.iter().map(Vec::len).sum();
        let keys_meet_only = (0..spans.len())
            .flat_map(|at| (0..spans.len()).map(move |other| (at, other)))
            .filter(|&(at, other)| {
                at != other
                    && meet(keys(spans[at]), keys(spans[other]))
                    && !meet(spans[at].orders, spans[other].orders)
            })
            .count();
        assert!(found > 0, "some files are counterparts");
        assert!(keys_meet_only > 0, "some files meet by key alone");
    }

    /// One ordered file of a test, indexed over an in-memory store of its
    /// own, so the GETs and local reads its stats count are its own.
    struct Built {
        file: Arc<LoadedFile>,
        stats: Arc<IoStats>,
        /// The store the file's GETs pass through, which a test may gate.
        gauged: Arc<Gauged>,
        /// The Disk tier's local directory, which outlives the index.
        _dir: tempfile::TempDir,
    }

    impl Built {
        /// GETs and local reads of the file so far.
        fn reads(&self) -> (u64, u64) {
            (self.stats.gets(), self.stats.local_reads())
        }
    }

    /// The rows of `rows`, (key, order value) pairs ascending, as file
    /// `name` of the first bucket of two, in pages of `page_rows` rows and
    /// one row group, indexed in `residency`.
    async fn built(
        name: &str,
        rows: &[(i64, i64)],
        page_rows: usize,
        residency: Residency,
    ) -> Built {
        built_in_groups(name, rows, page_rows, rows.len(), residency).await
    }

    /// `built`, in row groups of `group_rows` rows.
    async fn built_in_groups(
        name: &str,
        rows: &[(i64, i64)],
        page_rows: usize,
        group_rows: usize,
        residency: Residency,
    ) -> Built {
        let layout = index_testing::ordered_layout(KeyType::Long, OrderType::Long);
        let fixture = index_testing::Fixture::of_long_pairs(rows).in_pages(page_rows, group_rows);
        built_of(name, &fixture, &layout, Some((TWO, 0)), residency).await
    }

    /// `fixture`, an ordered file served under `layout`, as file `name`
    /// filed at `place`, indexed in `residency`, its GETs through a
    /// `Gauged` store, which passes every GET until a test gates them, so a
    /// test can hold them and see how many it runs together.
    async fn built_of(
        name: &str,
        fixture: &index_testing::Fixture,
        layout: &crate::manifest::TableLayout,
        place: Place,
        residency: Residency,
    ) -> Built {
        let (opened, gauged) = index_testing::store_gauged(fixture).await;
        let dir = tempfile::tempdir().unwrap();
        let index = opened
            .build_under_in(layout, residency, dir.path())
            .await
            .unwrap();
        let (first, last) = index.key_range();
        let (first, last) = (first.clone(), last.clone());
        Built {
            file: Arc::new(LoadedFile::new(
                DataFileEntry {
                    path: name.to_string(),
                    bucket: place.map_or(0, |(_, bucket)| bucket),
                    bucketing: place.map(|(count, _)| bucketing(count)),
                    records: index.rows().unsigned_abs(),
                    sequence: SequenceNumber(1),
                    added_by: SnapshotId(1),
                    size: opened.handle.size(),
                    key_min: Some(first),
                    key_max: Some(last),
                    partition: manifest::Partition::unpartitioned(),
                },
                index,
            )),
            stats: opened.stats(),
            gauged,
            _dir: dir,
        }
    }

    /// The check across files of a generation of `built`, which are in
    /// file order, each reused unshrunk where `reused` says so and new
    /// otherwise.
    async fn check_rows(built: &[Built], reused: &[bool]) -> Result<RowCheck> {
        check_rows_within(built, reused, MERGE_READ_AHEAD_BYTES).await
    }

    /// `check_rows` with a merge reading ahead within `read_ahead` bytes.
    async fn check_rows_within(
        built: &[Built],
        reused: &[bool],
        read_ahead: u64,
    ) -> Result<RowCheck> {
        let none = vec![Vec::new(); built.len()];
        let unshrunk = vec![false; built.len()];
        check_rows_deleting(built, reused, &unshrunk, &none, read_ahead)
            .await
            .1
    }

    /// `check_rows` with each file whose deletes shrank where `shrank`
    /// says so, which is checked though reused, and the rows at the
    /// positions of `gone` of each file deleted, and the merge reading
    /// ahead within `read_ahead` bytes; with the overlap groups the check
    /// went through.
    async fn check_rows_deleting(
        built: &[Built],
        reused: &[bool],
        shrank: &[bool],
        gone: &[Vec<u64>],
        read_ahead: u64,
    ) -> (Vec<Vec<usize>>, Result<RowCheck>) {
        let generation = Checked::of(built, reused, shrank, gone);
        let check = verify_rows(
            TABLE,
            &generation.members,
            &generation.files,
            &generation.groups,
            &generation.deletes,
            read_ahead,
        )
        .await;
        (generation.groups, check)
    }

    /// The generation the check across files of a test sees: its files,
    /// their members, each file's deleted rows, and the overlap groups.
    struct Checked {
        files: Vec<Arc<LoadedFile>>,
        members: Vec<Member>,
        deletes: Arc<[Option<Arc<FileDeletes>>]>,
        groups: Vec<Vec<usize>>,
    }

    impl Checked {
        /// The generation of `built`, which are in file order, each reused
        /// where `reused` says so, with deletes that shrank where `shrank`
        /// says so, and the rows at the positions of `gone` deleted.
        fn of(built: &[Built], reused: &[bool], shrank: &[bool], gone: &[Vec<u64>]) -> Self {
            let files: Vec<Arc<LoadedFile>> =
                built.iter().map(|one| Arc::clone(&one.file)).collect();
            let deletes: Arc<[Option<Arc<FileDeletes>>]> = gone
                .iter()
                .map(|rows| {
                    (!rows.is_empty()).then(|| Arc::new(FileDeletes::of_rows(rows.iter().copied())))
                })
                .collect();
            let members: Vec<Member> = files
                .iter()
                .zip(reused.iter().zip(shrank))
                .zip(deletes.iter())
                .map(|((file, (&reused, &shrank)), deletes)| Member {
                    file: Arc::clone(file),
                    reused,
                    deletes: deletes.clone(),
                    shrank,
                })
                .collect();
            let spans: Vec<Span<'_>> = files.iter().map(|file| Span::of(file)).collect();
            let groups = overlap_groups(&spans, &routes(&spans));
            Self {
                files,
                members,
                deletes,
                groups,
            }
        }

        /// The check of every group at once, as `verify_rows` runs it, and
        /// then everything it left under way let end: its steps return and
        /// its reads land, and both are dropped. With the bytes `ReadAhead`
        /// still counts taken after that, which the end of every merge
        /// hands back, so none.
        async fn check_released(&self, read_ahead: u64) -> (Result<RowCheck>, u64) {
            let GroupMerges {
                checked, merges, ..
            } = GroupMerges::of(&self.members, &self.files, &self.groups);
            let mut ahead = ReadAhead::new(&self.files, &merges, read_ahead, load_concurrency());
            let mut run = Merges::new(TABLE, &self.files, &checked, merges, &self.deletes);
            let check = run.drive(&mut ahead).await;
            while let Some((merge, stepped)) = run.steps.next().await {
                run.stepped(merge, stepped, &mut ahead);
            }
            while let Some(one) = ahead.in_flight.next().await {
                run.land(one, &mut ahead);
            }
            (check, ahead.taken)
        }

        /// The outcome of checking the groups one at a time, in group
        /// order: the first group's failure, or the merges of all.
        async fn check_serially(&self, read_ahead: u64) -> Result<usize> {
            let mut merges = 0;
            for group in 0..self.groups.len() {
                let check = verify_rows(
                    TABLE,
                    &self.members,
                    &self.files,
                    &self.groups[group..=group],
                    &self.deletes,
                    read_ahead,
                )
                .await?;
                merges += check.merges;
            }
            Ok(merges)
        }
    }

    /// Files a first load of an appended bucket holds: one per append, each
    /// over the bucket's keys at order values later than the last's.
    const APPENDS: usize = 2_000;

    /// Keys every append writes, one row each.
    const APPENDED_KEYS: i64 = 8;

    /// The most comparisons the check may make per row it holds: the sweep
    /// sorts the files twice and passes each once, so on an appended bucket
    /// it compares far fewer times than there are rows, where a search of
    /// the group per checked file compares about `APPENDS` squared halved.
    const COMPARED_PER_ROW: u64 = 2;

    /// Rows per page where a test does not care how many pages a file has.
    const WHOLE_PAGE_ROWS: usize = 1_000;

    /// A first load of a bucket of `APPENDS` appends, whose order ranges
    /// are disjoint, merges nothing (RFD 025, Refresh): no file is read
    /// again after its build, which read its order chunk once, and the
    /// check's comparisons are linear in its rows.
    #[tokio::test]
    async fn test_first_load_of_an_appended_bucket_reads_each_file_once() {
        let mut files = Vec::with_capacity(APPENDS);
        for append in 0..APPENDS {
            let first = i64::try_from(append).unwrap() * APPENDED_KEYS;
            let rows: Vec<(i64, i64)> = (0..APPENDED_KEYS).map(|key| (key, first + key)).collect();
            files.push(
                built(
                    &format!("a-{append:05}"),
                    &rows,
                    WHOLE_PAGE_ROWS,
                    Residency::Full,
                )
                .await,
            );
        }
        let built_reads: Vec<(u64, u64)> = files.iter().map(Built::reads).collect();
        assert!(
            built_reads.iter().all(|&(gets, _)| gets > 0),
            "each build reads its file"
        );
        let check = check_rows(&files, &[false; APPENDS]).await.unwrap();
        for (one, before) in files.iter().zip(&built_reads) {
            assert_eq!(
                one.reads(),
                *before,
                "{} is not read again",
                one.file.entry.path
            );
        }
        assert_eq!(check.merges, 0);
        let rows = approx::count(APPENDS) * approx::count(usize::try_from(APPENDED_KEYS).unwrap());
        assert!(
            check.compared > 0 && check.compared <= COMPARED_PER_ROW * rows,
            "{} comparisons over {rows} rows",
            check.compared
        );
    }

    /// Files of a first load whose order ranges all overlap.
    const OVERLAPPING: usize = 50;

    /// Keys of each overlapping file, and rows per key.
    const OVERLAPPING_KEYS: i64 = 4;
    const OVERLAPPING_ROWS_PER_KEY: i64 = 8;

    /// Rows per page of the overlapping files: each file's order column has
    /// many pages, every one of which meets every other file's ranges.
    const FEW_PAGE_ROWS: usize = 4;

    /// The reads one row group's chosen pages cost when they are all of
    /// its pages, one run: under `full` the order pages by one GET, the key
    /// chunk being resident; under `bloom` the order and key pages, a GET
    /// each; under `disk` both from the chunk file.
    const FULL_CHECK_GETS: u64 = 1;
    const BLOOM_CHECK_GETS: u64 = 2;
    const DISK_CHECK_LOCAL_READS: u64 = 2;

    /// A first load of `OVERLAPPING` files whose order ranges all overlap
    /// merges them once, reading each file's order pages exactly once
    /// however many files they meet: every page of every file meets
    /// another file's, so each file's pages are chosen as one run, read in
    /// one read per column the tier does not hold.
    #[tokio::test]
    async fn test_first_load_reads_each_page_once_in_an_overlapping_group() {
        for residency in Residency::ALL {
            let mut files = Vec::new();
            let spread = i64::try_from(OVERLAPPING).unwrap();
            for at in 0..OVERLAPPING {
                let offset = i64::try_from(at).unwrap();
                let rows: Vec<(i64, i64)> = (0..OVERLAPPING_KEYS)
                    .flat_map(|key| {
                        (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, row * spread + offset))
                    })
                    .collect();
                files.push(built(&format!("o-{at:03}"), &rows, FEW_PAGE_ROWS, residency).await);
            }
            let before: Vec<(u64, u64)> = files.iter().map(Built::reads).collect();
            let check = check_rows(&files, &[false; OVERLAPPING]).await.unwrap();
            assert_eq!(check.merges, 1, "{residency:?}: one group, one merge");
            for (one, (gets, local)) in files.iter().zip(&before) {
                let (now_gets, now_local) = one.reads();
                let read = (now_gets - gets, now_local - local);
                let expected = match residency {
                    Residency::Full => (FULL_CHECK_GETS, 0),
                    Residency::Bloom => (BLOOM_CHECK_GETS, 0),
                    Residency::Disk => (0, DISK_CHECK_LOCAL_READS),
                };
                assert_eq!(read, expected, "{residency:?}: {}", one.file.entry.path);
            }
        }
    }

    /// The pair two unchanged files of the tie tests share, and the order
    /// values a new file holds around it.
    const SHARED: (i64, i64) = (0, 10);
    const BEFORE_SHARED: i64 = 5;
    const AFTER_SHARED: i64 = 15;

    /// Two reused files whose deletes did not shrink were proven unique by
    /// the previous generation, so a tie between them alone in a merge is no
    /// refusal (RFD 025, Refresh): both are in the merge, since the new
    /// file's ranges meet their pages, and the load passes. When the new
    /// file holds the pair too, the tie holds a checked file and is
    /// refused, naming the latest copy in file order.
    #[tokio::test]
    async fn test_a_tie_between_two_unchanged_files_is_not_refused() {
        let (key, order) = SHARED;
        let unchanged = [(key, order)];
        let around = [(key, BEFORE_SHARED), (key, AFTER_SHARED)];
        let files = [
            built("t-0", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-1", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-2", &around, WHOLE_PAGE_ROWS, Residency::Full).await,
        ];
        let before: Vec<(u64, u64)> = files.iter().map(Built::reads).collect();
        let check = check_rows(&files, &[true, true, false]).await.unwrap();
        assert_eq!(check.merges, 1);
        for (one, (gets, _)) in files.iter().zip(&before) {
            assert!(
                one.reads().0 > *gets,
                "{} is in the merge",
                one.file.entry.path
            );
        }

        let repeating = [(key, BEFORE_SHARED), (key, order), (key, AFTER_SHARED)];
        let files = [
            built("t-0", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-1", &repeating, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-2", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
        ];
        let RefusalReason::DuplicateRows {
            path,
            key: refused_key,
            order: refused_order,
        } = refusal_of(check_rows(&files, &[true, false, true]).await)
        else {
            panic!("expected duplicate_rows");
        };
        assert_eq!(path, "t-2", "the latest of the three copies");
        assert_eq!(refused_key, Key::Long(key));
        assert_eq!(*refused_order, OrderValue::Long(order));
    }

    /// The bytes a merge reads of order page `page` of `row_group` of
    /// `file` in `residency`: the page, and under `bloom` and `disk` the key
    /// page holding the same rows, the fixture writing both columns in pages
    /// of the same rows.
    fn page_bytes(file: &LoadedFile, row_group: usize, page: usize, residency: Residency) -> u64 {
        let metadata = file.index.metadata();
        let offsets = &metadata.offset_index().unwrap()[row_group];
        let order = &offsets[file.index.order_leaf().unwrap()].page_locations()[page];
        let key = &offsets[file.index.key_leaf()].page_locations()[page];
        assert_eq!(key.first_row_index, order.first_row_index, "aligned pages");
        let size = |location: &parquet::file::page_index::offset_index::PageLocation| {
            u64::try_from(location.compressed_page_size).unwrap()
        };
        match residency {
            Residency::Full => size(order),
            Residency::Bloom | Residency::Disk => size(order) + size(key),
        }
    }

    /// The order pages of `row_group` of `file`.
    fn order_pages(file: &LoadedFile, row_group: usize) -> usize {
        file.index.metadata().offset_index().unwrap()[row_group][file.index.order_leaf().unwrap()]
            .page_locations()
            .len()
    }

    /// Bytes `built` has read so far, by GET and from its chunk file.
    fn bytes_read(built: &Built) -> u64 {
        built.stats.bytes() + built.stats.local_bytes()
    }

    /// Rows of the run that fills most of each edge-key file, over many
    /// pages, and rows of the key the two files share on each side of the
    /// edge, fewer than a page holds.
    const EDGE_RUN: i64 = 400;
    const EDGE_ROWS: i64 = 4;

    /// The keys of the edge-key files: the first file's run, the key the
    /// two share, and the second file's run.
    const BEFORE_EDGE: i64 = 0;
    const EDGE_KEY: i64 = 1;
    const AFTER_EDGE: i64 = 2;

    /// Two new files that meet at one edge key, as a compaction rolling
    /// files by size writes them, read only the order pages that key's
    /// rows lie on for their check (RFD 025, Refresh): the first file's
    /// last page and the second's first, by GET under `full`, with the key
    /// pages they span under `bloom`, and from the chunk file under `disk`.
    #[tokio::test]
    async fn test_verify_rows_of_files_meeting_at_an_edge_key_reads_only_the_edge_pages() {
        for residency in Residency::ALL {
            let head: Vec<(i64, i64)> = (0..EDGE_RUN)
                .map(|order| (BEFORE_EDGE, order))
                .chain((0..EDGE_ROWS).map(|order| (EDGE_KEY, order)))
                .collect();
            let tail: Vec<(i64, i64)> = (EDGE_ROWS..2 * EDGE_ROWS)
                .map(|order| (EDGE_KEY, order))
                .chain((0..EDGE_RUN).map(|order| (AFTER_EDGE, order)))
                .collect();
            let files = [
                built("e-0", &head, FEW_PAGE_ROWS, residency).await,
                built("e-1", &tail, FEW_PAGE_ROWS, residency).await,
            ];
            let before: Vec<u64> = files.iter().map(bytes_read).collect();
            let check = check_rows(&files, &[false, false]).await.unwrap();
            assert_eq!(check.merges, 1, "{residency:?}");
            let last = order_pages(&files[0].file, 0) - 1;
            let whole = |file: &LoadedFile| -> u64 {
                (0..order_pages(file, 0))
                    .map(|page| page_bytes(file, 0, page, residency))
                    .sum()
            };
            for ((one, before), page) in files.iter().zip(&before).zip([last, 0]) {
                let read = bytes_read(one) - before;
                assert_eq!(
                    read,
                    page_bytes(&one.file, 0, page, residency),
                    "{residency:?}: {}",
                    one.file.entry.path
                );
                assert!(read < whole(&one.file), "{residency:?}: not every page");
            }
        }
    }

    /// Row groups of each file of the read-ahead tests, one key each.
    const GROUPS_PER_FILE: usize = 4;

    /// Files of the read-ahead test that holds every GET: few enough that
    /// every row group's reads of every file are in flight at once within
    /// a load's concurrency.
    const AHEAD_FILES: usize = 8;

    /// A read-ahead budget below any row group's chosen pages: the merge
    /// reads no row group ahead of need.
    const BELOW_ANY_ROW_GROUP: u64 = 1;

    /// `files` new files of `GROUPS_PER_FILE` row groups each, in
    /// `residency`, whose order ranges all overlap, so every page of each
    /// meets every other file's and the merge reads every page, each file
    /// over a `Gauged` store.
    async fn overlapping_in_groups(files: usize, residency: Residency) -> Vec<Built> {
        overlapping_in_groups_from("h", 0, files, residency).await
    }

    /// `overlapping_in_groups`, named after `prefix`, its keys from
    /// `first_key` on.
    async fn overlapping_in_groups_from(
        prefix: &str,
        first_key: i64,
        files: usize,
        residency: Residency,
    ) -> Vec<Built> {
        let group_rows = usize::try_from(OVERLAPPING_ROWS_PER_KEY).unwrap();
        let spread = i64::try_from(files).unwrap();
        let mut built = Vec::with_capacity(files);
        for at in 0..files {
            let offset = i64::try_from(at).unwrap();
            let rows: Vec<(i64, i64)> = (0..i64::try_from(GROUPS_PER_FILE).unwrap())
                .flat_map(|key| {
                    (0..OVERLAPPING_ROWS_PER_KEY)
                        .map(move |row| (first_key + key, row * spread + offset))
                })
                .collect();
            let name = format!("{prefix}-{at:03}");
            built.push(built_in_groups(&name, &rows, FEW_PAGE_ROWS, group_rows, residency).await);
        }
        for one in &built {
            assert_eq!(one.file.index.metadata().num_row_groups(), GROUPS_PER_FILE);
        }
        built
    }

    /// The bytes of chosen pages a merge of files whose every page is
    /// chosen reads in `residency`: the sum over the files of each one's
    /// first row group, of each one's largest, and of all its row groups.
    #[derive(Debug)]
    struct GroupBytes {
        first: u64,
        largest: u64,
        whole: u64,
    }

    fn group_bytes(files: &[Built], residency: Residency) -> GroupBytes {
        let mut bytes = GroupBytes {
            first: 0,
            largest: 0,
            whole: 0,
        };
        for one in files {
            let per_group: Vec<u64> = (0..GROUPS_PER_FILE)
                .map(|row_group| {
                    (0..order_pages(&one.file, row_group))
                        .map(|page| page_bytes(&one.file, row_group, page, residency))
                        .sum()
                })
                .collect();
            bytes.first += per_group[0];
            bytes.largest += per_group.iter().max().unwrap();
            bytes.whole += per_group.iter().sum::<u64>();
        }
        bytes
    }

    /// What `built` has read so far: GETs, their bytes, local reads, and
    /// theirs.
    fn io(built: &Built) -> [u64; 4] {
        let stats = &built.stats;
        [
            stats.gets(),
            stats.bytes(),
            stats.local_reads(),
            stats.local_bytes(),
        ]
    }

    /// What each of `files` read between `before` and now.
    fn io_since(files: &[Built], before: &[[u64; 4]]) -> Vec<[u64; 4]> {
        files
            .iter()
            .zip(before)
            .map(|(one, before)| {
                let now = io(one);
                std::array::from_fn(|at| now[at] - before[at])
            })
            .collect()
    }

    /// Waits until `gauged` has held `gets` GETs in flight at once,
    /// failing past the get deadline.
    async fn reach(gauged: &Gauged, gets: usize) {
        tokio::time::timeout(ServerConfig::default().get_deadline, async {
            while gauged.high_water() < gets {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_elapsed| panic!("{gets} GETs never reached the store at once"));
    }

    /// A merge whose budget holds every row group of its files issues the
    /// reads of every row group of every file in one round, before any
    /// cursor runs dry: with the store holding every GET, no cursor holds a
    /// pair, and every file's GETs for all its row groups are in flight at
    /// once. It makes the GETs, for the bytes, that a merge reading no row
    /// group ahead makes, one row group's at a time, and holds every row
    /// group's chosen pages, within the budget.
    #[tokio::test]
    async fn test_verify_rows_reads_later_row_groups_before_a_cursor_runs_dry() {
        for residency in [Residency::Full, Residency::Bloom] {
            let files = overlapping_in_groups(AHEAD_FILES, residency).await;
            let reused = [false; AHEAD_FILES];
            for one in &files {
                one.gauged.fill_to(0);
            }
            let before: Vec<[u64; 4]> = files.iter().map(io).collect();
            check_rows_within(&files, &reused, BELOW_ANY_ROW_GROUP)
                .await
                .unwrap();
            let unread_ahead = io_since(&files, &before);
            let per_group: Vec<usize> = files.iter().map(|one| one.gauged.high_water()).collect();
            for one in &files {
                one.gauged.hold();
            }
            let before: Vec<[u64; 4]> = files.iter().map(io).collect();
            let every_group_in_flight = async {
                for (one, &per_group) in files.iter().zip(&per_group) {
                    reach(&one.gauged, per_group * GROUPS_PER_FILE).await;
                }
                for one in &files {
                    one.gauged.release();
                }
            };
            let (check, ()) = futures::join!(
                check_rows_within(&files, &reused, MERGE_READ_AHEAD_BYTES),
                every_group_in_flight
            );
            let check = check.unwrap();
            let read_ahead = io_since(&files, &before);
            for (((one, ahead), unread), &per_group) in files
                .iter()
                .zip(&read_ahead)
                .zip(&unread_ahead)
                .zip(&per_group)
            {
                let path = &one.file.entry.path;
                assert!(unread[0] > 0 && per_group > 0, "{residency:?}: {path}");
                assert_eq!(ahead, unread, "{residency:?}: {path}");
                assert_eq!(
                    one.gauged.high_water(),
                    per_group * GROUPS_PER_FILE,
                    "{residency:?}: {path}"
                );
            }
            let bytes = group_bytes(&files, residency);
            assert_eq!(check.held, bytes.whole, "{residency:?}");
            assert!(check.held <= MERGE_READ_AHEAD_BYTES, "{residency:?}");
        }
    }

    /// A merge reads ahead first the row group it reaches first, whatever
    /// file order says: of two files over the same keys, the second in
    /// file order holds one key per row group and the first two, so the
    /// merge runs dry of the second's first row group before the first's.
    /// Under a budget of both first row groups and the second file's next,
    /// with the store holding every GET, the second file's next row group
    /// is read ahead and the first file's is not.
    #[tokio::test]
    async fn test_verify_rows_reads_ahead_first_the_row_group_the_merge_reaches_first() {
        let residency = Residency::Full;
        let keys = i64::try_from(GROUPS_PER_FILE).unwrap();
        let rows = |offset: i64| -> Vec<(i64, i64)> {
            (0..keys)
                .flat_map(|key| {
                    (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, 2 * row + offset))
                })
                .collect()
        };
        let key_rows = usize::try_from(OVERLAPPING_ROWS_PER_KEY).unwrap();
        let keys_per_group_of_the_first = 2;
        let files = [
            built_in_groups(
                "r-0",
                &rows(1),
                FEW_PAGE_ROWS,
                keys_per_group_of_the_first * key_rows,
                residency,
            )
            .await,
            built_in_groups("r-1", &rows(0), FEW_PAGE_ROWS, key_rows, residency).await,
        ];
        let group = |one: &Built, row_group: usize| -> u64 {
            (0..order_pages(&one.file, row_group))
                .map(|page| page_bytes(&one.file, row_group, page, residency))
                .sum()
        };
        let budget = group(&files[0], 0) + group(&files[1], 0) + group(&files[1], 1);
        for one in &files {
            one.gauged.hold();
        }
        let first_ahead = async {
            reach(&files[1].gauged, 2).await;
            let first_file_in_flight = files[0].gauged.high_water();
            for one in &files {
                one.gauged.release();
            }
            first_file_in_flight
        };
        let (check, first_file_in_flight) = futures::join!(
            check_rows_within(&files, &[false, false], budget),
            first_ahead
        );
        check.unwrap();
        assert_eq!(
            first_file_in_flight, 1,
            "the first file's next row group waits for the budget"
        );
    }

    /// Slots of a read-ahead test: enough that slots never hold a read
    /// back.
    const ROOMY_SLOTS: usize = 64;

    /// Every order page of `row_group` of `file`, as chosen, the sweep
    /// passing the last at step `passed`.
    fn every_page(file: &LoadedFile, row_group: usize, passed: usize) -> GroupPages {
        GroupPages {
            row_group,
            pages: (0..order_pages(file, row_group)).collect(),
            passed,
        }
    }

    /// The reads `ahead` lets start, one after another, with none under
    /// way before them, of `slots` slots.
    fn next_reads(ahead: &mut ReadAhead, slots: usize) -> Vec<(usize, usize, PairRead)> {
        let mut reads = Vec::new();
        while let Some(read) = ahead.next(reads.len(), slots) {
            reads.push(read);
        }
        reads
    }

    /// The cursor and place of each of `reads`.
    fn places(reads: &[(usize, usize, PairRead)]) -> Vec<(usize, usize)> {
        reads
            .iter()
            .map(|&(cursor, place, _)| (cursor, place))
            .collect()
    }

    /// Under a budget that holds the first row groups and no more, a merge
    /// that drops a file's row group stops for the read ahead the bytes it
    /// dropped let in, while the cursor that read is for still holds pairs
    /// of its current row group: the first file ends inside the second
    /// file's first row group, whose next one the third file meets.
    #[tokio::test]
    async fn test_merge_stops_to_read_ahead_once_it_drops_what_the_next_read_needs() {
        let residency = Residency::Full;
        let (first_rows, second_rows, last_key_rows) = (16, 24, 4);
        let last_key = 2;
        let first: Vec<(i64, i64)> = (0..first_rows).map(|row| (0, 2 * row)).collect();
        let second: Vec<(i64, i64)> = (0..second_rows)
            .map(|row| (0, 2 * row + 1))
            .chain((0..last_key_rows).map(|row| (last_key, 2 * row + 1)))
            .collect();
        let third: Vec<(i64, i64)> = (0..last_key_rows).map(|row| (last_key, 2 * row)).collect();
        let group_rows = usize::try_from(second_rows).unwrap();
        let built = [
            built_in_groups("s-0", &first, FEW_PAGE_ROWS, group_rows, residency).await,
            built_in_groups("s-1", &second, FEW_PAGE_ROWS, group_rows, residency).await,
            built_in_groups("s-2", &third, FEW_PAGE_ROWS, group_rows, residency).await,
        ];
        let files: Vec<Arc<LoadedFile>> = built.iter().map(|one| Arc::clone(&one.file)).collect();
        let reads = vec![
            (0, vec![every_page(&files[0], 0, 0)]),
            (
                1,
                vec![every_page(&files[1], 0, 0), every_page(&files[1], 1, 0)],
            ),
            (2, vec![every_page(&files[2], 0, 0)]),
        ];
        let bytes = |at: usize, row_group: usize| {
            let group = &reads[at].1[row_group];
            files[at]
                .index
                .pair_read(group.row_group, &group.pages)
                .bytes()
        };
        assert!(
            bytes(0, 0) >= bytes(1, 1),
            "the first file's drop makes room"
        );
        let budget = bytes(0, 0) + bytes(1, 0) + bytes(2, 0);
        let mut ahead = ReadAhead::new(&files, std::slice::from_ref(&reads), budget, ROOMY_SLOTS);
        let cursors = reads
            .iter()
            .map(|(at, groups)| PairCursor::new(*at, true, Arc::clone(&files[*at]), groups.len()))
            .collect();
        let deletes: Arc<[Option<Arc<FileDeletes>>]> = vec![None; files.len()].into();
        let mut merge = MergeState::new(TABLE, cursors, deletes);
        ahead.want(merge.wants.drain(..));
        let firsts = next_reads(&mut ahead, ROOMY_SLOTS);
        assert_eq!(
            places(&firsts),
            [(0, 0), (1, 0), (2, 0)],
            "nothing ahead fits"
        );
        let mut arrived = Vec::new();
        for (cursor, place, read) in firsts {
            arrived.push((
                cursor,
                place,
                files[cursor].index.pair_chunks(read).await.unwrap(),
            ));
        }
        let room = ahead.room();
        let progress = merge.resume(arrived, room).unwrap();
        assert_eq!(progress, Progress::Room);
        assert!(
            merge.waiting == 0 && !merge.heads.cursors[1].waits,
            "the second file still holds pairs of its first row group"
        );
        ahead.release(std::mem::take(&mut merge.dropped));
        assert_eq!(places(&next_reads(&mut ahead, ROOMY_SLOTS)), [(1, 1)]);
    }

    /// A read that lands while the merge steps frees a slot, and the next
    /// read ahead starts in it at once: of three slots, one cursor's wanted
    /// read and one read ahead start, a slot left for the cursor, and the
    /// first read to land lets its last row group start.
    #[tokio::test]
    async fn test_read_ahead_starts_the_next_read_in_the_slot_a_landed_read_frees() {
        let residency = Residency::Full;
        let groups = 3;
        let rows: Vec<(i64, i64)> = (0..i64::try_from(groups).unwrap())
            .flat_map(|key| (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, row)))
            .collect();
        let group_rows = usize::try_from(OVERLAPPING_ROWS_PER_KEY).unwrap();
        let built = built_in_groups("l-0", &rows, FEW_PAGE_ROWS, group_rows, residency).await;
        let files = vec![Arc::clone(&built.file)];
        let reads = vec![(
            0,
            (0..groups)
                .map(|row_group| every_page(&files[0], row_group, row_group))
                .collect(),
        )];
        let three_slots = 3;
        let mut ahead = ReadAhead::new(
            &files,
            std::slice::from_ref(&reads),
            MERGE_READ_AHEAD_BYTES,
            three_slots,
        );
        ahead.want([(0, 0)]);
        ahead.start();
        assert_eq!(ahead.in_flight.len(), 2, "the wanted read and one ahead");
        let one = ahead.in_flight.next().await.unwrap();
        assert!(
            ahead.land(one, &[Outcome::Running]).is_none(),
            "the read succeeds"
        );
        assert_eq!(ahead.in_flight.len(), 2, "the freed slot starts a read");
        assert_eq!(
            ahead.issued[0], groups,
            "every row group read or being read"
        );
    }

    /// Reads ahead leave a slot for every cursor with a row group to read,
    /// so a row group a cursor comes to want starts at once however the
    /// merge's needs depart from the order of the reads ahead: here the
    /// reads ahead favour the first file, and the merge needs the second
    /// file's row groups first, with three slots for two cursors.
    #[tokio::test]
    async fn test_read_ahead_leaves_a_slot_for_every_row_group_a_cursor_comes_to_want() {
        let residency = Residency::Full;
        let (first_groups, second_groups) = (6, 3);
        let group_rows = usize::try_from(OVERLAPPING_ROWS_PER_KEY).unwrap();
        let mut files = Vec::new();
        let mut built = Vec::new();
        for (at, groups) in [first_groups, second_groups].into_iter().enumerate() {
            let rows: Vec<(i64, i64)> = (0..i64::try_from(groups).unwrap())
                .flat_map(|key| (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, row)))
                .collect();
            let one = built_in_groups(
                &format!("w-{at}"),
                &rows,
                FEW_PAGE_ROWS,
                group_rows,
                residency,
            )
            .await;
            files.push(Arc::clone(&one.file));
            built.push(one);
        }
        let later = first_groups;
        let reads: Vec<(usize, Vec<GroupPages>)> = [(0, 0), (1, later)]
            .into_iter()
            .map(|(at, from)| {
                let groups = files[at].index.metadata().num_row_groups();
                (
                    at,
                    (0..groups)
                        .map(|row_group| every_page(&files[at], row_group, from + row_group))
                        .collect(),
                )
            })
            .collect();
        let needs: Vec<(usize, usize)> = [(0, 0), (1, 0)]
            .into_iter()
            .chain((1..second_groups).map(|place| (1, place)))
            .chain((1..first_groups).map(|place| (0, place)))
            .collect();
        let slots = 3;
        let mut ahead = ReadAhead::new(
            &files,
            std::slice::from_ref(&reads),
            MERGE_READ_AHEAD_BYTES,
            slots,
        );
        let mut in_flight: VecDeque<(usize, usize)> = VecDeque::new();
        let start = |ahead: &mut ReadAhead, in_flight: &mut VecDeque<(usize, usize)>| {
            while let Some((cursor, place, _)) = ahead.next(in_flight.len(), slots) {
                in_flight.push_back((cursor, place));
            }
        };
        ahead.want([(0, 0), (1, 0)]);
        start(&mut ahead, &mut in_flight);
        let mut landed = BTreeSet::new();
        let mut wanted_later = 0;
        for need in needs {
            if !landed.contains(&need) && !in_flight.contains(&need) {
                ahead.want([need]);
                start(&mut ahead, &mut in_flight);
                assert!(in_flight.contains(&need), "{need:?} starts once wanted");
                wanted_later += 1;
            }
            while !landed.contains(&need) {
                let one = in_flight
                    .pop_front()
                    .expect("a needed row group is being read");
                landed.insert(one);
                start(&mut ahead, &mut in_flight);
            }
        }
        assert!(wanted_later > 0, "some needs outrun the reads ahead");
        assert_eq!(landed.len(), first_groups + second_groups);
    }

    /// Under a budget below any row group, a first load of `OVERLAPPING`
    /// files of `GROUPS_PER_FILE` row groups each, every page of which
    /// meets every other file's, reads one row group of each file at a
    /// time and completes: it reads every page once, runs one row group's
    /// GETs of a file at a time, and holds at least every file's first row
    /// group, which the merge starts from, and at most each file's largest.
    #[tokio::test]
    async fn test_verify_rows_under_a_budget_below_a_row_group_reads_one_row_group_per_file() {
        for residency in Residency::ALL {
            let files = overlapping_in_groups(OVERLAPPING, residency).await;
            for one in &files {
                one.gauged.fill_to(0);
            }
            let before: Vec<[u64; 4]> = files.iter().map(io).collect();
            let check = check_rows_within(&files, &[false; OVERLAPPING], BELOW_ANY_ROW_GROUP)
                .await
                .unwrap();
            let read = io_since(&files, &before);
            let (per_group_gets, per_group_local) = match residency {
                Residency::Full => (FULL_CHECK_GETS, 0),
                Residency::Bloom => (BLOOM_CHECK_GETS, 0),
                Residency::Disk => (0, DISK_CHECK_LOCAL_READS),
            };
            let groups = approx::count(GROUPS_PER_FILE);
            for (one, read) in files.iter().zip(&read) {
                let path = &one.file.entry.path;
                assert_eq!(
                    (read[0], read[2]),
                    (per_group_gets * groups, per_group_local * groups),
                    "{residency:?}: {path}"
                );
                assert!(
                    approx::count(one.gauged.high_water()) <= per_group_gets,
                    "{residency:?}: {path} runs one row group's GETs at a time"
                );
            }
            let bytes = group_bytes(&files, residency);
            assert_eq!(check.merges, 1, "{residency:?}");
            assert_eq!(
                read.iter().map(|read| read[1] + read[3]).sum::<u64>(),
                bytes.whole,
                "{residency:?}: every page meets another file's"
            );
            assert!(
                bytes.first <= check.held && check.held <= bytes.largest,
                "{residency:?}: held {} outside {bytes:?}",
                check.held
            );
            assert!(check.held < bytes.whole, "{residency:?}");
        }
    }

    /// Under a budget of half the chosen pages, a first load of
    /// `OVERLAPPING` files of `GROUPS_PER_FILE` row groups each reads row
    /// groups ahead, holding more than one row group of each file at once,
    /// and holds no more than the budget beyond one row group of each
    /// file; it reads every page once.
    #[tokio::test]
    async fn test_verify_rows_holds_within_the_budget_beyond_one_row_group_per_file() {
        for residency in Residency::ALL {
            let files = overlapping_in_groups(OVERLAPPING, residency).await;
            let bytes = group_bytes(&files, residency);
            let budget = bytes.whole / 2;
            let before: Vec<[u64; 4]> = files.iter().map(io).collect();
            let check = check_rows_within(&files, &[false; OVERLAPPING], budget)
                .await
                .unwrap();
            let read = io_since(&files, &before);
            assert_eq!(
                read.iter().map(|read| read[1] + read[3]).sum::<u64>(),
                bytes.whole,
                "{residency:?}: every page once"
            );
            assert!(
                bytes.largest < check.held && check.held <= budget + bytes.largest,
                "{residency:?}: held {} under a budget of {budget}, {bytes:?}",
                check.held
            );
        }
    }

    /// Overlap groups of the tests of merges run together, and the keys
    /// between the first keys of two of them: more than a group's files
    /// hold, so no two groups' key ranges meet.
    const MERGED_GROUPS: usize = 3;
    const GROUP_KEY_GAP: i64 = 100;

    /// Under one budget of half the chosen pages of `MERGED_GROUPS`
    /// overlap groups, each of `AHEAD_FILES` files of `GROUPS_PER_FILE` row
    /// groups whose every page is chosen, the groups' merges hold more at
    /// once than any one group's chosen pages, so they merge together, and
    /// no more than the budget beyond one row group of each file of every
    /// group: the budget is the load's, not each merge's. Every page is
    /// read once, and every merge hands back the bytes it took.
    #[tokio::test]
    async fn test_verify_rows_holds_every_group_within_one_budget() {
        for residency in Residency::ALL {
            let mut files = Vec::new();
            for group in 0..MERGED_GROUPS {
                let first_key = i64::try_from(group).unwrap() * GROUP_KEY_GAP;
                let name = format!("m{group}");
                files.extend(
                    overlapping_in_groups_from(&name, first_key, AHEAD_FILES, residency).await,
                );
            }
            let bytes = group_bytes(&files, residency);
            let one_group = group_bytes(&files[..AHEAD_FILES], residency);
            let budget = bytes.whole / 2;
            let before: Vec<[u64; 4]> = files.iter().map(io).collect();
            let reused = vec![false; files.len()];
            let none = vec![Vec::new(); files.len()];
            let generation = Checked::of(&files, &reused, &reused, &none);
            let (check, taken) = generation.check_released(budget).await;
            let check = check.unwrap();
            assert_eq!(taken, 0, "{residency:?}: every merge hands back its bytes");
            assert_eq!(generation.groups.len(), MERGED_GROUPS, "{residency:?}");
            assert_eq!(check.merges, MERGED_GROUPS, "{residency:?}");
            let read = io_since(&files, &before);
            assert_eq!(
                read.iter().map(|read| read[1] + read[3]).sum::<u64>(),
                bytes.whole,
                "{residency:?}: every page once"
            );
            assert!(
                one_group.whole < check.held && check.held <= budget + bytes.largest,
                "{residency:?}: held {} under a budget of {budget}, {bytes:?}, one group {one_group:?}",
                check.held
            );
        }
    }

    /// Two new files of `key` that share the pair (`key`, `SHARED.1`),
    /// named after `prefix`: the merge of their group refuses it, naming
    /// the second.
    async fn sharing_a_pair(prefix: &str, key: i64) -> [Built; 2] {
        let order = SHARED.1;
        let around = [(key, BEFORE_SHARED), (key, order), (key, AFTER_SHARED)];
        [
            built(
                &format!("{prefix}-0"),
                &around,
                WHOLE_PAGE_ROWS,
                Residency::Full,
            )
            .await,
            built(
                &format!("{prefix}-1"),
                &[(key, order)],
                WHOLE_PAGE_ROWS,
                Residency::Full,
            )
            .await,
        ]
    }

    /// Waits until `condition` holds, failing past the get deadline with
    /// `what`.
    async fn until(condition: impl Fn() -> bool, what: &str) {
        tokio::time::timeout(ServerConfig::default().get_deadline, async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_elapsed| panic!("{what}"));
    }

    /// Merged together, overlap groups refuse as merged one at a time: the
    /// first group in group order that refuses, though a later one refuses
    /// first, and a group after one that refuses stops at once. Each of
    /// three groups shares a pair, and every GET is held. Once the third
    /// group's GETs are under way at the gate, the second group's pass, so
    /// a third group stopped before it reads anything never stands in for
    /// one stopped mid-read. The first group's GETs are held until the
    /// second has refused and the third, whose GETs are held throughout,
    /// has stopped: its GETs are dropped. The refusal names the first
    /// group's pair and file.
    #[tokio::test]
    async fn test_verify_rows_refuses_for_the_first_group_that_refuses_whichever_refuses_first() {
        let keys: Vec<i64> = (0..i64::try_from(MERGED_GROUPS).unwrap())
            .map(|group| group * GROUP_KEY_GAP)
            .collect();
        let mut files = Vec::new();
        for (prefix, &key) in ["f", "g", "h"].into_iter().zip(&keys) {
            files.extend(sharing_a_pair(prefix, key).await);
        }
        let (first, second, third) = (&files[..2], &files[2..4], &files[4..]);
        for one in &files {
            one.gauged.hold();
        }
        let refused_later_first = async {
            until(
                || third.iter().all(|one| one.gauged.high_water() > 0),
                "the third group's GETs never reached the store",
            )
            .await;
            for one in second {
                one.gauged.release();
            }
            until(
                || third.iter().all(|one| one.gauged.in_flight() == 0),
                "the third group's GETs went on after the second group refused",
            )
            .await;
            for one in first {
                one.gauged.release();
            }
        };
        let reused = vec![false; files.len()];
        let none = vec![Vec::new(); files.len()];
        let ((groups, check), ()) = futures::join!(
            check_rows_deleting(&files, &reused, &reused, &none, MERGE_READ_AHEAD_BYTES),
            refused_later_first
        );
        assert_eq!(groups.len(), MERGED_GROUPS);
        let RefusalReason::DuplicateRows { path, key, order } = refusal_of(check) else {
            panic!("expected duplicate_rows");
        };
        assert_eq!(
            (path.as_str(), key, *order),
            ("f-1", Key::Long(keys[0]), OrderValue::Long(SHARED.1)),
            "the first group's refusal"
        );
    }

    /// A merge told to stop stops before its next pair, and holds the row
    /// groups that arrived: of two files sharing a pair, it never reaches
    /// the pair.
    #[tokio::test]
    async fn test_merge_stops_before_its_next_pair_once_told_to() {
        let built = sharing_a_pair("s", 0).await;
        let files: Vec<Arc<LoadedFile>> = built.iter().map(|one| Arc::clone(&one.file)).collect();
        let mut cursors = Vec::new();
        let mut arrived = Vec::new();
        let mut bytes = 0;
        for (at, file) in files.iter().enumerate() {
            let group = every_page(file, 0, 0);
            let read = file.index.pair_read(group.row_group, &group.pages);
            bytes += read.bytes();
            cursors.push(PairCursor::new(at, true, Arc::clone(file), 1));
            arrived.push((at, 0, file.index.pair_chunks(read).await.unwrap()));
        }
        let deletes: Arc<[Option<Arc<FileDeletes>>]> = vec![None; files.len()].into();
        let mut merge = MergeState::new(TABLE, cursors, deletes);
        merge.stop.store(true, atomic::Ordering::Relaxed);
        assert_eq!(merge.resume(arrived, None).unwrap(), Progress::Stopped);
        assert_eq!(merge.held(), bytes);
    }

    /// A row group that fails to decode fails its merge's step, and every
    /// byte the step owned is held or counted dropped, the failing row
    /// group's and that of the one after it, never handed to its cursor,
    /// among them: the merge hands all of it back as it retires.
    #[tokio::test]
    async fn test_merge_failing_to_decode_hands_back_every_byte_it_owned() {
        let built = sharing_a_pair("d", 0).await;
        let files: Vec<Arc<LoadedFile>> = built.iter().map(|one| Arc::clone(&one.file)).collect();
        let mut cursors = Vec::new();
        let mut arrived = Vec::new();
        for (at, file) in files.iter().enumerate() {
            let group = every_page(file, 0, 0);
            let read = file.index.pair_read(group.row_group, &group.pages);
            cursors.push(PairCursor::new(at, true, Arc::clone(file), 1));
            let chunks = file.index.pair_chunks(read).await.unwrap();
            let chunks = if at == 0 {
                chunks.without_bytes()
            } else {
                chunks
            };
            arrived.push((at, 0, chunks));
        }
        assert!(
            arrived[1].2.read_bytes() > 0,
            "the row group after the failing one holds bytes"
        );
        let deletes: Arc<[Option<Arc<FileDeletes>>]> = vec![None; files.len()].into();
        let mut merge = MergeState::new(TABLE, cursors, deletes);
        let owned = merge.owned(&arrived);
        let failed = merge.resume(arrived, None);
        assert!(failed.is_err(), "{failed:?}");
        assert_eq!(merge.dropped + merge.held(), owned);
    }

    /// A step the blocking pool drops unstarted takes its state with it:
    /// the merge hands back the bytes the state owned with the row groups
    /// handed to it, as `drive` counted them at the spawn, and fails as
    /// canceled.
    #[tokio::test]
    async fn test_stepped_losing_a_step_hands_back_what_its_state_owned() {
        let built = sharing_a_pair("l", 0).await;
        let reused = vec![false; built.len()];
        let none = vec![Vec::new(); built.len()];
        let generation = Checked::of(&built, &reused, &reused, &none);
        let GroupMerges {
            checked, merges, ..
        } = GroupMerges::of(&generation.members, &generation.files, &generation.groups);
        let mut ahead = ReadAhead::new(
            &generation.files,
            &merges,
            MERGE_READ_AHEAD_BYTES,
            load_concurrency(),
        );
        let mut run = Merges::new(
            TABLE,
            &generation.files,
            &checked,
            merges,
            &generation.deletes,
        );
        run.hand_over(&mut ahead);
        ahead.start();
        while let Some(one) = ahead.in_flight.next().await {
            run.land(one, &mut ahead);
        }
        let (merge, state) = run.next_step(&ahead).expect("the merge's reads landed");
        let batch = std::mem::take(&mut ahead.landed[merge]);
        let owned = state.owned(&batch);
        assert!(
            owned > 0 && ahead.taken == owned,
            "the step owns what was read"
        );
        drop((state, batch));
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let canceled = task.await.unwrap_err();
        run.stepped(merge, Err(LostStep { canceled, owned }), &mut ahead);
        assert_eq!(ahead.taken, 0);
        assert!(
            matches!(run.settled(), Some(Err(Error::VerifyCanceled { .. }))),
            "the lost step fails its merge as canceled"
        );
    }

    /// The merges of a load share its slots: with five slots and two merges
    /// of one cursor of three row groups each, both first row groups start
    /// and one read ahead, the one the merges reach first, a slot left for
    /// each cursor; once the merge it is for ends, its cursor's slot goes
    /// to the other's next read ahead.
    #[tokio::test]
    async fn test_read_ahead_shares_its_slots_across_merges() {
        let residency = Residency::Full;
        let groups = 3;
        let rows: Vec<(i64, i64)> = (0..i64::try_from(groups).unwrap())
            .flat_map(|key| (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, row)))
            .collect();
        let group_rows = usize::try_from(OVERLAPPING_ROWS_PER_KEY).unwrap();
        let mut built = Vec::new();
        for at in 0..2 {
            let name = format!("x-{at}");
            built.push(built_in_groups(&name, &rows, FEW_PAGE_ROWS, group_rows, residency).await);
        }
        let files: Vec<Arc<LoadedFile>> = built.iter().map(|one| Arc::clone(&one.file)).collect();
        let later = groups;
        let merges: Vec<Vec<(usize, Vec<GroupPages>)>> = [(0, later), (1, 0)]
            .into_iter()
            .map(|(at, from)| {
                vec![(
                    at,
                    (0..groups)
                        .map(|row_group| every_page(&files[at], row_group, from + row_group))
                        .collect(),
                )]
            })
            .collect();
        let five_slots = 5;
        let mut ahead = ReadAhead::new(&files, &merges, MERGE_READ_AHEAD_BYTES, five_slots);
        ahead.want([(0, 0), (1, 0)]);
        let mut reads = next_reads(&mut ahead, five_slots);
        assert_eq!(
            places(&reads),
            [(0, 0), (1, 0), (1, 1)],
            "the second merge's next row group, which the merges reach first"
        );
        ahead.end(1);
        while let Some(read) = ahead.next(reads.len(), five_slots) {
            reads.push(read);
        }
        assert_eq!(
            places(&reads[3..]),
            [(0, 1)],
            "the ended merge reads no more"
        );
    }

    /// Files of a compaction's key chain, each sharing its edge keys with
    /// its neighbours, all over one order range, as files a compaction
    /// rolled by size from a bucket's whole history are.
    const CHAIN: usize = 4_000;

    /// Keys between a chained file's first key and its last, and the order
    /// range every chained file spans.
    const CHAIN_LINK_KEYS: i64 = 10;
    const CHAIN_ORDERS: (i64, i64) = (0, 1_000_000);

    /// The most comparisons the sweep of a key chain may make per file: a
    /// few sorts and searches of a group of `CHAIN` files, where testing
    /// each file against every file open by order range makes about
    /// `CHAIN` squared halved.
    const COMPARED_PER_CHAINED_FILE: u64 = 64;

    /// The sweep of an overlap group whose files chain by key and share one
    /// order range finds each file's neighbours, and only them, in a number
    /// of comparisons linear in the files up to a log factor.
    #[test]
    fn test_meetings_of_a_key_chain_with_one_order_range_is_linear() {
        let files: Vec<Described> = (0..i64::try_from(CHAIN).unwrap())
            .map(|at| {
                let first = at * CHAIN_LINK_KEYS;
                file(
                    Some((TWO, 0)),
                    (first, first + CHAIN_LINK_KEYS),
                    CHAIN_ORDERS,
                )
            })
            .collect();
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(span).collect();
        let checked = vec![true; spans.len()];
        let group: Vec<usize> = (0..spans.len()).collect();
        let mut compared = 0;
        let found = meetings(&spans, &checked, &group, &mut compared);
        for (at, theirs) in found.iter().enumerate() {
            let mut theirs = theirs.clone();
            theirs.sort_unstable();
            let neighbours: Vec<usize> =
                [at.checked_sub(1), Some(at + 1).filter(|&next| next < CHAIN)]
                    .into_iter()
                    .flatten()
                    .collect();
            assert_eq!(theirs, neighbours, "the counterparts of {at}");
        }
        assert!(
            compared > 0 && compared <= COMPARED_PER_CHAINED_FILE * approx::count(CHAIN),
            "{compared} comparisons over {CHAIN} files"
        );
    }

    /// Cases of the drawn check, files per case at least and at most, and
    /// the pair universe the files draw from: few keys and order values,
    /// so files repeat pairs.
    const DRAWN_CASES: u64 = 240;
    const FEWEST_DRAWN_FILES: u64 = 2;
    const MOST_DRAWN_FILES: u64 = 7;
    const DRAWN_KEYS: u64 = 8;
    const DRAWN_ORDERS: usize = 8;

    /// One in how many drawn rows is deleted, one in how many drawn files
    /// is checked, one in how many checked files is reused with deletes
    /// that shrank, and one in how many cases draws its files around the
    /// corners of their own key and order ranges.
    const DELETED_ONE_IN: u64 = 7;
    const CHECKED_ONE_IN: u64 = 2;
    const SHRANK_ONE_IN: u64 = 2;
    const CORNERS_ONE_IN: u64 = 3;

    /// The sparsest a drawn file's rows are: one in how many pairs of its
    /// range at most.
    const SPARSEST_ONE_IN: u64 = 3;

    /// The most rows a drawn file's pages and row groups hold.
    const MOST_DRAWN_PAGE_ROWS: u64 = 4;
    const MOST_DRAWN_GROUP_ROWS: u64 = 14;

    /// Where drawn files are filed: unbucketed, and every bucket of two
    /// bucketings, one key falling in buckets congruent modulo two.
    const DRAWN_PLACES: [Place; 7] = [
        None,
        Some((TWO, 0)),
        Some((TWO, 1)),
        Some((FOUR, 0)),
        Some((FOUR, 1)),
        Some((FOUR, 2)),
        Some((FOUR, 3)),
    ];

    /// The bytes every drawn string key, and every drawn string order
    /// value, starts with: long shared prefixes, which the fences cut past.
    const TEXT_KEY_PREFIX: usize = 40;
    const TEXT_ORDER_PREFIX: usize = 64;

    /// The ends of the drawn string order values, ascending, several a
    /// prefix of the next, so fences cut at every length.
    const TEXT_ORDER_ENDS: [&str; DRAWN_ORDERS] = ["", "a", "aa", "ab", "b", "ba", "bb", "c"];

    /// How a drawn file's keys, or its order values, are written: as longs,
    /// or as strings sharing a long prefix, ascending as the indexes do.
    #[derive(Clone, Copy, Debug)]
    enum Written {
        Long,
        Text,
    }

    /// Drawn key `index` as `written`.
    fn drawn_key(written: Written, index: u64) -> Key {
        match written {
            Written::Long => Key::Long(i64::try_from(index).unwrap()),
            Written::Text => Key::Str(format!(
                "{}{}",
                "k".repeat(TEXT_KEY_PREFIX),
                char::from(b'a' + u8::try_from(index).unwrap())
            )),
        }
    }

    /// Drawn order value `index` as `written`.
    fn drawn_order(written: Written, index: u64) -> OrderValue {
        match written {
            Written::Long => OrderValue::Long(i64::try_from(index).unwrap()),
            Written::Text => OrderValue::Str(format!(
                "{}{}",
                "p".repeat(TEXT_ORDER_PREFIX),
                TEXT_ORDER_ENDS[usize::try_from(index).unwrap()]
            )),
        }
    }

    /// One drawn file: where it is filed, its rows as (key, order value)
    /// indexes, ascending, whether the load checks it and whether it is
    /// reused with deletes that shrank, the positions of its deleted rows,
    /// and its rows per page and per row group.
    struct DrawnFile {
        place: Place,
        rows: Vec<(u64, u64)>,
        checked: bool,
        shrank: bool,
        deleted: Vec<u64>,
        page_rows: usize,
        group_rows: usize,
    }

    /// Files drawn from the pair universe by `draw`, in file order, each
    /// holding only keys its bucket can hold (a key falls in its index
    /// modulo the bucket count). Around corners, each file draws an order
    /// range of its own and holds the four pairs at its key range's and
    /// that range's ends, so the ranges files share often end on a pair.
    fn draw_files(draw: &mut impl FnMut(u64) -> u64, corners: bool) -> Vec<DrawnFile> {
        let count = FEWEST_DRAWN_FILES + draw(MOST_DRAWN_FILES - 1);
        let orders = approx::count(DRAWN_ORDERS);
        let mut drawn: Vec<DrawnFile> = (0..count)
            .map(|_| {
                let place =
                    DRAWN_PLACES[usize::try_from(draw(approx::count(DRAWN_PLACES.len()))).unwrap()];
                let keys: Vec<u64> = (0..DRAWN_KEYS)
                    .filter(|&key| {
                        place.is_none_or(|(of, bucket)| key % u64::from(of) == u64::from(bucket))
                    })
                    .collect();
                let first = usize::try_from(draw(approx::count(keys.len()))).unwrap();
                let last =
                    first + usize::try_from(draw(approx::count(keys.len() - first))).unwrap();
                let (low, high) = if corners {
                    let low = draw(orders);
                    (low, low + draw(orders - low))
                } else {
                    (0, orders - 1)
                };
                let one_in = 1 + draw(SPARSEST_ONE_IN);
                let ends = [keys[first], keys[last]];
                let mut rows: Vec<(u64, u64)> = keys[first..=last]
                    .iter()
                    .flat_map(|&key| (low..=high).map(move |order| (key, order)))
                    .filter(|&(key, order)| {
                        let corner =
                            corners && ends.contains(&key) && (order == low || order == high);
                        corner || draw(one_in) == 0
                    })
                    .collect();
                if rows.is_empty() {
                    rows.push((keys[first], low));
                }
                let deleted = (0..approx::count(rows.len()))
                    .filter(|_| draw(DELETED_ONE_IN) == 0)
                    .collect();
                let checked = draw(CHECKED_ONE_IN) == 0;
                let page_rows = 1 + draw(MOST_DRAWN_PAGE_ROWS);
                DrawnFile {
                    place,
                    rows,
                    checked,
                    shrank: checked && draw(SHRANK_ONE_IN) == 0,
                    deleted,
                    page_rows: usize::try_from(page_rows).unwrap(),
                    group_rows: usize::try_from(page_rows + draw(MOST_DRAWN_GROUP_ROWS)).unwrap(),
                }
            })
            .collect();
        drawn.sort_by_key(|file| (file.place, file.rows[0].0));
        drawn
    }

    /// `file` written with keys and order values as `keys` and `orders`
    /// say, as file `name`, indexed in `residency`.
    async fn built_drawn(
        name: &str,
        file: &DrawnFile,
        (keys, orders): (Written, Written),
        residency: Residency,
    ) -> Built {
        let (column, key_type) = match keys {
            Written::Long => (KeyColumn::Int64, KeyType::Long),
            Written::Text => (KeyColumn::Utf8, KeyType::String),
        };
        let mut fixture = index_testing::Fixture::with_keys(
            column,
            file.rows
                .iter()
                .map(|&(key, _)| Some(drawn_key(keys, key)))
                .collect(),
        );
        let values = file
            .rows
            .iter()
            .map(|&(_, order)| drawn_order(orders, order));
        let (column, order_type) = match orders {
            Written::Long => (
                index_testing::Orders::Long(
                    values
                        .map(|value| match value {
                            OrderValue::Long(value) => Some(value),
                            OrderValue::Str(_) | OrderValue::Bytes(_) => None,
                        })
                        .collect(),
                ),
                OrderType::Long,
            ),
            Written::Text => (
                index_testing::Orders::Bytes(
                    values
                        .map(|value| match value {
                            OrderValue::Str(text) => Some(text.into_bytes()),
                            OrderValue::Long(_) | OrderValue::Bytes(_) => None,
                        })
                        .collect(),
                ),
                OrderType::String,
            ),
        };
        fixture.orders = Some(column);
        let fixture = fixture.in_pages(file.page_rows, file.group_rows);
        let layout = index_testing::ordered_layout(key_type, order_type);
        built_of(name, &fixture, &layout, file.place, residency).await
    }

    /// What a scan of every live pair of `drawn` finds: the pair the check
    /// refuses in the first of `groups` holding one, the least live pair
    /// two files hold where one is checked, with the position of the latest
    /// file holding it; and how many pairs only unchecked files share live,
    /// and how many have other copies all deleted.
    struct Scanned {
        refused: Option<((u64, u64), usize)>,
        unchecked_ties: usize,
        deleted_ties: usize,
    }

    fn scan_live_pairs(drawn: &[DrawnFile], groups: &[Vec<usize>]) -> Scanned {
        let mut holders: BTreeMap<(u64, u64), Vec<usize>> = BTreeMap::new();
        let mut copies: BTreeMap<(u64, u64), usize> = BTreeMap::new();
        for (at, file) in drawn.iter().enumerate() {
            for (position, &pair) in file.rows.iter().enumerate() {
                *copies.entry(pair).or_insert(0) += 1;
                if !file.deleted.contains(&approx::count(position)) {
                    holders.entry(pair).or_default().push(at);
                }
            }
        }
        let refusable =
            |live: &Vec<usize>| live.len() > 1 && live.iter().any(|&at| drawn[at].checked);
        let refused = groups.iter().find_map(|group| {
            holders
                .iter()
                .filter(|(_, live)| refusable(live))
                .find(|(_, live)| group.contains(&live[0]))
                .map(|(pair, live)| (*pair, *live.iter().max().unwrap()))
        });
        Scanned {
            refused,
            unchecked_ties: holders
                .values()
                .filter(|live| live.len() > 1 && !refusable(live))
                .count(),
            deleted_ties: holders
                .iter()
                .filter(|(pair, live)| live.len() == 1 && copies[*pair] > 1)
                .count(),
        }
    }

    /// What the drawn check saw across its cases, each a count that must
    /// be above zero for the check to prove anything.
    #[derive(Debug, Default)]
    struct Seen {
        refusals: usize,
        serves: usize,
        unchecked_ties: usize,
        deleted_ties: usize,
        shrank_refusals: usize,
        several_groups: usize,
        corner_refusals: usize,
    }

    /// The check across drawn files, of several row groups and pages each,
    /// in buckets of two bucketings and unbucketed, some checked, some
    /// reused with deletes that shrank, some with deleted rows, with long
    /// keys and order values and string ones sharing long prefixes, in
    /// every tier, refuses exactly what a scan of every file's live pairs
    /// refuses (RFD 025, Refresh): in the first overlap group that holds
    /// one, the least live pair two files hold where one is checked, naming
    /// the latest file holding it in file order; a pair only unchecked
    /// files share, or one whose other copies are deleted, is no refusal.
    /// A third of the cases draw their files around the corners of their
    /// ranges, where the ranges files share end on a pair.
    #[tokio::test]
    async fn test_verify_rows_refuses_what_a_scan_of_every_live_pair_refuses() {
        refuses_what_a_scan_refuses(&[MERGE_READ_AHEAD_BYTES]).await;
    }

    /// Read-ahead budgets that bind on the drawn files, whose row groups'
    /// chosen pages take tens to hundreds of bytes: none read ahead, a row
    /// group or so, and a few.
    const DRAWN_BUDGETS: [u64; 3] = [BELOW_ANY_ROW_GROUP, 64, 256];

    /// The drawn cases of
    /// `test_verify_rows_refuses_what_a_scan_of_every_live_pair_refuses`
    /// refuse and serve alike under budgets that bind, which stop the merge
    /// to read ahead as it drops row groups and leave cursors waiting on
    /// row groups not yet read.
    #[tokio::test]
    async fn test_verify_rows_under_a_binding_budget_refuses_what_a_scan_refuses() {
        refuses_what_a_scan_refuses(&DRAWN_BUDGETS).await;
    }

    /// The drawn check, each case's merge reading ahead within the next of
    /// `budgets` in turn.
    async fn refuses_what_a_scan_refuses(budgets: &[u64]) {
        let mut state = 0_u64;
        let mut draw = |below: u64| {
            state = state.wrapping_mul(MIX).wrapping_add(STEP);
            (state >> 33) % below
        };
        let residencies = Residency::ALL;
        let written = [
            (Written::Long, Written::Long),
            (Written::Text, Written::Text),
            (Written::Long, Written::Text),
            (Written::Text, Written::Long),
        ];
        let mut seen = Seen::default();
        for case in 0..DRAWN_CASES {
            let at = usize::try_from(case).unwrap();
            let residency = residencies[at % residencies.len()];
            let kinds = written[(at / residencies.len()) % written.len()];
            let corners = draw(CORNERS_ONE_IN) == 0;
            let drawn = draw_files(&mut draw, corners);
            let mut files = Vec::new();
            for (at, file) in drawn.iter().enumerate() {
                files.push(built_drawn(&format!("d-{at:02}"), file, kinds, residency).await);
            }
            let reused: Vec<bool> = drawn
                .iter()
                .map(|file| !file.checked || file.shrank)
                .collect();
            let shrank: Vec<bool> = drawn.iter().map(|file| file.shrank).collect();
            let gone: Vec<Vec<u64>> = drawn.iter().map(|file| file.deleted.clone()).collect();
            let budget = budgets[(at / (residencies.len() * written.len())) % budgets.len()];
            let (groups, check) =
                check_rows_deleting(&files, &reused, &shrank, &gone, budget).await;
            let scanned = scan_live_pairs(&drawn, &groups);
            seen.unchecked_ties += scanned.unchecked_ties;
            seen.deleted_ties += scanned.deleted_ties;
            seen.several_groups += usize::from(groups.len() > 1);
            let Some(((key, order), latest)) = scanned.refused else {
                assert!(
                    check.is_ok(),
                    "case {case}, {kinds:?}, {residency:?}: {check:?}"
                );
                seen.serves += 1;
                continue;
            };
            let RefusalReason::DuplicateRows {
                path,
                key: refused_key,
                order: refused_order,
            } = refusal_of(check)
            else {
                panic!("case {case}: expected duplicate_rows");
            };
            assert_eq!(
                (path, refused_key, *refused_order),
                (
                    format!("d-{latest:02}"),
                    drawn_key(kinds.0, key),
                    drawn_order(kinds.1, order)
                ),
                "case {case}, {kinds:?}, {residency:?}"
            );
            seen.refusals += 1;
            seen.shrank_refusals += usize::from(drawn.iter().any(|file| file.shrank));
            seen.corner_refusals += usize::from(corners);
        }
        assert!(
            seen.refusals > 0
                && seen.serves > 0
                && seen.unchecked_ties > 0
                && seen.deleted_ties > 0
                && seen.shrank_refusals > 0
                && seen.several_groups > 0
                && seen.corner_refusals > 0,
            "{seen:?}"
        );
    }

    /// Worker threads of the runtime of the wide drawn check: several, so
    /// that several merges step at once (`shard::workers`).
    const WIDE_WORKERS: usize = 4;

    /// Cases of the wide drawn check, the seed of its draws, and the files
    /// per case: at least the fewest, and up to the more beyond them.
    const WIDE_CASES: u64 = 48;
    const WIDE_SEED: u64 = 17;
    const FEWEST_WIDE_FILES: u64 = 4;
    const MORE_WIDE_FILES: u64 = 12;

    /// The keys and order values a wide case draws from, at least the
    /// fewest and up to the more beyond them, and the most keys after its
    /// first a wide file spans.
    const FEWEST_WIDE_KEYS: u64 = 32;
    const MORE_WIDE_KEYS: u64 = 64;
    const FEWEST_WIDE_ORDERS: u64 = 4;
    const MORE_WIDE_ORDERS: u64 = 16;
    const WIDE_FILE_KEYS: u64 = 6;

    /// One in how many wide cases may hold an unbucketed file, which joins
    /// every bucket's files in one overlap group.
    const UNBUCKETED_ONE_IN: u64 = 4;

    /// Buckets of the third bucketing of the wide check: one key falls in
    /// buckets congruent modulo two and four under the other two.
    const EIGHT: u32 = 8;

    /// Where wide drawn files are filed: unbucketed, and every bucket of
    /// three bucketings.
    const WIDE_PLACES: [Place; 15] = [
        None,
        Some((TWO, 0)),
        Some((TWO, 1)),
        Some((FOUR, 0)),
        Some((FOUR, 1)),
        Some((FOUR, 2)),
        Some((FOUR, 3)),
        Some((EIGHT, 0)),
        Some((EIGHT, 1)),
        Some((EIGHT, 2)),
        Some((EIGHT, 3)),
        Some((EIGHT, 4)),
        Some((EIGHT, 5)),
        Some((EIGHT, 6)),
        Some((EIGHT, 7)),
    ];

    /// Files drawn from a wide pair universe by `draw`, in file order: many
    /// files over many keys in buckets of three bucketings, each over a few
    /// keys, so a case has many overlap groups, several of which can
    /// refuse, and one in `UNBUCKETED_ONE_IN` cases may file some
    /// unbucketed.
    fn draw_wide_files(draw: &mut impl FnMut(u64) -> u64) -> Vec<DrawnFile> {
        let count = FEWEST_WIDE_FILES + draw(MORE_WIDE_FILES);
        let all_keys = FEWEST_WIDE_KEYS + draw(MORE_WIDE_KEYS);
        let orders = FEWEST_WIDE_ORDERS + draw(MORE_WIDE_ORDERS);
        let unbucketed = draw(UNBUCKETED_ONE_IN) == 0;
        let places = approx::count(WIDE_PLACES.len());
        let mut drawn: Vec<DrawnFile> = (0..count)
            .map(|_| {
                let pick = if unbucketed {
                    draw(places)
                } else {
                    1 + draw(places - 1)
                };
                let place = WIDE_PLACES[usize::try_from(pick).unwrap()];
                let keys: Vec<u64> = (0..all_keys)
                    .filter(|&key| {
                        place.is_none_or(|(of, bucket)| key % u64::from(of) == u64::from(bucket))
                    })
                    .collect();
                let first = usize::try_from(draw(approx::count(keys.len()))).unwrap();
                let last = (first + usize::try_from(draw(1 + WIDE_FILE_KEYS)).unwrap())
                    .min(keys.len() - 1);
                let low = draw(orders);
                let high = low + draw(orders - low);
                let one_in = 1 + draw(SPARSEST_ONE_IN);
                let mut rows: Vec<(u64, u64)> = keys[first..=last]
                    .iter()
                    .flat_map(|&key| (low..=high).map(move |order| (key, order)))
                    .filter(|_| draw(one_in) == 0)
                    .collect();
                if rows.is_empty() {
                    rows.push((keys[first], low));
                }
                let deleted = (0..approx::count(rows.len()))
                    .filter(|_| draw(DELETED_ONE_IN) == 0)
                    .collect();
                let checked = draw(CHECKED_ONE_IN) == 0;
                let page_rows = 1 + draw(MOST_DRAWN_PAGE_ROWS);
                DrawnFile {
                    place,
                    rows,
                    checked,
                    shrank: checked && draw(SHRANK_ONE_IN) == 0,
                    deleted,
                    page_rows: usize::try_from(page_rows).unwrap(),
                    group_rows: usize::try_from(page_rows + draw(MOST_DRAWN_GROUP_ROWS)).unwrap(),
                }
            })
            .collect();
        drawn.sort_by_key(|file| (file.place, file.rows[0].0));
        drawn
    }

    /// What the wide drawn check saw across its cases, each a count that
    /// must be above zero for the check to prove anything.
    #[derive(Debug, Default)]
    struct WideSeen {
        refusals: usize,
        serves: usize,
        several_groups: usize,
        several_refusing: usize,
    }

    /// On a runtime of `WIDE_WORKERS` workers, so several merges step at
    /// once, the check across wide drawn files, of many overlap groups in
    /// buckets of three bucketings, several of which can refuse, under
    /// budgets that bind and one that does not, in every tier, refuses
    /// exactly what a scan of every file's live pairs refuses, the pair of
    /// the first group in group order that holds one, and what checking
    /// the groups one at a time refuses, and passes where they pass. Each
    /// case's merges hand back every byte they took, groups stopped after
    /// a refusal included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_verify_rows_merging_groups_on_several_workers_refuses_what_a_scan_refuses() {
        assert_eq!(shard::workers(), WIDE_WORKERS, "several steps at once");
        let mut state = WIDE_SEED;
        let mut draw = |below: u64| {
            state = state.wrapping_mul(MIX).wrapping_add(STEP);
            (state >> 33) % below
        };
        let budgets: Vec<u64> = DRAWN_BUDGETS
            .into_iter()
            .chain([MERGE_READ_AHEAD_BYTES])
            .collect();
        let residencies = Residency::ALL;
        let kinds = (Written::Long, Written::Long);
        let mut seen = WideSeen::default();
        for case in 0..WIDE_CASES {
            let at = usize::try_from(case).unwrap();
            let residency = residencies[at % residencies.len()];
            let budget = budgets[(at / residencies.len()) % budgets.len()];
            let drawn = draw_wide_files(&mut draw);
            let mut files = Vec::new();
            for (at, file) in drawn.iter().enumerate() {
                files.push(built_drawn(&format!("d-{at:02}"), file, kinds, residency).await);
            }
            let reused: Vec<bool> = drawn
                .iter()
                .map(|file| !file.checked || file.shrank)
                .collect();
            let shrank: Vec<bool> = drawn.iter().map(|file| file.shrank).collect();
            let gone: Vec<Vec<u64>> = drawn.iter().map(|file| file.deleted.clone()).collect();
            let generation = Checked::of(&files, &reused, &shrank, &gone);
            let groups = &generation.groups;
            let (check, taken) = generation.check_released(budget).await;
            let serial = generation.check_serially(budget).await;
            let what = format!("case {case}, {residency:?}, budget {budget}");
            assert_eq!(taken, 0, "{what}: every merge hands back its bytes");
            seen.several_groups += usize::from(groups.len() > 1);
            let refusing = groups
                .iter()
                .filter(|&group| {
                    scan_live_pairs(&drawn, std::slice::from_ref(group))
                        .refused
                        .is_some()
                })
                .count();
            seen.several_refusing += usize::from(refusing > 1);
            let Some(((key, order), latest)) = scan_live_pairs(&drawn, groups).refused else {
                let merges = check
                    .unwrap_or_else(|error| panic!("{what}: {error:?}"))
                    .merges;
                assert_eq!(Ok(merges), serial.map_err(|_| ()), "{what}: serially");
                seen.serves += 1;
                continue;
            };
            let expected = (
                format!("d-{latest:02}"),
                drawn_key(kinds.0, key),
                drawn_order(kinds.1, order),
            );
            for (outcome, how) in [
                (check.map(|_| ()), "at once"),
                (serial.map(|_| ()), "serially"),
            ] {
                let RefusalReason::DuplicateRows {
                    path,
                    key: refused_key,
                    order: refused_order,
                } = refusal_of(outcome)
                else {
                    panic!("{what}: expected duplicate_rows {how}");
                };
                assert_eq!(
                    (path, refused_key, *refused_order),
                    expected,
                    "{what}: {how}"
                );
            }
            seen.refusals += 1;
        }
        assert!(
            seen.refusals > 0
                && seen.serves > 0
                && seen.several_groups > 0
                && seen.several_refusing > 0,
            "{seen:?}"
        );
    }

    /// The shared range of the `may_hold` test: keys 2 to 5, order values
    /// 10 to 20.
    const SHARED_KEYS: (i64, i64) = (2, 5);
    const SHARED_ORDERS: (i64, i64) = (10, 20);

    /// A range of pairs holds a pair of the shared range exactly when one
    /// lies in both, ends included: within one key, by the key and the
    /// order values; across keys, by the shared range's least and greatest
    /// pairs, a top at or below either side of them as it covers them.
    #[test]
    fn test_may_hold_keeps_a_range_touching_the_shared_corners() {
        let shared = Shared {
            keys: (PairPart::Long(SHARED_KEYS.0), PairPart::Long(SHARED_KEYS.1)),
            orders: (
                PairPart::Long(SHARED_ORDERS.0),
                PairPart::Long(SHARED_ORDERS.1),
            ),
        };
        let (low_order, high_order) = (SHARED_ORDERS.0, SHARED_ORDERS.1);
        let (low_key, high_key) = (SHARED_KEYS.0, SHARED_KEYS.1);
        let inside = low_key + 1;
        let cases = [
            // Within one key.
            (
                long_pair(inside, high_order),
                Top::Below(long_pair(inside, high_order + 1)),
                true,
            ),
            (
                long_pair(inside, high_order + 1),
                Top::AtMost(long_pair(inside, high_order + 1)),
                false,
            ),
            (
                long_pair(inside, 0),
                Top::AtMost(long_pair(inside, low_order)),
                true,
            ),
            (
                long_pair(inside, 0),
                Top::Below(long_pair(inside, low_order)),
                false,
            ),
            (
                long_pair(low_key - 1, 0),
                Top::AtMost(long_pair(low_key - 1, low_order)),
                false,
            ),
            (
                long_pair(high_key + 1, low_order),
                Top::AtMost(long_pair(high_key + 1, high_order)),
                false,
            ),
            // Across keys.
            (
                long_pair(high_key, high_order),
                Top::Below(long_pair(high_key + 1, 0)),
                true,
            ),
            (
                long_pair(high_key, high_order + 1),
                Top::Below(long_pair(high_key + 1, 0)),
                false,
            ),
            (
                long_pair(low_key - 1, 0),
                Top::AtMost(long_pair(low_key, low_order)),
                true,
            ),
            (
                long_pair(low_key - 1, 0),
                Top::Below(long_pair(low_key, low_order)),
                false,
            ),
        ];
        for (low, top, held) in cases {
            assert_eq!(may_hold(low, top, shared), held, "{low:?} up to {top:?}");
        }
    }
}
