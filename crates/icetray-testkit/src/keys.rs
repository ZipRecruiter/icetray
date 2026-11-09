//! Key and value derivation shared by every fixture (RFD 014): the `i`-th
//! key of each type, an absent key beside it, a key past every fixture key,
//! and a value that is a function of its key, so any answer is checkable
//! without a stored oracle.

use std::fmt::Write as _;

use icetray_core::key::UUID_BYTES;
use icetray_core::{Key, KeyType};

use crate::protobuf;

/// The spacing between fixture keys, shared with the golden recipe
/// (`dev/golden-tables.py`): `key + 1` is absent and inside the range.
pub const KEY_STEP: u64 = 3;

/// Bytes in a fixture value where a specification does not say: a
/// hundred, so a contract page of a thousand values is about a hundred
/// kibibytes, under the doctor's value-page advice, and the small fixture
/// draws no finding.
pub const VALUE_BYTES: usize = 100;

/// The text between a string key's padding and its digits.
const STRING_PREFIX: &str = "user:";

/// Digits a string key's value is zero-padded to, so string order is
/// numeric order.
const STRING_DIGITS: usize = 16;

/// The bytes of a string key beyond its padding: the prefix and the digits.
/// A test that wants keys of a given length takes its `key_pad` from this.
pub const STRING_KEY_FIXED_BYTES: usize = STRING_PREFIX.len() + STRING_DIGITS;

/// The character `key_pad` repeats before a string key.
pub(crate) const STRING_PAD: char = 'p';

/// The suffix that turns a string key into an absent one greater than it.
const ABSENT_SUFFIX: char = 'x';

/// The tag byte that starts every binary key, `pad + 1` times, before the
/// big-endian value; bytewise order is then numeric order, as an encoded
/// composite key's would be.
const BINARY_TAG: u8 = 0x4b;

/// The byte appended to a binary key to make an absent one greater than it.
const ABSENT_BYTE: u8 = 0;

/// Where a uuid key's big-endian value starts: its last eight bytes, so the
/// bytewise unsigned order RFD 008 fixes for uuid is numeric order.
const UUID_VALUE_OFFSET: usize = UUID_BYTES - size_of::<u64>();

/// The string past every fixture string key: `~` is above the padding
/// character, the prefix's first letter, and every character of
/// `SCATTERED_ALPHABET`.
const FAR_STRING: &str = "~~~~";

/// The length of the binary far key; its first byte is above `BINARY_TAG`.
const FAR_BYTES_LEN: usize = 4;

/// The byte every far binary and uuid key is made of.
const FAR_BYTE: u8 = 0xff;

/// The FNV-1a offset basis, the seed of a string, binary, or uuid key's value stream.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// The FNV-1a prime.
const FNV_PRIME: u64 = 0x0100_0000_01b3;

/// The multiplier that spreads a key's seed before the xorshift stream
/// starts: the golden ratio's fractional bits, so neighboring keys start
/// far apart.
const SEED_MULTIPLIER: u64 = 0x9e37_79b9_7f4a_7c15;

/// The first shift of Marsaglia's xorshift64 triple (13, 7, 17), whose
/// stream has full period, so no fixture's values repeat.
const XORSHIFT_FIRST_LEFT: u32 = 13;

/// The second shift of the triple, to the right.
const XORSHIFT_RIGHT: u32 = 7;

/// The third shift of the triple, to the left.
const XORSHIFT_SECOND_LEFT: u32 = 17;

/// The base64url characters in byte order, so a string of them sorts as
/// the number they spell. A digit's value is its index.
const SCATTERED_ALPHABET: &[u8; 64] =
    b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

/// The bits one character of `SCATTERED_ALPHABET` spells.
const BASE64_BITS: u32 = SCATTERED_ALPHABET.len().trailing_zeros();

/// The bits of a scattered key's value: 126, the width of a 21-character
/// random identifier.
const SCATTERED_VALUE_BITS: u32 = 126;

/// The characters a scattered key's value is spelled in.
const SCATTERED_CHARS: u32 = SCATTERED_VALUE_BITS / BASE64_BITS;

/// The mask of one character's bits.
const SCATTERED_DIGIT_MASK: u8 = (1 << BASE64_BITS) - 1;

/// The top bits of a scattered key's value, which hold the id; the rest
/// are pseudo-random. Twenty-three bits admit about 8.4 million ids, room
/// for the benchmark's scattered table at `--scattered-files 128` and its
/// append, 4.85 million, while neighbors in one bucket of its smallest
/// table share only their first few characters, as sorted random keys do.
/// A key's spelling depends on this, so the benchmark's manifest records
/// `SCATTERED_ID_SPACE` and writes a table recorded under another again.
const SCATTERED_ID_BITS: u32 = 23;

/// The ids a scattered key exists for: `0..SCATTERED_ID_SPACE`.
pub const SCATTERED_ID_SPACE: u64 = 1 << SCATTERED_ID_BITS;

/// The pseudo-random low bits of a scattered key's value, below the id.
const SCATTERED_OFFSET_BITS: u32 = SCATTERED_VALUE_BITS - SCATTERED_ID_BITS;

/// What follows a scattered key's identifier, as the qualifier of a
/// composite key does.
const SCATTERED_SUFFIX: &str = "#model#1";

/// The bytes of a scattered key beyond its padding: the identifier and the
/// suffix.
pub const SCATTERED_KEY_FIXED_BYTES: usize = SCATTERED_CHARS as usize + SCATTERED_SUFFIX.len();

/// The increment of splitmix64's state: the golden ratio's fractional
/// bits, so consecutive ids start far apart.
const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// The first multiplier of splitmix64's finalizer.
const SPLITMIX_FIRST_MULTIPLIER: u64 = 0xbf58_476d_1ce4_e5b9;

/// The second multiplier of splitmix64's finalizer.
const SPLITMIX_SECOND_MULTIPLIER: u64 = 0x94d0_49bb_1331_11eb;

/// The finalizer's first shift.
const SPLITMIX_FIRST_SHIFT: u32 = 30;

/// The finalizer's second shift.
const SPLITMIX_SECOND_SHIFT: u32 = 27;

/// The finalizer's third shift.
const SPLITMIX_THIRD_SHIFT: u32 = 31;

/// How a fixture's string keys are spelled. A key of another type takes no
/// shape; `write_fixture` refuses a scattered one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyShape {
    /// `key_at_padded`: the padding, `user:`, and sixteen zero-padded
    /// digits, so neighboring keys share all but their last few characters
    /// and a page of them compresses to almost nothing.
    #[default]
    Sequential,
    /// `scattered_key_at`: the padding, a 126-bit value in 21 base64url
    /// characters, and a fixed suffix, about thirty bytes, as an opaque
    /// random identifier in a composite key is. The value's top bits are the
    /// id and the rest pseudo-random, so neighbors in one bucket share only
    /// their first few characters. Only ids below `SCATTERED_ID_SPACE` have
    /// one.
    Scattered,
}

/// The `i`-th fixture key of `key_type`, with no padding.
pub fn key_at(i: u64, key_type: KeyType) -> Key {
    key_at_padded(i, key_type, 0)
}

/// The `i`-th fixture key of `key_type` with `pad` extra prefix bytes on a
/// string or binary key: `Long(3i)`; `pad` copies of `p`, `user:`, and the
/// sixteen-digit value; `pad + 1` tag bytes and the big-endian value; a
/// uuid whose last eight bytes are the big-endian value. Strictly
/// increasing in `i` for every type under `Key`'s order. A long or uuid key
/// takes no padding; `write_fixture` refuses a specification that asks.
pub fn key_at_padded(i: u64, key_type: KeyType, pad: usize) -> Key {
    let value = i * KEY_STEP;
    match key_type {
        KeyType::Long => {
            Key::Long(i64::try_from(value).expect("fixture ids stay far below i64::MAX / KEY_STEP"))
        }
        KeyType::String => {
            let padding = String::from(STRING_PAD).repeat(pad);
            let width = STRING_DIGITS;
            Key::Str(format!("{padding}{STRING_PREFIX}{value:0width$}"))
        }
        KeyType::Binary => {
            let mut bytes = vec![BINARY_TAG; pad + 1];
            bytes.extend_from_slice(&value.to_be_bytes());
            Key::Bytes(bytes)
        }
        KeyType::Uuid => {
            let mut bytes = [0u8; UUID_BYTES];
            bytes[UUID_VALUE_OFFSET..].copy_from_slice(&value.to_be_bytes());
            Key::Uuid(bytes)
        }
    }
}

/// The `i`-th fixture key of `key_type` with `pad` padding, spelled as
/// `shape` says: `key_at_padded`, or for a scattered string key
/// `scattered_key_at`. A key of another type takes no shape and is
/// `key_at_padded`'s. Strictly increasing in `i` under every shape.
pub fn key_at_shaped(i: u64, key_type: KeyType, pad: usize, shape: KeyShape) -> Key {
    match (shape, key_type) {
        (KeyShape::Scattered, KeyType::String) => scattered_key_at(i, pad),
        (
            KeyShape::Sequential,
            KeyType::Long | KeyType::String | KeyType::Binary | KeyType::Uuid,
        )
        | (KeyShape::Scattered, KeyType::Long | KeyType::Binary | KeyType::Uuid) => {
            key_at_padded(i, key_type, pad)
        }
    }
}

/// The `i`-th scattered string key with `pad` padding: `pad` copies of `p`,
/// the 126-bit `scattered_value(i)` big-endian in 21 characters of
/// `SCATTERED_ALPHABET`, and `SCATTERED_SUFFIX`. Every key is the same
/// length and the alphabet is in byte order, so string order is the
/// values' order, which is the ids'. Panics for an id at or above
/// `SCATTERED_ID_SPACE`, which has no key; a fixture refuses such an id
/// before it asks.
pub fn scattered_key_at(i: u64, pad: usize) -> Key {
    let value = scattered_value(i);
    let mut text = String::from(STRING_PAD).repeat(pad);
    for position in (0..SCATTERED_CHARS).rev() {
        let digit = (value >> (position * BASE64_BITS)) & u128::from(SCATTERED_DIGIT_MASK);
        let digit = usize::try_from(digit).expect("a digit is below the alphabet's length");
        text.push(char::from(SCATTERED_ALPHABET[digit]));
    }
    text.push_str(SCATTERED_SUFFIX);
    Key::Str(text)
}

/// The value a scattered key spells for `i`: `i` in the top
/// `SCATTERED_ID_BITS` bits and a pseudo-random offset from `i` below them,
/// so `i < j` gives a smaller value whatever the offsets.
fn scattered_value(i: u64) -> u128 {
    assert!(
        i < SCATTERED_ID_SPACE,
        "id {i} is outside the scattered key space of {SCATTERED_ID_SPACE} ids"
    );
    let first = splitmix64(i);
    let second = splitmix64(first);
    let offset = ((u128::from(first) << u64::BITS) | u128::from(second))
        & ((1u128 << SCATTERED_OFFSET_BITS) - 1);
    (u128::from(i) << SCATTERED_OFFSET_BITS) | offset
}

/// splitmix64's output for `state`: a bijective mix whose outputs for
/// neighboring states share no visible pattern.
pub(crate) fn splitmix64(state: u64) -> u64 {
    let mut z = state.wrapping_add(SPLITMIX_GAMMA);
    z = (z ^ (z >> SPLITMIX_FIRST_SHIFT)).wrapping_mul(SPLITMIX_FIRST_MULTIPLIER);
    z = (z ^ (z >> SPLITMIX_SECOND_SHIFT)).wrapping_mul(SPLITMIX_SECOND_MULTIPLIER);
    z ^ (z >> SPLITMIX_THIRD_SHIFT)
}

/// The id a scattered key of `pad` padding was derived from, or `None` for
/// text no `scattered_key_at(_, pad)` produces.
fn scattered_id_of(text: &str, pad: usize) -> Option<u64> {
    let (padding, rest) = text.split_at_checked(pad)?;
    if padding.chars().any(|c| c != STRING_PAD) {
        return None;
    }
    let digits = rest.strip_suffix(SCATTERED_SUFFIX)?;
    if digits.len() != SCATTERED_CHARS as usize {
        return None;
    }
    let value = digits.bytes().try_fold(0u128, |value, byte| {
        let digit = SCATTERED_ALPHABET.iter().position(|&c| c == byte)?;
        Some((value << BASE64_BITS) | u128::try_from(digit).ok()?)
    })?;
    let id = u64::try_from(value >> SCATTERED_OFFSET_BITS).ok()?;
    (id < SCATTERED_ID_SPACE && scattered_value(id) == value).then_some(id)
}

/// A key just above `key` that no fixture holds: the long plus one, the
/// string with a character appended, the bytes with a byte appended, the
/// uuid with its big-endian value plus one.
pub fn absent_key_from(key: &Key) -> Key {
    match key {
        Key::Long(value) => Key::Long(value + 1),
        Key::Str(text) => Key::Str(format!("{text}{ABSENT_SUFFIX}")),
        Key::Bytes(bytes) => {
            let mut absent = bytes.clone();
            absent.push(ABSENT_BYTE);
            Key::Bytes(absent)
        }
        Key::Uuid(bytes) => {
            let value = uuid_value(bytes)
                .checked_add(1)
                .expect("a fixture uuid key's value is far below u64::MAX");
            let mut absent = *bytes;
            absent[UUID_VALUE_OFFSET..].copy_from_slice(&value.to_be_bytes());
            Key::Uuid(absent)
        }
    }
}

/// `absent_key_from` the `i`-th key of `key_type`.
pub fn absent_key_near(i: u64, key_type: KeyType) -> Key {
    absent_key_from(&key_at(i, key_type))
}

/// A key of `key_type` past every fixture key, so a lookup for it routes to
/// no file.
pub fn far_key(key_type: KeyType) -> Key {
    match key_type {
        KeyType::Long => Key::Long(i64::MAX),
        KeyType::String => Key::Str(FAR_STRING.to_string()),
        KeyType::Binary => Key::Bytes(vec![FAR_BYTE; FAR_BYTES_LEN]),
        KeyType::Uuid => Key::Uuid([FAR_BYTE; UUID_BYTES]),
    }
}

/// The fixture id `key` was derived from under `pad` and `shape`, or
/// `None` for a key no `key_at_shaped(_, _, pad, shape)` produces.
pub(crate) fn id_of(key: &Key, pad: usize, shape: KeyShape) -> Option<u64> {
    match (shape, key) {
        (KeyShape::Scattered, Key::Str(text)) => scattered_id_of(text, pad),
        (KeyShape::Sequential, Key::Long(_) | Key::Str(_) | Key::Bytes(_) | Key::Uuid(_))
        | (KeyShape::Scattered, Key::Long(_) | Key::Bytes(_) | Key::Uuid(_)) => {
            sequential_id_of(key, pad)
        }
    }
}

/// The fixture id `key` was derived from under `pad`, or `None` for a key
/// no `key_at_padded(_, _, pad)` produces: a value that is not a multiple
/// of `KEY_STEP`, a string without the padding and prefix, bytes of another
/// length, a uuid with a non-zero head.
fn sequential_id_of(key: &Key, pad: usize) -> Option<u64> {
    let value = match key {
        Key::Long(value) => u64::try_from(*value).ok()?,
        Key::Str(text) => {
            let (padding, rest) = text.split_at_checked(pad)?;
            if padding.chars().any(|c| c != STRING_PAD) {
                return None;
            }
            let digits = rest.strip_prefix(STRING_PREFIX)?;
            if digits.len() != STRING_DIGITS {
                return None;
            }
            digits.parse().ok()?
        }
        Key::Bytes(bytes) => {
            let (tags, value) = bytes.split_at_checked(pad + 1)?;
            if tags.iter().any(|&byte| byte != BINARY_TAG) {
                return None;
            }
            u64::from_be_bytes(<[u8; size_of::<u64>()]>::try_from(value).ok()?)
        }
        Key::Uuid(bytes) => {
            if bytes[..UUID_VALUE_OFFSET].iter().any(|&byte| byte != 0) {
                return None;
            }
            uuid_value(bytes)
        }
    };
    (value % KEY_STEP == 0).then_some(value / KEY_STEP)
}

/// The big-endian value in a uuid key's last eight bytes.
fn uuid_value(bytes: &[u8; UUID_BYTES]) -> u64 {
    u64::from_be_bytes(
        <[u8; size_of::<u64>()]>::try_from(&bytes[UUID_VALUE_OFFSET..])
            .expect("the tail of a sixteen-byte uuid is eight bytes"),
    )
}

/// The `VALUE_BYTES`-byte value stored for `key`.
pub fn value_for(key: &Key) -> Vec<u8> {
    value_for_len(key, VALUE_BYTES)
}

/// The `len`-byte value stored for `key` in a table with values of that
/// length: a xorshift64 stream seeded from the key (a long's bits, or
/// FNV-1a over the key's bytes), deterministic, distinct for neighboring
/// keys, and incompressible, so a fixture of `n` rows occupies about
/// `n * len` bytes on disk after zstd.
pub fn value_for_len(key: &Key, len: usize) -> Vec<u8> {
    let mut stream = Stream::from_key(key);
    (0..len).map(|_| stream.next().to_le_bytes()[0]).collect()
}

/// What a fixture's values are made of.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValueKind {
    /// `value_for_len`: pseudo-random bytes, which zstd does not shrink.
    #[default]
    Random,
    /// `text_value_for_len`: JSON-like records whose field names repeat on
    /// every row and whose words come from a small vocabulary drawn with a
    /// skew, beside numbers that vary per row, so a page of them compresses
    /// as structured records do.
    Text,
    /// `protobuf::value`: a protobuf message of every wire type, written by
    /// hand, for a column declared protobuf (RFD 024).
    Protobuf,
    /// `json_value_for_len`: a JSON document of short fields and long
    /// prose, whose words are common ones or made of random syllables, so
    /// a page of them compresses about as far as documents of listings do.
    Json,
}

/// The `len`-byte value of `kind` stored for `key`.
pub fn value_of_kind(key: &Key, len: usize, kind: ValueKind) -> Vec<u8> {
    match kind {
        ValueKind::Random => value_for_len(key, len),
        ValueKind::Text => text_value_for_len(key, len),
        ValueKind::Protobuf => protobuf::value(key, len, FIRST_VERSION),
        ValueKind::Json => json_value_for_len(key, len),
    }
}

/// The version a key's first write carries: `value_of_version` at it is
/// `value_of_kind`.
pub const FIRST_VERSION: u64 = 0;

/// The bit every byte of a later version's mask carries, so no mask byte is
/// zero and every byte of that version's value differs from the first's.
const MASK_BYTE_SET_BIT: u8 = 1;

/// The `len`-byte value of `kind` that version `version` of `key` stores
/// (RFD 014): `value_of_kind` at `FIRST_VERSION`, and at a later version
/// that value with every byte masked by a stream seeded from the key and
/// the version, so an update's value differs from every other version's
/// and a lookup that answered from a deleted copy is told apart from one
/// that answered from the live row. A protobuf value's later version is the
/// message its stream draws under that version, which stays protobuf.
pub fn value_of_version(key: &Key, len: usize, kind: ValueKind, version: u64) -> Vec<u8> {
    if kind == ValueKind::Protobuf {
        return protobuf::value(key, len, version);
    }
    let mut value = value_of_kind(key, len, kind);
    if version != FIRST_VERSION {
        let mut mask = Stream::from_key(key);
        mask.state ^= version.wrapping_mul(SEED_MULTIPLIER);
        for byte in &mut value {
            *byte ^= mask.next().to_le_bytes()[0] | MASK_BYTE_SET_BIT;
        }
    }
    value
}

/// The word-valued fields of a text value, in the order they are written,
/// repeated from the first when a value is long enough to need more.
const TEXT_FIELDS: [&str; 6] = ["status", "region", "plan", "device", "locale", "segment"];

/// The words a text value's fields take, drawn with a skew toward the
/// front (`TextStream::word`), as a categorical field's values are.
const TEXT_WORDS: [&str; 16] = [
    "active",
    "us-east-1",
    "pro",
    "ios",
    "en-US",
    "returning",
    "pending",
    "eu-west-1",
    "free",
    "android",
    "de-DE",
    "new",
    "closed",
    "ap-south-1",
    "team",
    "web",
];

/// The earliest timestamp a text value carries, in epoch seconds
/// (September 2025).
const TEXT_TS_BASE: u64 = 1_757_000_000;

/// The span a text value's timestamp falls in after `TEXT_TS_BASE`: a year
/// of seconds.
const TEXT_TS_SPAN: u64 = 365 * 24 * 60 * 60;

/// Scores in a text value run below this.
const TEXT_SCORE_LIMIT: u64 = 1_000;

/// The `len`-byte text value stored for `key` in a table of text values: a
/// JSON-like record of a 32-bit id, a timestamp within a year, and a score,
/// then word fields from `TEXT_FIELDS`, each a word of `TEXT_WORDS`, until
/// the record reaches `len` bytes, where it is cut and closed with a brace.
/// Drawn from the key's xorshift stream, so it is deterministic and
/// distinct for neighboring keys.
pub fn text_value_for_len(key: &Key, len: usize) -> Vec<u8> {
    let mut stream = Stream::from_key(key);
    let id = stream.next() & u64::from(u32::MAX);
    let ts = TEXT_TS_BASE + stream.next() % TEXT_TS_SPAN;
    let score = stream.next() % TEXT_SCORE_LIMIT;
    let mut text = format!("{{\"id\":{id},\"ts\":{ts},\"score\":{score}");
    for field in TEXT_FIELDS.iter().cycle() {
        if text.len() >= len {
            break;
        }
        let word = stream.word();
        write!(text, ",\"{field}\":\"{word}\"").expect("writing to a String does not fail");
    }
    let mut bytes = text.into_bytes();
    bytes.truncate(len.saturating_sub(1));
    if len > 0 {
        bytes.push(b'}');
    }
    bytes
}

/// The fields of a JSON document that hold a few words each, written in
/// this order after its id, before its prose.
const JSON_SHORT_FIELDS: [&str; 6] = ["title", "company", "city", "category", "schedule", "pay"];

/// The words of a short field.
const JSON_SHORT_WORDS: u64 = 3;

/// The field of a JSON document that holds its prose marked up.
const JSON_HTML_FIELD: &str = "html_description";

/// The field of a JSON document that holds its prose bare, to its end.
const JSON_PROSE_FIELD: &str = "description";

/// The common words of a document's prose, drawn uniformly.
const JSON_COMMON_WORDS: [&str; 32] = [
    "the",
    "and",
    "of",
    "to",
    "a",
    "in",
    "for",
    "with",
    "on",
    "is",
    "you",
    "our",
    "will",
    "be",
    "as",
    "are",
    "team",
    "work",
    "this",
    "we",
    "your",
    "or",
    "an",
    "at",
    "by",
    "from",
    "experience",
    "skills",
    "role",
    "customer",
    "support",
    "required",
];

/// In 256ths, the share of a document's words that are common
/// (`JSON_COMMON_WORDS`); the rest are made-up words
/// (`JSON_MADE_UP_WORDS`). The share decides how far zstd compresses the
/// prose: at 90, the documents table's value pages of 10,800-byte
/// documents compress 4.4 times, about the 4.3 of measured documents.
const JSON_COMMON_SHARE: u64 = 90;

/// The consonants a made-up word's syllables start with.
const JSON_CONSONANTS: &[u8; 16] = b"bcdfghjklmnprstv";

/// The vowels a made-up word's syllables end with.
const JSON_VOWELS: &[u8; 4] = b"aeio";

/// The bits of a stream draw that pick a syllable's consonant.
const JSON_CONSONANT_BITS: u32 = JSON_CONSONANTS.len().trailing_zeros();

/// The bits of a stream draw one syllable takes, its consonant's and its
/// vowel's.
const JSON_SYLLABLE_BITS: u32 = JSON_CONSONANT_BITS + JSON_VOWELS.len().trailing_zeros();

/// The made-up words of a document's prose, each the same syllables
/// wherever it is drawn, so a page repeats them as prose repeats its
/// vocabulary.
const JSON_MADE_UP_WORDS: u64 = 4096;

/// The most syllables of a made-up word; it has from one to this many.
const JSON_MOST_SYLLABLES: u64 = 3;

/// The `len`-byte JSON document stored for `key` in a table of documents:
/// a 32-bit id, then `JSON_SHORT_FIELDS` of `JSON_SHORT_WORDS` words each,
/// then one passage of prose twice, as a document of a listing holds its
/// description once marked up and once plain: `JSON_HTML_FIELD` with the
/// passage in paragraph tags, then `JSON_PROSE_FIELD` with it bare, cut
/// where the document reaches `len` bytes and closed with a brace. A word
/// is common at `JSON_COMMON_SHARE` or one of `JSON_MADE_UP_WORDS` made of
/// random syllables. Drawn from the key's xorshift stream, so it is
/// deterministic and distinct for neighboring keys.
pub fn json_value_for_len(key: &Key, len: usize) -> Vec<u8> {
    let mut stream = Stream::from_key(key);
    let id = stream.next() & u64::from(u32::MAX);
    let mut text = format!("{{\"id\":{id}");
    for field in JSON_SHORT_FIELDS {
        write!(text, ",\"{field}\":\"").expect("writing to a String does not fail");
        for word in 0..JSON_SHORT_WORDS {
            if word > 0 {
                text.push(' ');
            }
            stream.json_word(&mut text);
        }
        text.push('"');
    }
    let mut passage = String::new();
    while text.len() + 2 * passage.len() < len {
        stream.json_word(&mut passage);
        passage.push(' ');
    }
    write!(
        text,
        ",\"{JSON_HTML_FIELD}\":\"<div><p>{passage}</p></div>\",\"{JSON_PROSE_FIELD}\":\"{passage}\""
    )
    .expect("writing to a String does not fail");
    while text.len() < len {
        text.push(' ');
    }
    let mut bytes = text.into_bytes();
    bytes.truncate(len.saturating_sub(1));
    if len > 0 {
        bytes.push(b'}');
    }
    bytes
}

/// A key's xorshift64 stream: seeded from the key (a long's bits, or FNV-1a
/// over the key's bytes) spread by `SEED_MULTIPLIER`.
pub(crate) struct Stream {
    state: u64,
}

impl Stream {
    pub(crate) fn from_key(key: &Key) -> Self {
        let seed = match key {
            Key::Long(value) => u64::from_ne_bytes(value.to_ne_bytes()),
            Key::Str(_) | Key::Bytes(_) | Key::Uuid(_) => fnv1a(&key.as_bytes()),
        };
        Self {
            state: seed.wrapping_mul(SEED_MULTIPLIER) | 1,
        }
    }

    /// The stream of `key` under `version`, apart from its first version's.
    pub(crate) fn from_key_at(key: &Key, version: u64) -> Self {
        let mut stream = Self::from_key(key);
        stream.state = (stream.state ^ version.wrapping_mul(SEED_MULTIPLIER)) | 1;
        stream
    }

    /// The next state of the stream.
    pub(crate) fn next(&mut self) -> u64 {
        self.state ^= self.state << XORSHIFT_FIRST_LEFT;
        self.state ^= self.state >> XORSHIFT_RIGHT;
        self.state ^= self.state << XORSHIFT_SECOND_LEFT;
        self.state
    }

    /// Appends a word of a JSON document's prose to `text`: common at
    /// `JSON_COMMON_SHARE`, else one of `JSON_MADE_UP_WORDS` drawn
    /// uniformly, each one to `JSON_MOST_SYLLABLES` syllables of a
    /// consonant and a vowel fixed by the word's own hash.
    fn json_word(&mut self, text: &mut String) {
        let draw = self.next();
        let byte_mask = u64::from(u8::MAX);
        if draw & byte_mask < JSON_COMMON_SHARE {
            let words = u64::try_from(JSON_COMMON_WORDS.len()).expect("a few dozen words");
            let at = (draw >> u8::BITS) % words;
            text.push_str(
                JSON_COMMON_WORDS[usize::try_from(at).expect("an index below the word count")],
            );
            return;
        }
        let word = (draw >> u8::BITS) % JSON_MADE_UP_WORDS;
        let mut bits = splitmix64(word);
        let syllables = 1 + bits % JSON_MOST_SYLLABLES;
        bits >>= u8::BITS;
        let pick = |letters: &[u8], bits: u64| {
            let count = u64::try_from(letters.len()).expect("a handful of letters");
            char::from(letters[usize::try_from(bits % count).expect("an index below the count")])
        };
        for _ in 0..syllables {
            text.push(pick(JSON_CONSONANTS, bits));
            text.push(pick(JSON_VOWELS, bits >> JSON_CONSONANT_BITS));
            bits >>= JSON_SYLLABLE_BITS;
        }
    }

    /// A word of `TEXT_WORDS`: the lesser of two uniform draws, so the word
    /// at index `i` of `n` comes up with probability `(2(n - i) - 1) / n²`,
    /// from about one in eight for the first to one in 256 for the last.
    fn word(&mut self) -> &'static str {
        let words = u64::try_from(TEXT_WORDS.len()).expect("a handful of words");
        let draw = self.next();
        let first = draw % words;
        let second = (draw >> u32::BITS) % words;
        TEXT_WORDS[usize::try_from(first.min(second)).expect("an index below the word count")]
    }
}

/// FNV-1a over `bytes`.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ids spanning several orders of magnitude, so the order test crosses
    /// digit and byte boundaries.
    const SAMPLE_IDS: [u64; 12] = [
        0, 1, 2, 9, 10, 99, 100, 999, 1_000, 65_535, 65_536, 1_000_000,
    ];

    /// The padding the padded cases use.
    const PAD: usize = 3;

    /// The length the sized value test asks for.
    const OTHER_LEN: usize = 17;

    /// Every key shape.
    const SHAPES: [KeyShape; 2] = [KeyShape::Sequential, KeyShape::Scattered];

    /// An odd multiplier that visits the scattered id space out of order.
    const SCRAMBLE: u64 = 0x2545_f491_4f6c_dd1d;

    /// The padding each type admits: none for the fixed-width types.
    fn pads(key_type: KeyType) -> Vec<usize> {
        if key_type.is_fixed_width() {
            vec![0]
        } else {
            vec![0, PAD]
        }
    }

    #[test]
    fn test_key_at_shaped_is_strictly_increasing_per_type_and_shape() {
        for shape in SHAPES {
            for key_type in KeyType::ALL {
                for pad in pads(key_type) {
                    let keys: Vec<Key> = SAMPLE_IDS
                        .iter()
                        .map(|&id| key_at_shaped(id, key_type, pad, shape))
                        .collect();
                    assert!(
                        keys.windows(2).all(|pair| pair[0] < pair[1]),
                        "{shape:?} {key_type} pad {pad}: {keys:?}"
                    );
                    assert!(keys.iter().all(|key| key.key_type() == key_type));
                }
            }
        }
    }

    #[test]
    fn test_key_at_is_key_at_padded_with_no_pad() {
        for key_type in KeyType::ALL {
            for id in SAMPLE_IDS {
                assert_eq!(key_at(id, key_type), key_at_padded(id, key_type, 0));
            }
        }
    }

    #[test]
    fn test_key_at_shaped_scatters_string_keys_alone() {
        for key_type in KeyType::ALL {
            for pad in pads(key_type) {
                for id in SAMPLE_IDS {
                    let sequential = key_at_shaped(id, key_type, pad, KeyShape::Sequential);
                    let scattered = key_at_shaped(id, key_type, pad, KeyShape::Scattered);
                    assert_eq!(sequential, key_at_padded(id, key_type, pad));
                    if key_type == KeyType::String {
                        assert_eq!(scattered, scattered_key_at(id, pad));
                    } else {
                        assert_eq!(scattered, sequential);
                    }
                }
            }
        }
    }

    #[test]
    fn test_absent_key_from_is_above_its_key_and_not_a_fixture_key() {
        for shape in SHAPES {
            for key_type in KeyType::ALL {
                for pad in pads(key_type) {
                    for id in SAMPLE_IDS {
                        let key = key_at_shaped(id, key_type, pad, shape);
                        let absent = absent_key_from(&key);
                        assert!(absent > key, "{key} < {absent}");
                        assert!(absent < key_at_shaped(id + 1, key_type, pad, shape));
                        assert_eq!(id_of(&absent, pad, shape), None, "{absent}");
                        assert_eq!(absent.key_type(), key_type);
                    }
                }
            }
        }
    }

    #[test]
    fn test_absent_key_near_is_absent_key_from_key_at() {
        for key_type in KeyType::ALL {
            assert_eq!(
                absent_key_near(SAMPLE_IDS[3], key_type),
                absent_key_from(&key_at(SAMPLE_IDS[3], key_type))
            );
        }
    }

    #[test]
    fn test_far_key_is_above_every_sample_key() {
        let top = SCATTERED_ID_SPACE - 1;
        for shape in SHAPES {
            for key_type in KeyType::ALL {
                let far = far_key(key_type);
                assert_eq!(far.key_type(), key_type);
                for pad in pads(key_type) {
                    for id in SAMPLE_IDS.into_iter().chain([top]) {
                        let key = key_at_shaped(id, key_type, pad, shape);
                        assert!(far > key, "{far} <= {key}");
                        assert!(far > absent_key_from(&key));
                    }
                }
            }
        }
    }

    #[test]
    fn test_id_of_inverts_key_at_shaped() {
        for shape in SHAPES {
            for key_type in KeyType::ALL {
                for pad in pads(key_type) {
                    for id in SAMPLE_IDS {
                        let key = key_at_shaped(id, key_type, pad, shape);
                        assert_eq!(id_of(&key, pad, shape), Some(id), "{key}");
                    }
                }
            }
            assert_eq!(
                id_of(&key_at_shaped(1, KeyType::String, PAD, shape), 0, shape),
                None
            );
            assert_eq!(id_of(&far_key(KeyType::String), 0, shape), None);
        }
        let sequential = KeyShape::Sequential;
        assert_eq!(
            id_of(&Key::Long(-KEY_STEP.cast_signed()), 0, sequential),
            None
        );
        assert_eq!(id_of(&far_key(KeyType::Uuid), 0, sequential), None);
        let scattered = KeyShape::Scattered;
        let other_shape = key_at_padded(SAMPLE_IDS[4], KeyType::String, 0);
        assert_eq!(id_of(&other_shape, 0, scattered), None);
        let other_shape = scattered_key_at(SAMPLE_IDS[4], 0);
        assert_eq!(id_of(&other_shape, 0, sequential), None);
    }

    #[test]
    fn test_scattered_key_at_is_strictly_increasing_over_the_whole_id_space() {
        let mut previous = scattered_key_at(0, 0);
        for id in 1..SCATTERED_ID_SPACE {
            let key = scattered_key_at(id, 0);
            assert!(key > previous, "{previous} >= {key} at id {id}");
            previous = key;
        }
    }

    #[test]
    fn test_scattered_key_order_is_id_order() {
        let sample = 10_000;
        let mut ids: Vec<u64> = (0..sample)
            .map(|n: u64| n.wrapping_mul(SCRAMBLE) % SCATTERED_ID_SPACE)
            .collect();
        let mut keys: Vec<(Key, u64)> = ids
            .iter()
            .map(|&id| (scattered_key_at(id, 0), id))
            .collect();
        keys.sort();
        ids.sort_unstable();
        let by_key: Vec<u64> = keys.into_iter().map(|(_, id)| id).collect();
        assert_eq!(by_key, ids);
    }

    #[test]
    fn test_scattered_key_is_the_fixed_length_in_the_alphabet() {
        for pad in [0, PAD] {
            for id in SAMPLE_IDS {
                let Key::Str(text) = scattered_key_at(id, pad) else {
                    panic!("a scattered key is a string");
                };
                assert_eq!(text.len(), pad + SCATTERED_KEY_FIXED_BYTES, "{text}");
                let digits = &text[pad..pad + SCATTERED_CHARS as usize];
                assert!(
                    digits
                        .bytes()
                        .all(|byte| SCATTERED_ALPHABET.contains(&byte)),
                    "{text}"
                );
                assert!(text.ends_with(SCATTERED_SUFFIX), "{text}");
            }
        }
    }

    /// Adjacent keys of one bucket of the benchmark's scattered table (every
    /// `buckets`-th id, as many as a file holds) share no more than the
    /// first few characters, as sorted random identifiers do; the
    /// sequential keys of the same ids share all but the last few.
    #[test]
    fn test_scattered_keys_in_a_bucket_share_few_leading_characters() {
        let buckets = 32;
        let rows_per_file = 37_500;
        let bucket: Vec<u64> = (0..rows_per_file).map(|n| n * buckets).collect();
        let shared = |shape| {
            let keys: Vec<Vec<u8>> = bucket
                .iter()
                .map(|&id| {
                    key_at_shaped(id, KeyType::String, 0, shape)
                        .as_bytes()
                        .to_vec()
                })
                .collect();
            keys.windows(2)
                .map(|pair| common_prefix(&pair[0], &pair[1]))
                .max()
                .unwrap()
        };
        assert!(
            shared(KeyShape::Scattered) <= 3,
            "{}",
            shared(KeyShape::Scattered)
        );
        assert!(
            shared(KeyShape::Sequential) >= STRING_KEY_FIXED_BYTES - 3,
            "{}",
            shared(KeyShape::Sequential)
        );
    }

    #[test]
    fn test_scattered_id_of_refuses_a_changed_character() {
        let id = SAMPLE_IDS[7];
        let Key::Str(text) = scattered_key_at(id, 0) else {
            panic!("a scattered key is a string");
        };
        let last = SCATTERED_CHARS as usize - 1;
        let mut bytes = text.into_bytes();
        bytes[last] = if bytes[last] == SCATTERED_ALPHABET[0] {
            SCATTERED_ALPHABET[1]
        } else {
            SCATTERED_ALPHABET[0]
        };
        let changed = Key::Str(String::from_utf8(bytes).unwrap());
        assert_eq!(id_of(&changed, 0, KeyShape::Scattered), None, "{changed}");
    }

    /// The length of the longest common prefix of `a` and `b`.
    fn common_prefix(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b).take_while(|(x, y)| x == y).count()
    }

    #[test]
    fn test_value_for_is_deterministic_and_distinct_for_neighbors() {
        for key_type in KeyType::ALL {
            for id in SAMPLE_IDS {
                let key = key_at(id, key_type);
                let value = value_for(&key);
                assert_eq!(value.len(), VALUE_BYTES);
                assert_eq!(value, value_for(&key));
                assert_ne!(value, value_for(&key_at(id + 1, key_type)));
                assert_ne!(value, value_for(&absent_key_from(&key)));
            }
        }
    }

    #[test]
    fn test_text_value_has_the_length_and_is_deterministic_and_distinct_for_neighbors() {
        for key_type in [KeyType::Long, KeyType::String] {
            for id in SAMPLE_IDS {
                let key = key_at(id, key_type);
                let value = text_value_for_len(&key, VALUE_BYTES);
                assert_eq!(value.len(), VALUE_BYTES);
                assert_eq!(value, text_value_for_len(&key, VALUE_BYTES));
                assert_ne!(
                    value,
                    text_value_for_len(&key_at(id + 1, key_type), VALUE_BYTES)
                );
                assert!(value.starts_with(b"{\"id\":"), "{value:?}");
                assert_eq!(value.last(), Some(&b'}'));
            }
        }
        let key = key_at(SAMPLE_IDS[5], KeyType::Long);
        assert_eq!(
            value_of_kind(&key, VALUE_BYTES, ValueKind::Text),
            text_value_for_len(&key, VALUE_BYTES)
        );
        assert_eq!(
            value_of_kind(&key, VALUE_BYTES, ValueKind::Random),
            value_for(&key)
        );
        assert!(text_value_for_len(&key, 0).is_empty());
    }

    #[test]
    fn test_json_value_has_the_length_and_is_deterministic_and_distinct_for_neighbors() {
        let len = 10_800;
        for id in SAMPLE_IDS {
            let key = scattered_key_at(id, 0);
            let value = json_value_for_len(&key, len);
            assert_eq!(value.len(), len);
            assert_eq!(value, json_value_for_len(&key, len));
            assert_ne!(value, json_value_for_len(&scattered_key_at(id + 1, 0), len));
            assert!(value.starts_with(b"{\"id\":"), "{value:?}");
            assert_eq!(value.last(), Some(&b'}'));
            let text = String::from_utf8(value).unwrap();
            assert!(text.contains("\"description\":\""), "{text}");
        }
        let key = scattered_key_at(SAMPLE_IDS[5], 0);
        assert_eq!(
            value_of_kind(&key, len, ValueKind::Json),
            json_value_for_len(&key, len)
        );
        assert!(json_value_for_len(&key, 0).is_empty());
    }

    #[test]
    fn test_value_for_len_has_the_length_and_shares_the_prefix() {
        let key = key_at(SAMPLE_IDS[5], KeyType::Long);
        let short = value_for_len(&key, OTHER_LEN);
        assert_eq!(short.len(), OTHER_LEN);
        assert_eq!(&value_for(&key)[..OTHER_LEN], &short[..]);
    }
}
