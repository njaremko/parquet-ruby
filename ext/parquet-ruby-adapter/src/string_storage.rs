use std::collections::{HashMap, HashSet};
use std::os::raw::{c_char, c_long};
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};

use magnus::value::{BoxValue, ReprValue};
use magnus::{RString, Ruby, Value};

/// Default cap on how many distinct strings the [`StringStorageMode::Shared`]
/// strategy will leak before returning frozen owned copies (overridable via the
/// shared budget).
///
/// `Shared` hands Ruby a zero-copy view into Rust-owned bytes, which requires
/// those bytes to live for the entire (unbounded) lifetime of the Ruby string,
/// i.e. `'static`. We obtain `'static` by leaking one copy per distinct value
/// into a process-wide registry. The registry is shared by all reads, so
/// repeated `each_row`/`each_column` calls reuse the same leaked values. The
/// requested budget bounds how many values the current read may return this way
/// and how many new process-wide leaks that read may admit; hard process
/// ceilings below bound the registry even when callers request larger budgets.
pub const DEFAULT_SHARED_MAX_ENTRIES: usize = 8192;

/// Default cap on the size of an individual string [`StringStorageMode::Shared`]
/// will leak (overridable per read). Longer values are returned as a frozen
/// owned copy rather than leaked, so a column of large blobs cannot blow the
/// leak budget. `Shared` targets short, repeated, low-cardinality strings (enums,
/// categories, codes); large values gain little from zero-copy and would
/// dominate the leak, so they opt out.
pub const DEFAULT_SHARED_MAX_VALUE_BYTES: usize = 4096;

/// Hard process-wide entry ceiling for `:shared`, regardless of user-supplied
/// budgets. This keeps a single large requested budget from making the leak
/// table unbounded. The default budget is still much smaller, but callers can
/// explicitly request more up to this ceiling.
const SHARED_PROCESS_MAX_ENTRIES: usize = 65_536;

/// Hard process-wide byte ceiling for leaked `:shared` string buffers. This
/// bounds the data plane independently from hash-table overhead and from any
/// single caller's requested budget.
const SHARED_PROCESS_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Per-read cache for shared values that could not use the process registry.
/// It avoids repeating the global lock for known process-cap fallbacks while
/// staying bounded for high-cardinality data.
const SHARED_FALLBACK_CACHE_ENTRY_COUNT_MAX: usize = 8192;
const SHARED_FALLBACK_CACHE_RETAINED_BYTES_MAX: usize = 4 * 1024 * 1024;

/// Cache size bound for `:intern` *values*. Low-cardinality columns (the case
/// `:intern` targets) fit well within this, making their repeats allocation
/// free; higher-cardinality values past the bound become frozen owned copies
/// rather than adding more entries to Ruby's immortal intern table.
const INTERN_VALUE_CACHE_ENTRY_COUNT_MAX: usize = 8192;

/// Cache size bound for hash *keys* (struct field names). Field-name cardinality
/// is fixed by the schema and small; the bound is a defensive ceiling.
const KEY_CACHE_ENTRY_COUNT_MAX: usize = 4096;

/// How a Rust string value is materialized as a Ruby `String` when reading.
///
/// The choice trades per-value allocation against memory ownership:
/// - [`Copy`](Self::Copy) is always safe and produces independent, mutable strings.
/// - [`Intern`](Self::Intern) deduplicates equal values through Ruby's frozen
///   string table (Ruby owns the bytes); repeats reuse one immortal object.
/// - [`Shared`](Self::Shared) avoids the byte copy entirely by viewing leaked
///   `'static` Rust bytes; bounded per read and by process-wide ceilings (see
///   [`StringStorageConfig`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StringStorageMode {
    /// Allocate a fresh, mutable Ruby `String` (one allocation + byte copy) per
    /// value. This is the default and matches historical behavior.
    #[default]
    Copy,
    /// Deduplicate equal values through Ruby's interned (frozen) string table up
    /// to a bounded per-read cache. Values after that bound become frozen owned
    /// copies, so high-cardinality columns cannot keep growing Ruby's immortal
    /// intern table. Note: a transient copy still happens per value (even on a
    /// dedup hit), so this is not a per-value throughput win over `Copy`; it
    /// lowers retained footprint and GC pressure for low-cardinality /
    /// repeat-heavy columns.
    Intern,
    /// Zero byte-copy: equal values share leaked `'static` Rust bytes via a
    /// frozen static Ruby string. Strings are always frozen in this mode. Best
    /// for short, repeated, low-cardinality values. The shared budget bounds
    /// per-read entry count, per-value byte size, and new process leak admission;
    /// values past either bound are returned as a frozen owned copy. See
    /// [`StringStorageConfig`].
    Shared,
}

/// The reader's string-materialization configuration: the [`StringStorageMode`]
/// plus the budget that bounds [`StringStorageMode::Shared`] values for this read
/// and new process-wide leak admission. The budget is ignored by `Copy` and
/// `Intern`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StringStorageConfig {
    pub mode: StringStorageMode,
    pub shared_max_entries: usize,
    pub shared_max_value_bytes: usize,
}

impl Default for StringStorageConfig {
    fn default() -> Self {
        Self {
            mode: StringStorageMode::default(),
            shared_max_entries: DEFAULT_SHARED_MAX_ENTRIES,
            shared_max_value_bytes: DEFAULT_SHARED_MAX_VALUE_BYTES,
        }
    }
}

impl StringStorageConfig {
    /// A config for `mode` with the default shared budget.
    pub fn from_mode(mode: StringStorageMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }
}

impl FromStr for StringStorageMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "copy" => Ok(StringStorageMode::Copy),
            "intern" => Ok(StringStorageMode::Intern),
            "shared" => Ok(StringStorageMode::Shared),
            other => Err(format!(
                "Invalid string_storage: {} (expected :copy, :intern, or :shared)",
                other
            )),
        }
    }
}

impl std::fmt::Display for StringStorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            StringStorageMode::Copy => "copy",
            StringStorageMode::Intern => "intern",
            StringStorageMode::Shared => "shared",
        };
        f.write_str(name)
    }
}

/// Process-wide registry of leaked `'static` string bytes.
///
/// The registry performs no Ruby calls and is protected by a small mutex. Reads
/// only take the lock while checking/inserting a string; the hot Ruby object
/// creation path happens after the leaked slice is returned.
#[derive(Debug)]
struct SharedLeakRegistry {
    entries: HashSet<&'static str>,
    leaked_bytes: usize,
}

impl SharedLeakRegistry {
    fn new() -> Self {
        Self {
            entries: HashSet::new(),
            leaked_bytes: 0,
        }
    }

    fn intern(
        &mut self,
        s: &str,
        requested_max_entries: usize,
        requested_max_value_bytes: usize,
    ) -> Option<&'static str> {
        if let Some(&existing) = self.entries.get(s) {
            return Some(existing);
        }

        let entry_limit = requested_max_entries.min(SHARED_PROCESS_MAX_ENTRIES);
        let requested_byte_limit =
            requested_max_entries.saturating_mul(requested_max_value_bytes.saturating_add(1));
        let byte_limit = requested_byte_limit.min(SHARED_PROCESS_MAX_BYTES);
        let entry_bytes = s.len().checked_add(1)?;

        if self.entries.len() >= entry_limit {
            return None;
        }
        if self.leaked_bytes.saturating_add(entry_bytes) > byte_limit {
            return None;
        }

        let leaked = leak_nul_terminated(s);
        self.entries.insert(leaked);
        self.leaked_bytes += entry_bytes;

        debug_assert!(self.entries.len() <= SHARED_PROCESS_MAX_ENTRIES);
        debug_assert!(self.leaked_bytes <= SHARED_PROCESS_MAX_BYTES);
        Some(leaked)
    }
}

fn shared_leak_registry() -> &'static Mutex<SharedLeakRegistry> {
    static REGISTRY: OnceLock<Mutex<SharedLeakRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(SharedLeakRegistry::new()))
}

fn lock_shared_leak_registry() -> std::sync::MutexGuard<'static, SharedLeakRegistry> {
    shared_leak_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Deduplicating, bounded interner of leaked `'static` string bytes.
///
/// Each distinct string is leaked at most once process-wide (so equal values
/// reuse the same `'static` slice). Each read still enforces its own entry and
/// value-size budget before using the process registry; a larger earlier read
/// cannot make a later smaller read return out-of-budget zero-copy strings.
/// Values outside those bounds are returned by the caller as frozen owned copies.
#[derive(Debug)]
pub struct SharedLeakInterner {
    entries: HashSet<&'static str>,
    fallbacks: HashSet<Box<str>>,
    fallback_bytes: usize,
    max_entries: usize,
    max_value_bytes: usize,
}

impl SharedLeakInterner {
    /// Both limits must be positive; callers parse them from positive Integers.
    fn new(max_entries: usize, max_value_bytes: usize) -> Self {
        debug_assert!(max_entries > 0);
        debug_assert!(max_value_bytes > 0);
        Self {
            entries: HashSet::new(),
            fallbacks: HashSet::new(),
            fallback_bytes: 0,
            max_entries,
            max_value_bytes,
        }
    }

    /// Return a `'static` view of `s`, leaking one NUL-terminated copy when the
    /// current read and process registry both have room, or `None` when either
    /// bound is reached (caller then copies).
    fn intern(&mut self, s: &str) -> Option<&'static str> {
        if let Some(&existing) = self.entries.get(s) {
            return Some(existing);
        }
        if self.fallbacks.contains(s) {
            return None;
        }

        if s.len() > self.max_value_bytes {
            return None;
        }

        if self.entries.len() >= self.max_entries {
            self.remember_fallback(s);
            return None;
        }

        match lock_shared_leak_registry().intern(s, self.max_entries, self.max_value_bytes) {
            Some(leaked) => {
                self.entries.insert(leaked);
                debug_assert!(self.entries.len() <= self.max_entries);
                Some(leaked)
            }
            None => {
                self.remember_fallback(s);
                None
            }
        }
    }

    fn remember_fallback(&mut self, s: &str) {
        if s.len() > self.max_value_bytes {
            return;
        }
        if self.fallbacks.len() >= SHARED_FALLBACK_CACHE_ENTRY_COUNT_MAX {
            return;
        }
        if self.fallback_bytes.saturating_add(s.len()) > SHARED_FALLBACK_CACHE_RETAINED_BYTES_MAX {
            return;
        }
        if self.fallbacks.insert(Box::from(s)) {
            self.fallback_bytes += s.len();
        }
        debug_assert!(self.fallbacks.len() <= SHARED_FALLBACK_CACHE_ENTRY_COUNT_MAX);
        debug_assert!(self.fallback_bytes <= SHARED_FALLBACK_CACHE_RETAINED_BYTES_MAX);
    }
}

/// Leak one copy of `s` with a trailing NUL byte and return a `'static` view of
/// the content only (excluding the NUL).
///
/// The NUL terminator is mandatory: `rb_utf8_str_new_static` (used by
/// [`StringStorage::to_ruby_string`] for `Shared`) builds a string that points
/// at this buffer and relies on Ruby's invariant that `ptr[len] == '\0'`. The
/// boxed bytes are never freed, so the returned reference is genuinely `'static`.
fn leak_nul_terminated(s: &str) -> &'static str {
    let mut bytes = Vec::with_capacity(s.len() + 1);
    bytes.extend_from_slice(s.as_bytes());
    bytes.push(0);
    let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
    // SAFETY: the leading `s.len()` bytes are exactly `s`'s valid UTF-8 content;
    // only the trailing NUL is excluded from the returned slice.
    unsafe { std::str::from_utf8_unchecked(&leaked[..leaked.len() - 1]) }
}

/// Per-read string output: how string *values* are materialized (per the
/// configured mode) plus a cache that always interns hash *keys* (struct field
/// names). One `StringStorage` is created per `each_row`/`each_column`
/// invocation; its local caches are not shared across calls. In `Shared` mode,
/// value bytes are coordinated through the process-wide leak registry above.
#[derive(Debug)]
pub struct StringStorage {
    values: ValueStrategy,
    keys: InternCache,
}

impl StringStorage {
    pub fn new(config: StringStorageConfig) -> Self {
        Self {
            values: ValueStrategy::new(config),
            keys: InternCache::new(KEY_CACHE_ENTRY_COUNT_MAX),
        }
    }

    /// Materialize a string *value* per the configured mode. The result is
    /// frozen for `Intern`/`Shared` and mutable for `Copy`.
    pub fn ruby_string(&mut self, ruby: &Ruby, s: &str) -> Value {
        self.values.ruby_string(ruby, s)
    }

    /// Materialize a hash *key* (a struct field name). Keys are always interned
    /// and reused regardless of the value mode, because field names are a small
    /// set repeated on every row.
    pub fn ruby_key(&mut self, ruby: &Ruby, name: &str) -> Value {
        self.keys.intern_key(ruby, name)
    }
}

/// The value-materialization strategy (mode plus any per-mode state).
#[derive(Debug)]
enum ValueStrategy {
    Copy,
    Intern(InternCache),
    Shared(SharedLeakInterner),
}

impl ValueStrategy {
    fn new(config: StringStorageConfig) -> Self {
        match config.mode {
            StringStorageMode::Copy => ValueStrategy::Copy,
            StringStorageMode::Intern => {
                ValueStrategy::Intern(InternCache::new(INTERN_VALUE_CACHE_ENTRY_COUNT_MAX))
            }
            StringStorageMode::Shared => ValueStrategy::Shared(SharedLeakInterner::new(
                config.shared_max_entries,
                config.shared_max_value_bytes,
            )),
        }
    }

    fn ruby_string(&mut self, ruby: &Ruby, s: &str) -> Value {
        match self {
            ValueStrategy::Copy => ruby.str_new(s).as_value(),
            ValueStrategy::Intern(cache) => cache
                .intern_cached(ruby, s)
                .unwrap_or_else(|| frozen_copy(ruby, s)),
            ValueStrategy::Shared(interner) => match interner.intern(s) {
                Some(leaked) => unsafe { static_ruby_string(ruby, leaked) },
                // Past a leak bound: a frozen owned copy, so `Shared` results are
                // uniformly frozen and no extra memory is leaked.
                None => frozen_copy(ruby, s),
            },
        }
    }
}

/// Caches the interned Ruby string for each distinct content value, so a
/// repeated string is interned once and then returned with no further Ruby
/// allocation. Used both for hash keys (struct field names) and for `:intern`
/// values.
///
/// Each cached value is held in a [`BoxValue`], which registers the string with
/// Ruby's GC via `rb_gc_register_address`. That is required for correctness: a
/// plain `RString` stored in this Rust-heap map is invisible to the GC, and
/// `GC.compact` would relocate the interned string and leave the cached handle
/// dangling. `BoxValue` keeps the handle at a stable address that the GC updates
/// on compaction.
///
/// Bounded: at most `capacity` distinct values are cached. Value callers fall
/// back to frozen owned copies after that; key callers continue interning after
/// the cache because key cardinality is fixed by the schema and key identity is
/// part of the public read contract.
#[derive(Debug)]
struct InternCache {
    cache: HashMap<Box<str>, BoxValue<RString>>,
    capacity: usize,
}

impl InternCache {
    fn new(capacity: usize) -> Self {
        Self {
            cache: HashMap::new(),
            capacity,
        }
    }

    fn intern_cached(&mut self, ruby: &Ruby, s: &str) -> Option<Value> {
        if let Some(boxed) = self.cache.get(s) {
            return Some(boxed.as_value());
        }
        if self.cache.len() >= self.capacity {
            return None;
        }
        let interned = ruby.str_new(s).to_interned_str();
        self.cache.insert(Box::from(s), BoxValue::new(interned));
        debug_assert!(self.cache.len() <= self.capacity);
        Some(interned.as_value())
    }

    fn intern_key(&mut self, ruby: &Ruby, s: &str) -> Value {
        self.intern_cached(ruby, s).unwrap_or_else(|| {
            let interned = ruby.str_new(s).to_interned_str();
            interned.as_value()
        })
    }
}

/// Build a frozen, owned Ruby `String` (a normal copy that is then frozen).
fn frozen_copy(ruby: &Ruby, s: &str) -> Value {
    let string = ruby.str_new(s);
    string.freeze();
    string.as_value()
}

/// Build a frozen Ruby `String` that points directly at `bytes` without copying.
///
/// # Safety
/// `bytes` must remain valid and immutable for the entire lifetime of the
/// returned Ruby string. Because Ruby code may retain the string for an
/// unbounded time, this requires `bytes: &'static str`. The backing buffer must
/// additionally be NUL-terminated at `bytes.as_ptr()[bytes.len()]`, which Ruby's
/// static-string constructor requires; [`leak_nul_terminated`] guarantees this.
/// The returned string is frozen so the shared, immutable backing is never
/// mutated in place.
unsafe fn static_ruby_string(ruby: &Ruby, bytes: &'static str) -> Value {
    // The static-string constructor reads bytes[len] expecting a NUL; check the
    // byte we are about to rely on rather than trusting the caller's comment.
    debug_assert_eq!(
        *bytes.as_ptr().add(bytes.len()),
        0,
        "static_ruby_string requires a NUL terminator at bytes[len]"
    );
    let string = ruby.str_new_lit(bytes.as_ptr() as *const c_char, bytes.len() as c_long);
    string.freeze();
    string.as_value()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn shared_leak_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn reset_shared_leak_registry() {
        let mut registry = lock_shared_leak_registry();
        registry.entries.clear();
        registry.leaked_bytes = 0;
    }

    fn shared_leak_registry_len() -> usize {
        lock_shared_leak_registry().entries.len()
    }

    fn with_clean_shared_leak_registry(test: impl FnOnce()) {
        let _guard = shared_leak_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_shared_leak_registry();
        test();
        reset_shared_leak_registry();
    }

    #[test]
    fn interns_distinct_values_once_and_reuses_pointer() {
        with_clean_shared_leak_registry(|| {
            let mut interner =
                SharedLeakInterner::new(DEFAULT_SHARED_MAX_ENTRIES, DEFAULT_SHARED_MAX_VALUE_BYTES);

            let first = interner.intern("repeat").unwrap();
            let second = interner.intern("repeat").unwrap();
            let other = interner.intern("different").unwrap();

            // Equal values share the same leaked allocation.
            assert_eq!(first.as_ptr(), second.as_ptr());
            assert_eq!(first, "repeat");
            // Distinct values get distinct allocations with the right contents.
            assert_ne!(first.as_ptr(), other.as_ptr());
            assert_eq!(other, "different");
            assert_eq!(shared_leak_registry_len(), 2);
        });
    }

    #[test]
    fn leak_is_bounded_and_falls_back_past_the_cap() {
        with_clean_shared_leak_registry(|| {
            let max_entries = 4;
            let mut interner = SharedLeakInterner::new(max_entries, DEFAULT_SHARED_MAX_VALUE_BYTES);

            for index in 0..max_entries {
                assert!(interner.intern(&format!("value-{index}")).is_some());
            }
            assert_eq!(shared_leak_registry_len(), max_entries);

            // A new distinct value past the cap is not leaked; caller must copy.
            assert!(interner.intern("over-the-bound").is_none());
            assert_eq!(shared_leak_registry_len(), max_entries);

            // A value already interned still resolves even after the cap is hit.
            assert!(interner.intern("value-0").is_some());
            assert_eq!(shared_leak_registry_len(), max_entries);
        });
    }

    #[test]
    fn oversized_values_are_not_leaked() {
        with_clean_shared_leak_registry(|| {
            let mut interner =
                SharedLeakInterner::new(DEFAULT_SHARED_MAX_ENTRIES, DEFAULT_SHARED_MAX_VALUE_BYTES);

            let at_limit = "x".repeat(DEFAULT_SHARED_MAX_VALUE_BYTES);
            let over_limit = "x".repeat(DEFAULT_SHARED_MAX_VALUE_BYTES + 1);

            assert!(interner.intern(&at_limit).is_some());
            assert!(interner.intern(&over_limit).is_none());
            // Only the in-bound value was leaked.
            assert_eq!(shared_leak_registry_len(), 1);
            assert!(
                interner.fallbacks.is_empty(),
                "oversized fallbacks must not be retained in the per-read cache"
            );
            assert_eq!(interner.fallback_bytes, 0);
        });
    }

    #[test]
    fn fallback_cache_retained_bytes_are_bounded() {
        with_clean_shared_leak_registry(|| {
            let mut first = SharedLeakInterner::new(1, DEFAULT_SHARED_MAX_VALUE_BYTES);
            assert!(first.intern("already-leaked").is_some());

            let mut second = SharedLeakInterner::new(1, DEFAULT_SHARED_MAX_VALUE_BYTES);
            let suffix = "x".repeat(1024);
            for index in 0..(SHARED_FALLBACK_CACHE_ENTRY_COUNT_MAX * 2) {
                assert!(second.intern(&format!("{index:08}-{suffix}")).is_none());
            }

            assert!(second.fallbacks.len() <= SHARED_FALLBACK_CACHE_ENTRY_COUNT_MAX);
            assert!(second.fallback_bytes <= SHARED_FALLBACK_CACHE_RETAINED_BYTES_MAX);
        });
    }

    #[test]
    fn shared_leak_budget_is_process_wide_for_matching_budget() {
        with_clean_shared_leak_registry(|| {
            let mut first = SharedLeakInterner::new(4, DEFAULT_SHARED_MAX_VALUE_BYTES);
            for index in 0..4 {
                assert!(first.intern(&format!("reader-one-{index}")).is_some());
            }

            let mut second = SharedLeakInterner::new(4, DEFAULT_SHARED_MAX_VALUE_BYTES);
            assert!(second.intern("reader-one-0").is_some());
            assert!(
                second.intern("reader-two-new").is_none(),
                "the shared leak budget must not reset for each reader"
            );
        });
    }

    #[test]
    fn current_read_value_bound_applies_to_registry_hits() {
        with_clean_shared_leak_registry(|| {
            let value = "larger-than-second-budget";
            let mut first = SharedLeakInterner::new(4, value.len());
            assert!(first.intern(value).is_some());
            assert_eq!(shared_leak_registry_len(), 1);

            let mut second = SharedLeakInterner::new(4, value.len() - 1);
            assert!(second.intern(value).is_none());
            assert_eq!(second.entries.len(), 0);
            assert_eq!(shared_leak_registry_len(), 1);
        });
    }

    #[test]
    fn current_read_entry_bound_applies_to_registry_hits() {
        with_clean_shared_leak_registry(|| {
            let mut first = SharedLeakInterner::new(4, DEFAULT_SHARED_MAX_VALUE_BYTES);
            assert!(first.intern("already-leaked-one").is_some());
            assert!(first.intern("already-leaked-two").is_some());
            assert_eq!(shared_leak_registry_len(), 2);

            let mut second = SharedLeakInterner::new(1, DEFAULT_SHARED_MAX_VALUE_BYTES);
            assert!(second.intern("already-leaked-one").is_some());
            assert!(second.intern("already-leaked-two").is_none());
            assert_eq!(second.entries.len(), 1);
            assert_eq!(shared_leak_registry_len(), 2);
        });
    }

    #[test]
    fn mode_parses_and_round_trips() {
        for mode in [
            StringStorageMode::Copy,
            StringStorageMode::Intern,
            StringStorageMode::Shared,
        ] {
            assert_eq!(
                StringStorageMode::from_str(&mode.to_string()).unwrap(),
                mode
            );
        }
        assert_eq!(StringStorageMode::default(), StringStorageMode::Copy);
        assert!(StringStorageMode::from_str("nonsense").is_err());
    }
}
