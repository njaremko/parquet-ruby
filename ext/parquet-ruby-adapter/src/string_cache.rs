use std::collections::HashSet;
use triomphe::Arc;

/// Default cap on distinct strings retained for reuse when `string_cache: true`.
/// The cap is a deliberate memory bound (safety over hit rate): a high-cardinality
/// column cannot grow the cache without limit. Once full, further distinct strings
/// are not retained but already-cached values still reuse their shared storage.
/// Callers can override it (`string_cache: <Integer>`). Reported statistics expose
/// the cumulative miss count rather than pretending to know exact distinct
/// cardinality after the bounded cache fills.
pub const DEFAULT_STRING_CACHE_CAPACITY: usize = 100;

/// Hard capacity ceiling for `string_cache:`. Each retained entry owns hash-table
/// metadata plus one shared string allocation, so an explicit upper bound keeps a
/// caller-provided capacity from becoming an eager unbounded allocation.
pub const STRING_CACHE_CAPACITY_MAX: usize = 65_536;

/// Per-value byte ceiling for retained cache entries. Oversized values still
/// write correctly, but they are not retained for reuse across later rows.
pub const STRING_CACHE_VALUE_BYTES_MAX: usize = 4096;

/// Total retained UTF-8 bytes for cached string contents. This does not include
/// hash-table metadata, which is separately bounded by `STRING_CACHE_CAPACITY_MAX`.
pub const STRING_CACHE_RETAINED_BYTES_MAX: usize = 16 * 1024 * 1024;

/// A cache for reusing string storage to reduce memory usage
/// when there are many repeated strings
#[derive(Debug)]
pub struct StringCache {
    capacity: usize,
    entries: HashSet<Arc<str>>,
    retained_bytes: usize,
    hits: usize,
    misses: usize,
}

impl StringCache {
    /// Create a new string cache that retains at most `capacity` distinct
    /// strings. The caller only constructs a cache when caching is enabled; a
    /// disabled cache is represented by not creating one at all.
    pub fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        let capacity = capacity.min(STRING_CACHE_CAPACITY_MAX);

        Self {
            capacity,
            entries: HashSet::with_capacity(capacity),
            retained_bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Intern a string, returning shared storage for repeated values.
    pub fn intern(&mut self, s: String) -> Arc<str> {
        debug_assert!(self.entries.len() <= self.capacity);

        if let Some(interned) = self.entries.get(s.as_str()) {
            self.hits += 1;
            return Arc::clone(interned);
        }

        let interned = Arc::<str>::from(s);
        self.misses += 1;

        let retained_bytes_next = self.retained_bytes.saturating_add(interned.len());
        if self.entries.len() < self.capacity
            && interned.len() <= STRING_CACHE_VALUE_BYTES_MAX
            && retained_bytes_next <= STRING_CACHE_RETAINED_BYTES_MAX
        {
            self.retained_bytes = retained_bytes_next;
            self.entries.insert(Arc::clone(&interned));
        }

        debug_assert!(self.entries.len() <= self.capacity);
        debug_assert!(self.retained_bytes <= STRING_CACHE_RETAINED_BYTES_MAX);
        interned
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        debug_assert!(self.entries.len() <= self.capacity);

        CacheStats {
            size: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            hit_rate: if self.hits + self.misses > 0 {
                self.hits as f64 / (self.hits + self.misses) as f64
            } else {
                0.0
            },
        }
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.retained_bytes = 0;
        self.hits = 0;
        self.misses = 0;
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub size: usize,
    pub hits: usize,
    pub misses: usize,
    pub hit_rate: f64,
}

#[cfg(test)]
#[path = "./string_cache_test.rs"]
mod tests;
