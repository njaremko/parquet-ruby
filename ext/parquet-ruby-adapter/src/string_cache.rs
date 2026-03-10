use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex};

static STRING_CACHE: LazyLock<Mutex<HashSet<Arc<str>>>> =
    LazyLock::new(|| Mutex::new(HashSet::with_capacity(100)));

/// A cache for interning strings to reduce memory usage
/// when there are many repeated strings
#[derive(Debug)]
pub struct StringCache {
    enabled: bool,
    hits: Arc<Mutex<usize>>,
    misses: Arc<Mutex<usize>>,
}

impl StringCache {
    /// Create a new string cache
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            hits: Arc::new(Mutex::new(0)),
            misses: Arc::new(Mutex::new(0)),
        }
    }

    /// Intern a string, returning a shared reference.
    /// Deduplicates repeated strings via an internal cache.
    pub fn intern(&mut self, s: String) -> Arc<str> {
        if !self.enabled {
            return Arc::from(s.as_str());
        }

        let result = (|| -> Result<Arc<str>, String> {
            let mut cache = STRING_CACHE.lock().map_err(|e| e.to_string())?;

            if let Some(cached) = cache.get(s.as_str()) {
                let mut hits = self.hits.lock().map_err(|e| e.to_string())?;
                *hits += 1;
                Ok(Arc::clone(cached))
            } else {
                let arc: Arc<str> = Arc::from(s.as_str());
                cache.insert(Arc::clone(&arc));

                let mut misses = self.misses.lock().map_err(|e| e.to_string())?;
                *misses += 1;
                Ok(arc)
            }
        })();

        match result {
            Ok(arc) => arc,
            Err(e) => {
                eprintln!("String cache error: {}", e);
                Arc::from(s.as_str())
            }
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let cache_size = STRING_CACHE.lock().map(|c| c.len()).unwrap_or(0);
        let hits = self.hits.lock().map(|h| *h).unwrap_or(0);
        let misses = self.misses.lock().map(|m| *m).unwrap_or(0);

        CacheStats {
            enabled: self.enabled,
            size: cache_size,
            hits,
            misses,
            hit_rate: if hits + misses > 0 {
                hits as f64 / (hits + misses) as f64
            } else {
                0.0
            },
        }
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        if let Ok(mut cache) = STRING_CACHE.lock() {
            cache.clear();
        }
        if let Ok(mut hits) = self.hits.lock() {
            *hits = 0;
        }
        if let Ok(mut misses) = self.misses.lock() {
            *misses = 0;
        }
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub enabled: bool,
    pub size: usize,
    pub hits: usize,
    pub misses: usize,
    pub hit_rate: f64,
}
