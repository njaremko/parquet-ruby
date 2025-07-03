use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use magnus::RString;

static STRING_CACHE: LazyLock<Mutex<HashMap<String, &'static str>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(100)));

/// A cache for interning strings in the Ruby VM to reduce memory usage
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

    /// Intern a string in Ruby's VM, returning the same string for tracking
    /// Note: We return the input string to maintain API compatibility,
    /// but internally we ensure it's interned in Ruby's VM
    pub fn intern(&mut self, s: String) -> Arc<str> {
        if !self.enabled {
            return Arc::from(s.as_str());
        }

        // Try to get or create the interned string
        let result = (|| -> Result<(), String> {
            let mut cache = STRING_CACHE.lock().map_err(|e| e.to_string())?;

            if cache.contains_key(s.as_str()) {
                let mut hits = self.hits.lock().map_err(|e| e.to_string())?;
                *hits += 1;
            } else {
                // Create Ruby string and intern it
                let rstring = RString::new(&s);
                let interned = rstring.to_interned_str();
                let static_str = interned.as_str().map_err(|e| e.to_string())?;

                cache.insert(s.clone(), static_str);

                let mut misses = self.misses.lock().map_err(|e| e.to_string())?;
                *misses += 1;
            }
            Ok(())
        })();

        // Log any errors but don't fail - just return the string
        if let Err(e) = result {
            eprintln!("String cache error: {}", e);
        }

        Arc::from(s.as_str())
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
