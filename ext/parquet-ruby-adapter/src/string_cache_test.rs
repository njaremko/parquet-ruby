use super::{
    StringCache, DEFAULT_STRING_CACHE_CAPACITY, STRING_CACHE_CAPACITY_MAX,
    STRING_CACHE_RETAINED_BYTES_MAX, STRING_CACHE_VALUE_BYTES_MAX,
};
use triomphe::Arc;

#[test]
fn cache_reuses_storage_and_counts_hits() {
    let mut cache = StringCache::new(DEFAULT_STRING_CACHE_CAPACITY);

    let first = cache.intern("repeat".to_string());
    let second = cache.intern("repeat".to_string());

    assert!(Arc::ptr_eq(&first, &second));

    let stats = cache.stats();
    assert_eq!(stats.size, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hit_rate, 0.5);
}

#[test]
fn cache_stats_are_instance_local() {
    let mut first_cache = StringCache::new(DEFAULT_STRING_CACHE_CAPACITY);
    let mut second_cache = StringCache::new(DEFAULT_STRING_CACHE_CAPACITY);

    let first_value = first_cache.intern("shared".to_string());
    let first_value_again = first_cache.intern("shared".to_string());
    let second_value = second_cache.intern("shared".to_string());

    assert!(Arc::ptr_eq(&first_value, &first_value_again));
    assert!(!Arc::ptr_eq(&first_value, &second_value));

    let first_stats = first_cache.stats();
    assert_eq!(first_stats.size, 1);
    assert_eq!(first_stats.hits, 1);
    assert_eq!(first_stats.misses, 1);

    let second_stats = second_cache.stats();
    assert_eq!(second_stats.size, 1);
    assert_eq!(second_stats.hits, 0);
    assert_eq!(second_stats.misses, 1);
}

#[test]
fn cache_retention_is_bounded() {
    let mut cache = StringCache::new(DEFAULT_STRING_CACHE_CAPACITY);

    for index in 0..DEFAULT_STRING_CACHE_CAPACITY {
        cache.intern(format!("value-{index}"));
    }

    let first_uncached = cache.intern("outside-bound".to_string());
    let second_uncached = cache.intern("outside-bound".to_string());

    assert!(!Arc::ptr_eq(&first_uncached, &second_uncached));

    let stats = cache.stats();
    assert_eq!(stats.size, DEFAULT_STRING_CACHE_CAPACITY);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, DEFAULT_STRING_CACHE_CAPACITY + 2);
}

#[test]
fn cache_capacity_has_a_hard_ceiling() {
    let cache = StringCache::new(STRING_CACHE_CAPACITY_MAX + 1);

    assert_eq!(cache.capacity, STRING_CACHE_CAPACITY_MAX);
}

#[test]
fn oversized_values_are_not_retained() {
    let mut cache = StringCache::new(DEFAULT_STRING_CACHE_CAPACITY);
    let value = "x".repeat(STRING_CACHE_VALUE_BYTES_MAX + 1);

    let first = cache.intern(value.clone());
    let second = cache.intern(value);
    let stats = cache.stats();

    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(stats.size, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 2);
    assert_eq!(cache.retained_bytes, 0);
}

#[test]
fn retained_bytes_are_bounded() {
    let mut cache = StringCache::new(STRING_CACHE_CAPACITY_MAX);
    let value_len = STRING_CACHE_VALUE_BYTES_MAX;
    let retained_entry_count_max = STRING_CACHE_RETAINED_BYTES_MAX / value_len;

    for index in 0..retained_entry_count_max {
        cache.intern(format!("{index:08}-{}", "x".repeat(value_len - 9)));
    }

    assert_eq!(cache.retained_bytes, STRING_CACHE_RETAINED_BYTES_MAX);

    let overflow = cache.intern("y".repeat(value_len));
    let overflow_again = cache.intern("y".repeat(value_len));
    let stats = cache.stats();

    assert!(!Arc::ptr_eq(&overflow, &overflow_again));
    assert_eq!(stats.size, retained_entry_count_max);
    assert_eq!(cache.retained_bytes, STRING_CACHE_RETAINED_BYTES_MAX);
}

#[test]
fn clear_removes_entries_and_resets_counts() {
    let mut cache = StringCache::new(DEFAULT_STRING_CACHE_CAPACITY);

    cache.intern("repeat".to_string());
    cache.intern("repeat".to_string());
    cache.clear();

    let stats = cache.stats();
    assert_eq!(stats.size, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.hit_rate, 0.0);
}
