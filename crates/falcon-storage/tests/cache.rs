//! Behavioural tests for the Falcon Cache engine.
//!
//! The properties under test are the ones the design exists to guarantee: a
//! hard RAM bound under sustained write pressure (the OOM fix), TTL handled
//! inside the entry, sampled-LRU recency, and that every removal is reported.

use falcon_storage::CacheEngine;
use std::sync::Arc;

const MB: usize = 1024 * 1024;

/// Read a key as an owned `Vec`, so assertions read naturally against literals.
fn got(cache: &CacheEngine, key: &[u8]) -> Option<Vec<u8>> {
    cache.get_shared(key).map(|v| v.to_vec())
}

#[tokio::test]
async fn put_get_delete_roundtrip() {
    let cache = CacheEngine::new(8 * MB, 8);

    cache.put(b"k", b"v");
    assert_eq!(got(&cache, b"k"), Some(b"v".to_vec()));

    cache.put(b"k", b"v2");
    assert_eq!(got(&cache, b"k"), Some(b"v2".to_vec()));

    cache.delete(b"k");
    assert_eq!(got(&cache, b"k"), None);
}

#[tokio::test]
async fn missing_key_returns_none() {
    let cache = CacheEngine::new(8 * MB, 8);
    assert_eq!(got(&cache, b"absent"), None);
    assert_eq!(cache.stats().misses, 1);
}

/// The OOM fix, directly: sustained writes far exceeding the budget must leave
/// resident memory at or under the configured bound. This is the scenario that
/// killed the previous engine at 512 MB.
#[tokio::test]
async fn memory_stays_within_budget_under_sustained_writes() {
    let capacity = MB;
    let cache = CacheEngine::new(capacity, 8);
    let value = vec![b'v'; 1024];

    // Write ~20 MB of values into a 1 MB cache.
    for i in 0..20_000u32 {
        cache.put(format!("k:{i:08}").as_bytes(), &value);
    }

    let stats = cache.stats();
    assert!(
        stats.bytes as usize <= capacity,
        "resident bytes {} exceeded the {capacity}-byte budget",
        stats.bytes
    );
    assert!(stats.evictions > 0, "eviction should have run");
    // Keys are shed, not accumulated: the cache holds a working set, not
    // everything ever written.
    assert!((stats.keys as usize) < 20_000);
}

#[tokio::test]
async fn concurrent_writers_stay_within_budget() {
    let capacity = 2 * MB;
    let cache = Arc::new(CacheEngine::new(capacity, 8));

    let mut tasks = Vec::new();
    for t in 0..8u32 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            let value = vec![b'c'; 512];
            for i in 0..2000u32 {
                cache.put(format!("t{t}:k{i}").as_bytes(), &value);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let bytes = cache.stats().bytes as usize;
    assert!(
        bytes <= capacity,
        "resident bytes {bytes} exceeded {capacity} under concurrent load"
    );
}

/// A large value must not be able to push a shard past its budget just because
/// it arrived last.
#[tokio::test]
async fn oversized_values_do_not_breach_the_bound() {
    let capacity = MB;
    let cache = CacheEngine::new(capacity, 8);
    // Each value is a large fraction of one shard's budget (1 MB / 64 = 16 KB).
    let value = vec![b'x'; 32 * 1024];
    for i in 0..500u32 {
        cache.put(format!("big:{i}").as_bytes(), &value);
    }
    let bytes = cache.stats().bytes as usize;
    assert!(bytes <= capacity, "resident bytes {bytes} exceeded {capacity}");
}

#[tokio::test]
async fn expired_entry_is_not_served() {
    let cache = CacheEngine::new(8 * MB, 8);
    cache.put_with_expiry(b"stale", b"v", 1); // unix millis 1 — long gone
    assert_eq!(got(&cache, b"stale"), None);
    assert_eq!(cache.stats().expired, 1);

    // An expiry of 0 means "never".
    cache.put_with_expiry(b"forever", b"v", 0);
    assert_eq!(got(&cache, b"forever"), Some(b"v".to_vec()));
}

#[tokio::test]
async fn reaper_reclaims_expired_entries() {
    let cache = CacheEngine::new(8 * MB, 8);
    for i in 0..100u32 {
        cache.put_with_expiry(format!("e:{i}").as_bytes(), b"value", 1);
    }
    assert_eq!(cache.stats().keys, 100);

    cache.reap_expired();
    let stats = cache.stats();
    assert_eq!(stats.keys, 0, "reaper must drop every expired entry");
    assert_eq!(stats.bytes, 0, "and reclaim their bytes");
}

#[tokio::test]
async fn ttl_keys_are_tracked_without_a_parallel_map() {
    let cache = CacheEngine::new(8 * MB, 8);
    cache.put_with_expiry(b"a", b"v", 0); // no expiry
    cache.put_with_expiry(b"b", b"v", u64::MAX); // far future
    cache.put_with_expiry(b"c", b"v", u64::MAX);
    assert_eq!(cache.tracked_ttl_keys(), 2);
}

/// Sampled LRU should keep a key under active read traffic alive while cold
/// keys written after it are evicted around it.
#[tokio::test]
async fn recently_read_keys_survive_eviction() {
    // One shard's budget is small, so eviction runs constantly.
    let cache = CacheEngine::new(512 * 1024, 8);
    let value = vec![b'v'; 256];

    cache.put(b"hot-key", &value);
    let mut survived = 0;
    for i in 0..5000u32 {
        cache.put(format!("cold:{i}").as_bytes(), &value);
        // Keep touching the key so its access stamp stays recent.
        if got(&cache, b"hot-key").is_some() {
            survived += 1;
        }
    }
    // It need not survive every single round, but a continuously-read key
    // should be present for the overwhelming majority of them.
    assert!(
        survived > 4500,
        "a continuously-read key should resist eviction; survived {survived}/5000"
    );
}

/// Overwriting a key must not double-count its bytes.
#[tokio::test]
async fn overwrite_does_not_leak_accounting() {
    let cache = CacheEngine::new(8 * MB, 8);
    let big = vec![b'x'; 4096];
    for _ in 0..100 {
        cache.put(b"same", &big);
    }
    let stats = cache.stats();
    assert_eq!(stats.keys, 1);
    // One key's worth of bytes, not a hundred.
    assert!(
        (stats.bytes as usize) < 4096 * 2,
        "repeated overwrite leaked accounting: {} bytes for one key",
        stats.bytes
    );

    // Shrinking the value must shrink the accounting too.
    cache.put(b"same", b"tiny");
    assert!((cache.stats().bytes as usize) < 512);
}

#[tokio::test]
async fn delete_reclaims_bytes() {
    let cache = CacheEngine::new(8 * MB, 8);
    let value = vec![b'v'; 1024];
    for i in 0..100u32 {
        cache.put(format!("k:{i}").as_bytes(), &value);
    }
    assert!(cache.stats().bytes > 100 * 1024);

    for i in 0..100u32 {
        cache.delete(format!("k:{i}").as_bytes());
    }
    let stats = cache.stats();
    assert_eq!(stats.keys, 0);
    assert_eq!(stats.bytes, 0, "delete must reclaim every byte");
}

#[tokio::test]
async fn hit_rate_reflects_traffic() {
    let cache = CacheEngine::new(8 * MB, 8);
    cache.put(b"present", b"v");
    for _ in 0..3 {
        got(&cache, b"present");
    }
    got(&cache, b"absent");

    let stats = cache.stats();
    assert_eq!(stats.hits, 3);
    assert_eq!(stats.misses, 1);
    assert!((stats.hit_rate() - 0.75).abs() < f64::EPSILON);
}

/// Sampled LRU must prefer genuinely cold keys. Half the keyspace is read
/// continuously and half is written once and abandoned; the cold half should
/// absorb the overwhelming majority of evictions.
#[tokio::test]
async fn eviction_prefers_cold_keys_over_hot_ones() {
    let cache = CacheEngine::new(256 * 1024, 8);
    let value = vec![b'v'; 256];

    // A small hot set that is read on every round.
    for i in 0..20u32 {
        cache.put(format!("hot:{i}").as_bytes(), &value);
    }
    // Sustained cold traffic that must not displace the hot set.
    for i in 0..4000u32 {
        cache.put(format!("cold:{i}").as_bytes(), &value);
        for h in 0..20u32 {
            cache.get_shared(format!("hot:{h}").as_bytes());
        }
    }

    let mut hot_alive = 0;
    for i in 0..20u32 {
        if cache.get_shared(format!("hot:{i}").as_bytes()).is_some() {
            hot_alive += 1;
        }
    }
    assert!(
        hot_alive >= 18,
        "sampled LRU should keep the continuously-read set; {hot_alive}/20 survived"
    );
}

/// The engine must report every key it drops on its own initiative, or a
/// subscriber would go on believing in a key the cache no longer holds.
#[tokio::test]
async fn evictions_and_expiries_are_reported() {
    use falcon_storage::{EvictionListener, RemovalCause};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        seen: Mutex<Vec<(Vec<u8>, RemovalCause)>>,
    }
    impl EvictionListener for Recorder {
        fn on_removed(&self, key: &[u8], cause: RemovalCause) {
            self.seen.lock().unwrap().push((key.to_vec(), cause));
        }
    }

    let cache = CacheEngine::new(128 * 1024, 8);
    let recorder = Arc::new(Recorder::default());
    cache.set_eviction_listener(Arc::clone(&recorder) as Arc<dyn EvictionListener>);

    // Force eviction with sustained writes.
    let value = vec![b'v'; 512];
    for i in 0..2000u32 {
        cache.put(format!("k:{i}").as_bytes(), &value);
    }
    let evicted = recorder
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, c)| *c == RemovalCause::Evicted)
        .count();
    assert!(evicted > 0, "evictions must be reported");
    assert_eq!(
        evicted as u64,
        cache.stats().evictions,
        "every counted eviction must also be reported"
    );

    // And expiry, via the reaper.
    recorder.seen.lock().unwrap().clear();
    cache.put_with_expiry(b"doomed", b"v", 1);
    cache.reap_expired();
    let seen = recorder.seen.lock().unwrap();
    assert!(
        seen.iter().any(|(k, c)| k == b"doomed" && *c == RemovalCause::Expired),
        "expiry must be reported as Expired"
    );
}

/// Expiry discovered while sampling should be reported as such, not miscounted
/// as a capacity eviction — the two mean different things to an operator.
#[tokio::test]
async fn expired_entries_are_not_counted_as_evictions() {
    let cache = CacheEngine::new(8 * MB, 8);
    for i in 0..50u32 {
        cache.put_with_expiry(format!("e:{i}").as_bytes(), b"v", 1);
    }
    cache.reap_expired();
    let stats = cache.stats();
    assert_eq!(stats.expired, 50);
    assert_eq!(stats.evictions, 0, "TTL expiry is not a capacity eviction");
}

/// Deleting must not leave anything behind that grows over time.
#[tokio::test]
async fn repeated_insert_delete_does_not_grow() {
    let cache = CacheEngine::new(8 * MB, 8);
    for round in 0..1000u32 {
        cache.put(format!("k:{round}").as_bytes(), b"value");
        cache.delete(format!("k:{round}").as_bytes());
    }
    let stats = cache.stats();
    assert_eq!(stats.keys, 0);
    assert_eq!(stats.bytes, 0, "insert/delete churn must leave nothing behind");
}

/// Expiry is enforced on read, not only by the background sweep: a stale
/// value must never be served just because nothing has swept yet.
#[tokio::test]
async fn reads_never_serve_an_expired_value() {
    let cache = CacheEngine::new(8 * MB, 8);

    cache.put(b"k", b"v");
    assert_eq!(got(&cache, b"k"), Some(b"v".to_vec()));

    // An expiry already in the past (unix millis 1) is dead on arrival.
    cache.put_with_expiry(b"stale", b"v", 1);
    assert!(cache.get_shared(b"stale").is_none());

    // A miss and an expired entry are indistinguishable to the caller.
    assert!(cache.get_shared(b"absent").is_none());
}

/// The zero-copy read shares the engine's buffer rather than copying it.
#[tokio::test]
async fn shared_read_does_not_copy() {
    let cache = CacheEngine::new(8 * MB, 8);
    cache.put(b"k", b"value");

    let a = cache.get_shared(b"k").unwrap();
    let b = cache.get_shared(b"k").unwrap();
    // Both handles address the same allocation the engine holds.
    assert!(Arc::ptr_eq(&a, &b), "reads must share one allocation, not copy");
    assert_eq!(&*a, b"value");
}

/// Concurrent reads must scale with cores, not degrade.
///
/// This is a regression guard for a real architectural defect: when recency
/// lived in a plain field and the counters were process-global, every read took
/// its shard exclusively and hammered two shared cachelines, so throughput
/// *fell* as threads were added (17.2 M ops/s on one thread, 10.6 M on eight).
/// The threshold is deliberately loose — this asserts the shape (more cores do
/// not make it slower), not a throughput number that would be machine-specific
/// and flaky.
#[test]
fn concurrent_reads_scale_with_threads() {
    use std::time::Instant;

    let cache = Arc::new(CacheEngine::new(512 * 1024 * 1024, 8));
    let value = vec![b'v'; 128];
    let keys: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..10_000u32)
            .map(|i| format!("key:{i:08}").into_bytes())
            .collect(),
    );
    for k in keys.iter() {
        cache.put(k, &value);
    }

    let run = |threads: usize| -> f64 {
        const PER: u32 = 200_000;
        let start = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|tid| {
                let cache = Arc::clone(&cache);
                let keys = Arc::clone(&keys);
                std::thread::spawn(move || {
                    let off = (tid as u32) * 977;
                    for i in 0..PER {
                        std::hint::black_box(
                            cache.get_shared(&keys[((i + off) % 10_000) as usize]),
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        (PER as f64) * threads as f64 / start.elapsed().as_secs_f64()
    };

    let one = run(1);
    let four = run(4);
    assert!(
        four > one,
        "four threads ({four:.0} ops/s) must beat one ({one:.0} ops/s) — \
         reads are taking a shard exclusively or contending on a shared counter"
    );
}

/// Sharding must not starve eviction of candidates.
///
/// Shards are independent LRU populations, so splitting a small cache too
/// finely leaves each with almost nothing to choose between when sampling. At
/// one entry per shard, every insert evicts its predecessor and recency stops
/// working entirely — which is exactly what a fixed 512-shard count produced
/// for a 256 KB cache.
#[tokio::test]
async fn small_caches_still_evict_by_recency() {
    let cache = CacheEngine::new(256 * 1024, 8);
    let value = vec![b'v'; 256];

    for i in 0..10u32 {
        cache.put(format!("hot:{i}").as_bytes(), &value);
    }
    for i in 0..2000u32 {
        cache.put(format!("cold:{i}").as_bytes(), &value);
        for h in 0..10u32 {
            cache.get_shared(format!("hot:{h}").as_bytes());
        }
    }

    let mut alive = 0;
    for i in 0..10u32 {
        if cache.get_shared(format!("hot:{i}").as_bytes()).is_some() {
            alive += 1;
        }
    }
    assert!(
        alive >= 8,
        "a small cache must still honour recency; {alive}/10 of the hot set survived"
    );
}
