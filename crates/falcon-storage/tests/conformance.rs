//! The cache's storage guarantees — chiefly what it deliberately does *not* do.

use falcon_storage::CacheEngine;

const CAP: usize = 64 * 1024 * 1024;

#[test]
fn a_fresh_cache_never_sees_prior_data() {
    // Not a literal restart test (there is no persistence to reload from) —
    // this documents the guarantee. Cache contents are regenerable, so nothing
    // is written to disk and a new process always starts empty.
    let engine1 = CacheEngine::new(CAP, 8);
    engine1.put(b"a", b"1");
    assert!(engine1.get_shared(b"a").is_some());
    drop(engine1);

    let engine2 = CacheEngine::new(CAP, 8);
    assert!(
        engine2.get_shared(b"a").is_none(),
        "a fresh cache must start empty — the cache persists nothing"
    );
}

#[test]
fn basic_read_write_delete_contract() {
    let engine = CacheEngine::new(CAP, 8);
    assert!(engine.get_shared(b"foo").is_none());

    engine.put(b"foo", b"bar");
    assert_eq!(
        engine.get_shared(b"foo").map(|v| v.to_vec()),
        Some(b"bar".to_vec())
    );

    // Overwrite replaces rather than accumulates.
    engine.put(b"foo", b"baz");
    assert_eq!(
        engine.get_shared(b"foo").map(|v| v.to_vec()),
        Some(b"baz".to_vec())
    );

    assert!(
        engine.delete(b"foo"),
        "deleting a live key reports it was present"
    );
    assert!(engine.get_shared(b"foo").is_none());
    assert!(
        !engine.delete(b"foo"),
        "deleting an absent key reports false"
    );
}
