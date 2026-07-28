use falcon_core::{Config, Node};
use std::time::Duration;

fn test_node() -> Node {
    Node::build(Config::default()).unwrap()
}

fn value(v: Option<std::sync::Arc<[u8]>>) -> Option<Vec<u8>> {
    v.map(|v| v.to_vec())
}

#[tokio::test]
async fn per_write_ttl_expires_on_get() {
    let node = test_node();
    let ks = node.keyspace("cache").unwrap();

    ks.put_with_ttl(b"temp", b"v", Some(1));
    assert_eq!(value(ks.get(b"temp")), Some(b"v".to_vec()));

    // Once the TTL passes the entry is dead, and a read must not serve it —
    // expiry is enforced on read, not only by the background sweep.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(value(ks.get(b"temp")), None);
}

#[tokio::test]
async fn put_without_ttl_never_expires() {
    let node = test_node();
    let ks = node.keyspace("cache").unwrap();

    ks.put(b"permanent", b"v");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(value(ks.get(b"permanent")), Some(b"v".to_vec()));
    assert_eq!(ks.tracked_ttl_keys(), 0, "no TTL tracked for a plain put");
}

#[tokio::test]
async fn expired_entries_stop_being_tracked() {
    let node = test_node();
    let ks = node.keyspace("cache").unwrap();

    ks.put_with_ttl(b"a", b"1", Some(1));
    ks.put_with_ttl(b"b", b"2", Some(1));
    assert_eq!(ks.tracked_ttl_keys(), 2);

    tokio::time::sleep(Duration::from_millis(1100)).await;

    assert_eq!(value(ks.get(b"a")), None);
    assert_eq!(value(ks.get(b"b")), None);
    assert_eq!(ks.tracked_ttl_keys(), 0, "expired entries stop being tracked");
}

#[tokio::test]
async fn refreshing_an_expired_key_makes_it_live_again() {
    // Writing over a key whose TTL has passed must resurrect it with the new
    // value and new expiry — the pending expiry must not linger and delete the
    // fresh value out from under the writer.
    let node = test_node();
    let ks = node.keyspace("cache").unwrap();

    ks.put_with_ttl(b"k", b"old", Some(1));
    tokio::time::sleep(Duration::from_millis(1100)).await; // now expired

    ks.put_with_ttl(b"k", b"fresh", Some(3600));

    assert_eq!(
        value(ks.get(b"k")),
        Some(b"fresh".to_vec()),
        "a refreshed key must not be expired away by its previous TTL"
    );
}

#[tokio::test]
async fn ttl_zero_clears_existing_expiry() {
    let node = test_node();
    let ks = node.keyspace("cache").unwrap();

    ks.put_with_ttl(b"k", b"v1", Some(1));
    assert_eq!(ks.tracked_ttl_keys(), 1);
    // Rewrite with ttl=0 -> pinned, no expiry.
    ks.put_with_ttl(b"k", b"v2", Some(0));
    assert_eq!(ks.tracked_ttl_keys(), 0);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(value(ks.get(b"k")), Some(b"v2".to_vec()));
}

#[tokio::test]
async fn default_ttl_applies_to_writes_that_omit_one() {
    // The keyspace default is what a caller gets when they don't name a TTL;
    // an explicit per-write TTL overrides it.
    let mut config = Config::default();
    config.keyspaces[0].default_ttl_secs = 1;
    let node = Node::build(config).unwrap();
    let ks = node.keyspace("cache").unwrap();

    ks.put(b"inherits", b"v");
    ks.put_with_ttl(b"pinned", b"v", Some(0));
    assert_eq!(ks.tracked_ttl_keys(), 1, "only the inheriting key has an expiry");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(value(ks.get(b"inherits")), None, "default TTL must apply");
    assert_eq!(value(ks.get(b"pinned")), Some(b"v".to_vec()));
}
