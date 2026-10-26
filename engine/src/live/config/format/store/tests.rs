use crate::live::config::format::FormatConfigStore;

#[test]
fn new_store_is_empty() {
    let store = FormatConfigStore::new();

    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
    assert_eq!(store.trade_count(), 0);
    assert_eq!(store.depth_count(), 0);
}

#[test]
fn default_store_is_empty() {
    let store = FormatConfigStore::default();

    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
}

mod loading;
