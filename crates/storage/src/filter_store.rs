use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tuples_core::filter::Filter;

/// Persistent storage and matching index for filters.
#[async_trait]
pub trait FilterStore: Send + Sync {
    /// Register or overwrite a filter.
    async fn register(&mut self, filter: Filter) -> Result<()>;
    /// Retrieve a filter by ID, or `None` if not found.
    async fn get(&self, id: &str) -> Result<Option<Filter>>;
    /// List all registered filters, sorted by ID.
    async fn list(&self) -> Result<Vec<Filter>>;
    /// Return IDs of all filters that match `data`, sorted.
    async fn match_data(&self, data: &Value) -> Result<Vec<String>>;
    /// Remove all filters.
    async fn clear(&mut self) -> Result<()>;
}

/// In-memory filter store with a discrimination tree.
///
/// The tree is indexed on exact-match key/value pairs; `type` is used as the
/// primary discriminator. At match time we build a candidate set from the
/// index, then verify all conditions against each candidate.
#[derive(Default)]
pub struct InMemoryFilterStore {
    filters: HashMap<String, Filter>,
    /// exact_index[key][value] = IDs of filters with exact(key=value)
    exact_index: HashMap<String, HashMap<Value, HashSet<String>>>,
    /// IDs of filters that have no exact conditions (always candidates)
    no_exact: HashSet<String>,
}

impl InMemoryFilterStore {
    fn index_filter(&mut self, filter: &Filter) {
        if filter.exact.is_empty() {
            self.no_exact.insert(filter.id.clone());
        } else {
            for (key, value) in &filter.exact {
                self.exact_index
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .insert(filter.id.clone());
            }
        }
    }

    fn deindex_filter(&mut self, filter: &Filter) {
        if filter.exact.is_empty() {
            self.no_exact.remove(&filter.id);
        } else {
            for (key, value) in &filter.exact {
                if let Some(key_map) = self.exact_index.get_mut(key) {
                    if let Some(id_set) = key_map.get_mut(value) {
                        id_set.remove(&filter.id);
                    }
                }
            }
        }
    }

    /// Build candidate set using the discrimination tree.
    ///
    /// A filter is a candidate if at least one of its exact conditions matches
    /// a key/value pair present in `data`, or if it has no exact conditions.
    /// Full condition checking happens afterwards in `match_data`.
    fn candidates(&self, data: &serde_json::Map<String, Value>) -> HashSet<String> {
        let mut candidates = self.no_exact.clone();

        // Branch on `type` first (most selective), then all other keys.
        let type_first = data
            .get("type")
            .into_iter()
            .map(|v| ("type", v))
            .chain(data.iter().filter(|(k, _)| *k != "type").map(|(k, v)| (k.as_str(), v)));

        for (key, value) in type_first {
            if let Some(key_map) = self.exact_index.get(key) {
                if let Some(ids) = key_map.get(value) {
                    candidates.extend(ids.iter().cloned());
                }
            }
        }

        candidates
    }
}

#[async_trait]
impl FilterStore for InMemoryFilterStore {
    async fn register(&mut self, filter: Filter) -> Result<()> {
        if let Some(existing) = self.filters.get(&filter.id).cloned() {
            self.deindex_filter(&existing);
        }
        self.index_filter(&filter);
        self.filters.insert(filter.id.clone(), filter);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Filter>> {
        Ok(self.filters.get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<Filter>> {
        let mut filters: Vec<Filter> = self.filters.values().cloned().collect();
        filters.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(filters)
    }

    async fn match_data(&self, data: &Value) -> Result<Vec<String>> {
        let obj = match data.as_object() {
            Some(o) => o,
            None => return Ok(vec![]),
        };
        let candidates = self.candidates(obj);
        let mut matched: Vec<String> = candidates
            .into_iter()
            .filter(|id| self.filters.get(id).map_or(false, |f| f.matches(data)))
            .collect();
        matched.sort();
        Ok(matched)
    }

    async fn clear(&mut self) -> Result<()> {
        self.filters.clear();
        self.exact_index.clear();
        self.no_exact.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tuples_core::filter::{Predicate, PredicateCondition};

    fn order_filter() -> Filter {
        Filter {
            id: "f-order".to_string(),
            exact: [("type".to_string(), json!("order"))].into(),
            wildcards: vec!["customer_id".to_string()],
            predicates: vec![PredicateCondition {
                key: "amount".to_string(),
                predicate: Predicate::Gt(100.0),
            }],
        }
    }

    fn payment_filter() -> Filter {
        Filter {
            id: "f-payment".to_string(),
            exact: [("type".to_string(), json!("payment"))].into(),
            wildcards: vec![],
            predicates: vec![],
        }
    }

    #[tokio::test]
    async fn register_and_match() {
        let mut store = InMemoryFilterStore::default();
        store.register(order_filter()).await.unwrap();

        let data = json!({ "type": "order", "customer_id": "c1", "amount": 200.0 });
        let ids = store.match_data(&data).await.unwrap();
        assert_eq!(ids, vec!["f-order"]);
    }

    #[tokio::test]
    async fn no_match_wrong_type() {
        let mut store = InMemoryFilterStore::default();
        store.register(order_filter()).await.unwrap();

        let data = json!({ "type": "payment", "customer_id": "c1", "amount": 200.0 });
        let ids = store.match_data(&data).await.unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn multiple_filters_only_matching_returned() {
        let mut store = InMemoryFilterStore::default();
        store.register(order_filter()).await.unwrap();
        store.register(payment_filter()).await.unwrap();

        let data = json!({ "type": "order", "customer_id": "c1", "amount": 200.0 });
        let ids = store.match_data(&data).await.unwrap();
        assert_eq!(ids, vec!["f-order"]);
    }

    #[tokio::test]
    async fn filter_with_no_exact_matches_any_type() {
        let mut store = InMemoryFilterStore::default();
        store
            .register(Filter {
                id: "f-all".to_string(),
                exact: Default::default(),
                wildcards: vec!["id".to_string()],
                predicates: vec![],
            })
            .await
            .unwrap();

        assert!(!store.match_data(&json!({ "type": "order" })).await.unwrap().is_empty() == false);
        // missing wildcard key → no match
        assert!(store
            .match_data(&json!({ "type": "order" }))
            .await
            .unwrap()
            .is_empty());
        // wildcard key present → match
        assert_eq!(
            store
                .match_data(&json!({ "type": "anything", "id": "x" }))
                .await
                .unwrap(),
            vec!["f-all"]
        );
    }

    #[tokio::test]
    async fn re_register_updates_index() {
        let mut store = InMemoryFilterStore::default();
        store.register(order_filter()).await.unwrap();
        // Update the filter to match payments instead
        store.register(payment_filter()).await.unwrap();
        store
            .register(Filter { id: "f-order".to_string(), ..payment_filter() })
            .await
            .unwrap();

        // f-order now has payment semantics
        let data = json!({ "type": "payment" });
        let ids = store.match_data(&data).await.unwrap();
        assert!(ids.contains(&"f-order".to_string()));

        let data = json!({ "type": "order", "customer_id": "c1", "amount": 200.0 });
        let ids = store.match_data(&data).await.unwrap();
        assert!(!ids.contains(&"f-order".to_string()));
    }

    #[tokio::test]
    async fn list_returns_sorted() {
        let mut store = InMemoryFilterStore::default();
        store.register(payment_filter()).await.unwrap();
        store.register(order_filter()).await.unwrap();
        let ids: Vec<_> = store.list().await.unwrap().into_iter().map(|f| f.id).collect();
        assert_eq!(ids, vec!["f-order", "f-payment"]);
    }
}
