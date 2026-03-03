use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tuples_core::schema::Schema;

/// Persistent storage for schemas.
#[async_trait]
pub trait SchemaStore: Send + Sync {
    /// Register or overwrite a schema.
    async fn register(&mut self, schema: Schema) -> Result<()>;
    /// Retrieve a schema by name, or `None` if not found.
    async fn get(&self, name: &str) -> Result<Option<Schema>>;
    /// List all registered schemas, sorted by name.
    async fn list(&self) -> Result<Vec<Schema>>;
    /// Remove all schemas.
    async fn clear(&mut self) -> Result<()>;
}

/// In-memory schema store (for testing and early stages).
#[derive(Default)]
pub struct InMemorySchemaStore {
    schemas: HashMap<String, Schema>,
}

#[async_trait]
impl SchemaStore for InMemorySchemaStore {
    async fn register(&mut self, schema: Schema) -> Result<()> {
        self.schemas.insert(schema.name.clone(), schema);
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Schema>> {
        Ok(self.schemas.get(name).cloned())
    }

    async fn list(&self) -> Result<Vec<Schema>> {
        let mut schemas: Vec<Schema> = self.schemas.values().cloned().collect();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(schemas)
    }

    async fn clear(&mut self) -> Result<()> {
        self.schemas.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn example_schema(name: &str) -> Schema {
        Schema {
            name: name.to_string(),
            definition: json!({ "type": "object" }),
        }
    }

    #[tokio::test]
    async fn register_and_get() {
        let mut store = InMemorySchemaStore::default();
        store.register(example_schema("order")).await.unwrap();
        let result = store.get("order").await.unwrap();
        assert_eq!(result.unwrap().name, "order");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = InMemorySchemaStore::default();
        assert!(store.get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_sorted() {
        let mut store = InMemorySchemaStore::default();
        store.register(example_schema("zebra")).await.unwrap();
        store.register(example_schema("apple")).await.unwrap();
        store.register(example_schema("mango")).await.unwrap();
        let names: Vec<_> = store.list().await.unwrap().into_iter().map(|s| s.name).collect();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    #[tokio::test]
    async fn register_overwrites() {
        let mut store = InMemorySchemaStore::default();
        store.register(example_schema("order")).await.unwrap();
        let updated = Schema {
            name: "order".to_string(),
            definition: json!({ "type": "object", "version": 2 }),
        };
        store.register(updated.clone()).await.unwrap();
        let result = store.get("order").await.unwrap().unwrap();
        assert_eq!(result.definition, updated.definition);
    }
}
