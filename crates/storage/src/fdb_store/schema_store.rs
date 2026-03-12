use anyhow::{anyhow, Result};
use async_trait::async_trait;
use foundationdb::{options::StreamingMode, Database, RangeOption};
use std::sync::Arc;
use tuples_core::schema::Schema;

use crate::SchemaStore;

pub struct FdbSchemaStore {
    db: Arc<Database>,
}

impl FdbSchemaStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn schema_key(name: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("schemas", name))
}

fn schemas_prefix() -> (Vec<u8>, Vec<u8>) {
    let prefix = foundationdb::tuple::pack(&("schemas",));
    let mut end = prefix.clone();
    // Increment last byte to form exclusive end of range
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

#[async_trait]
impl SchemaStore for FdbSchemaStore {
    async fn register(&mut self, schema: Schema) -> Result<()> {
        let key = schema_key(&schema.name);
        let value = serde_json::to_vec(&schema)?;
        let trx = self.db.create_trx()?;
        trx.set(&key, &value);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Schema>> {
        let key = schema_key(name);
        let trx = self.db.create_trx()?;
        let result = trx.get(&key, false).await.map_err(|e| anyhow!("fdb get: {e}"))?;
        match result {
            None => Ok(None),
            Some(bytes) => {
                let schema: Schema = serde_json::from_slice(&bytes)?;
                Ok(Some(schema))
            }
        }
    }

    async fn list(&self) -> Result<Vec<Schema>> {
        let (begin, end) = schemas_prefix();
        let trx = self.db.create_trx()?;
        let range_opt = RangeOption {
            begin: foundationdb::KeySelector::first_greater_or_equal(begin),
            end: foundationdb::KeySelector::first_greater_or_equal(end),
            mode: StreamingMode::WantAll,
            reverse: false,
            ..Default::default()
        };
        let kvs = trx
            .get_range(&range_opt, 1, false)
            .await
            .map_err(|e| anyhow!("fdb range: {e}"))?;
        let mut schemas: Vec<Schema> = kvs
            .iter()
            .map(|kv| serde_json::from_slice(kv.value()))
            .collect::<Result<_, _>>()?;
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(schemas)
    }

    async fn clear(&mut self) -> Result<()> {
        let (begin, end) = schemas_prefix();
        let trx = self.db.create_trx()?;
        trx.clear_range(&begin, &end);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }
}

#[cfg(feature = "fdb")]
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn example_schema(name: &str) -> Schema {
        Schema { name: name.to_string(), definition: json!({ "type": "object" }) }
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_schema_register_get_list() {
        let network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbSchemaStore::new(db);
        store.register(example_schema("alpha")).await.unwrap();
        store.register(example_schema("beta")).await.unwrap();

        let got = store.get("alpha").await.unwrap().unwrap();
        assert_eq!(got.name, "alpha");

        let list = store.list().await.unwrap();
        let names: Vec<_> = list.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));

        drop(network);
    }
}
