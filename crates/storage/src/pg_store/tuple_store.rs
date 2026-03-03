use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tuples_core::tuple::Tuple;

use crate::TupleStore;

pub struct PgTupleStore {
    pool: PgPool,
}

impl PgTupleStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TupleStore for PgTupleStore {
    async fn put(&self, tuple: Tuple) -> Result<()> {
        let data = serde_json::to_value(&tuple)?;
        sqlx::query("INSERT INTO tuples (uuid7, tuple_type, trace_id, data) VALUES ($1, $2, $3, $4) ON CONFLICT (uuid7) DO UPDATE SET data = $4")
            .bind(&tuple.uuid7)
            .bind(&tuple.tuple_type)
            .bind(&tuple.trace_id)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get(&self, uuid7: &str) -> Result<Option<Tuple>> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM tuples WHERE uuid7 = $1",
        )
        .bind(uuid7)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((data,)) => Ok(Some(serde_json::from_value(data)?)),
        }
    }

    async fn put_batch(&self, tuples: &[Tuple]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for tuple in tuples {
            let data = serde_json::to_value(tuple)?;
            sqlx::query("INSERT INTO tuples (uuid7, tuple_type, trace_id, data) VALUES ($1, $2, $3, $4) ON CONFLICT (uuid7) DO UPDATE SET data = $4")
                .bind(&tuple.uuid7)
                .bind(&tuple.tuple_type)
                .bind(&tuple.trace_id)
                .bind(&data)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
