mod agent_store;
mod filter_store;
mod playbook_store;
mod run_store;
mod schema_store;
mod tuple_store;

pub use agent_store::DynamoAgentStore;
pub use filter_store::DynamoFilterStore;
pub use playbook_store::DynamoPlaybookStore;
pub use run_store::DynamoRunStore;
pub use schema_store::DynamoSchemaStore;
pub use tuple_store::DynamoTupleStore;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    WriteRequest,
};
use aws_sdk_dynamodb::Client;

/// Ensure all required DynamoDB tables exist, creating any that are missing.
pub async fn ensure_tables(client: &Client, prefix: &str) -> anyhow::Result<()> {
    let tables: &[(&str, &str)] = &[
        ("schemas", "name"),
        ("filters", "id"),
        ("tuples", "uuid7"),
        ("playbooks", "name"),
        ("agents", "name"),
        ("runs", "trace_id"),
        ("agent_runs", "id"),
    ];

    let existing = client.list_tables().send().await?;
    let existing_names: Vec<&str> = existing
        .table_names()
        .iter()
        .map(|s| s.as_str())
        .collect();

    let mut created = Vec::new();
    for (suffix, key) in tables {
        let table_name = format!("{prefix}{suffix}");
        if existing_names.contains(&table_name.as_str()) {
            continue;
        }
        println!("  creating table {table_name}");
        client
            .create_table()
            .table_name(&table_name)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(*key)
                    .key_type(KeyType::Hash)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(*key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await?;
        created.push(table_name);
    }

    // Wait for newly created tables to become ACTIVE
    for table_name in &created {
        print!("  waiting for {table_name}...");
        loop {
            let resp = client
                .describe_table()
                .table_name(table_name)
                .send()
                .await?;
            let status = resp
                .table()
                .and_then(|t| t.table_status())
                .map(|s| s.as_str().to_string());
            if status.as_deref() == Some("ACTIVE") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        println!(" ready");
    }

    Ok(())
}

/// Scan all items in a table and batch-delete them by primary key.
pub(crate) async fn clear_table(client: &Client, table: &str, key: &str) -> anyhow::Result<()> {
    let mut start_key: Option<std::collections::HashMap<String, AttributeValue>> = None;
    loop {
        let mut scan = client
            .scan()
            .table_name(table)
            .projection_expression(format!("#{key}"))
            .expression_attribute_names(format!("#{key}"), key);
        if let Some(ref k) = start_key {
            scan = scan.set_exclusive_start_key(Some(k.clone()));
        }
        let result = scan.send().await?;
        let items = result.items();
        if items.is_empty() {
            break;
        }
        for chunk in items.chunks(25) {
            let requests: Vec<WriteRequest> = chunk
                .iter()
                .filter_map(|item| {
                    let val = item.get(key)?.clone();
                    Some(
                        WriteRequest::builder()
                            .delete_request(
                                aws_sdk_dynamodb::types::DeleteRequest::builder()
                                    .key(key, val)
                                    .build()
                                    .ok()?,
                            )
                            .build(),
                    )
                })
                .collect();
            if !requests.is_empty() {
                client
                    .batch_write_item()
                    .request_items(table, requests)
                    .send()
                    .await?;
            }
        }
        start_key = result.last_evaluated_key().map(|k| k.to_owned());
        if start_key.is_none() {
            break;
        }
    }
    Ok(())
}
