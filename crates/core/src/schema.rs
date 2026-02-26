use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A named JSON Schema definition.
///
/// The `name` maps to the `type` field on tuples and is used as a
/// lookup key in the schema registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    /// Unique name for this schema; matches the `type` field on tuples.
    pub name: String,
    /// JSON Schema definition.
    pub definition: Value,
}

impl Schema {
    /// Validate `data` against this schema.
    ///
    /// Returns `Ok(())` if valid, or `Err` with a human-readable message.
    pub fn validate(&self, data: &Value) -> Result<(), String> {
        let compiled = jsonschema::JSONSchema::compile(&self.definition)
            .map_err(|e| format!("invalid schema '{}': {e}", self.name))?;
        if compiled.is_valid(data) {
            return Ok(());
        }
        let msgs: Vec<String> = compiled
            .validate(data)
            .unwrap_err()
            .map(|e| e.to_string())
            .collect();
        Err(msgs.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn order_schema() -> Schema {
        Schema {
            name: "order".to_string(),
            definition: json!({
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            }),
        }
    }

    #[test]
    fn roundtrip_serialization() {
        let schema = order_schema();
        let json = serde_json::to_string(&schema).unwrap();
        let back: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, back);
    }

    #[test]
    fn validate_valid_data() {
        let schema = order_schema();
        assert!(schema.validate(&json!({ "id": "abc" })).is_ok());
    }

    #[test]
    fn validate_missing_required_field() {
        let schema = order_schema();
        assert!(schema.validate(&json!({})).is_err());
    }

    #[test]
    fn validate_wrong_type() {
        let schema = order_schema();
        assert!(schema.validate(&json!({ "id": 42 })).is_err());
    }
}
