use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A tuple stored in the tuplespace.
///
/// Required fields are always present. `data` holds the schema-validated
/// payload and may contain additional user-defined fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tuple {
    /// Time-sortable unique identifier (UUID v7).
    pub uuid7: String,
    /// Correlates related tuples across a workflow.
    pub trace_id: String,
    /// When this tuple was created (server-assigned).
    pub created_at: DateTime<Utc>,
    /// Schema name; maps to a registered schema.
    pub tuple_type: String,
    /// User-provided payload, validated against the named schema.
    pub data: Value,
}

impl Tuple {
    /// Flatten the tuple into a single JSON object suitable for filter matching.
    ///
    /// Required fields (`uuid7`, `trace_id`, `created_at`, `type`) are merged
    /// with the user data. Data fields are overwritten by required fields on
    /// collision.
    pub fn to_matchable(&self) -> Value {
        let mut obj = self.data.as_object().cloned().unwrap_or_default();
        obj.insert("uuid7".to_string(), Value::String(self.uuid7.clone()));
        obj.insert("trace_id".to_string(), Value::String(self.trace_id.clone()));
        obj.insert("created_at".to_string(), Value::String(self.created_at.to_rfc3339()));
        obj.insert("type".to_string(), Value::String(self.tuple_type.clone()));
        Value::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_serialization() {
        let tuple = Tuple {
            uuid7: "018e4e1f-0000-7000-8000-000000000001".to_string(),
            trace_id: "trace-1".to_string(),
            created_at: DateTime::from_timestamp(0, 0).unwrap(),
            tuple_type: "order".to_string(),
            data: json!({ "id": "abc" }),
        };
        let json = serde_json::to_string(&tuple).unwrap();
        let back: Tuple = serde_json::from_str(&json).unwrap();
        assert_eq!(tuple, back);
    }
}
