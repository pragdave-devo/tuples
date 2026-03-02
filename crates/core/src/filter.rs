use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A predicate evaluated against a single field value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Predicate {
    /// Value (numeric) must be strictly greater than the threshold.
    Gt(f64),
    /// Value (numeric) must be strictly less than the threshold.
    Lt(f64),
    /// Value (numeric) must be greater than or equal to the threshold.
    Gte(f64),
    /// Value (numeric) must be less than or equal to the threshold.
    Lte(f64),
    /// Value (string) must match the regular expression.
    Regex(String),
    /// Value must be one of the given values.
    In(Vec<Value>),
}

impl Predicate {
    /// Returns true if `value` satisfies this predicate.
    pub fn evaluate(&self, value: &Value) -> bool {
        match self {
            Predicate::Gt(t) => value.as_f64().map_or(false, |v| v > *t),
            Predicate::Lt(t) => value.as_f64().map_or(false, |v| v < *t),
            Predicate::Gte(t) => value.as_f64().map_or(false, |v| v >= *t),
            Predicate::Lte(t) => value.as_f64().map_or(false, |v| v <= *t),
            Predicate::Regex(pattern) => {
                let s = match value.as_str() {
                    Some(s) => s,
                    None => return false,
                };
                Regex::new(pattern).map_or(false, |r| r.is_match(s))
            }
            Predicate::In(values) => values.contains(value),
        }
    }
}

/// A predicate condition targeting a named field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PredicateCondition {
    /// Field name to test.
    pub key: String,
    #[serde(flatten)]
    pub predicate: Predicate,
}

/// A filter definition.
///
/// All conditions must hold simultaneously for the filter to match. Fields
/// default to empty/none if omitted in JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Filter {
    /// Unique filter identifier.
    pub id: String,
    /// Keys that must be present with a specific exact value.
    #[serde(default)]
    pub exact: HashMap<String, Value>,
    /// Keys that must exist (value is ignored).
    #[serde(default)]
    pub wildcards: Vec<String>,
    /// Predicate conditions on specific keys.
    #[serde(default)]
    pub predicates: Vec<PredicateCondition>,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            id: String::new(),
            exact: Default::default(),
            wildcards: Default::default(),
            predicates: Default::default(),
        }
    }
}

impl Filter {
    /// Returns true if this filter matches the given JSON object.
    pub fn matches(&self, data: &Value) -> bool {
        let obj = match data.as_object() {
            Some(o) => o,
            None => return false,
        };

        for (key, expected) in &self.exact {
            if obj.get(key) != Some(expected) {
                return false;
            }
        }

        for key in &self.wildcards {
            if !obj.contains_key(key) {
                return false;
            }
        }

        for pc in &self.predicates {
            match obj.get(&pc.key) {
                Some(v) if pc.predicate.evaluate(v) => {}
                _ => return false,
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn order_filter() -> Filter {
        Filter {
            id: "f1".to_string(),
            exact: [("type".to_string(), json!("order"))].into(),
            wildcards: vec!["customer_id".to_string()],
            predicates: vec![PredicateCondition {
                key: "amount".to_string(),
                predicate: Predicate::Gt(100.0),
            }],
        }
    }

    #[test]
    fn matches_valid_tuple() {
        let f = order_filter();
        let data = json!({ "type": "order", "customer_id": "c1", "amount": 150.0 });
        assert!(f.matches(&data));
    }

    #[test]
    fn rejects_wrong_exact() {
        let f = order_filter();
        let data = json!({ "type": "payment", "customer_id": "c1", "amount": 150.0 });
        assert!(!f.matches(&data));
    }

    #[test]
    fn rejects_missing_wildcard() {
        let f = order_filter();
        let data = json!({ "type": "order", "amount": 150.0 });
        assert!(!f.matches(&data));
    }

    #[test]
    fn rejects_failing_predicate() {
        let f = order_filter();
        let data = json!({ "type": "order", "customer_id": "c1", "amount": 50.0 });
        assert!(!f.matches(&data));
    }

    #[test]
    fn predicate_regex_matches() {
        let f = Filter {
            id: "f2".to_string(),
            exact: HashMap::new(),
            wildcards: vec![],
            predicates: vec![PredicateCondition {
                key: "email".to_string(),
                predicate: Predicate::Regex(r"^.+@example\.com$".to_string()),
            }],
        };
        assert!(f.matches(&json!({ "email": "alice@example.com" })));
        assert!(!f.matches(&json!({ "email": "alice@other.com" })));
    }

    #[test]
    fn predicate_in_matches() {
        let f = Filter {
            id: "f3".to_string(),
            exact: HashMap::new(),
            wildcards: vec![],
            predicates: vec![PredicateCondition {
                key: "status".to_string(),
                predicate: Predicate::In(vec![json!("pending"), json!("processing")]),
            }],
        };
        assert!(f.matches(&json!({ "status": "pending" })));
        assert!(!f.matches(&json!({ "status": "shipped" })));
    }

    #[test]
    fn roundtrip_serialization() {
        let f = order_filter();
        let json = serde_json::to_string(&f).unwrap();
        let back: Filter = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }
}
