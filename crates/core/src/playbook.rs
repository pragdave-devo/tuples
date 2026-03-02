use crate::filter::Filter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Where a trigger parameter value comes from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ParamSource {
    /// A hard-coded JSON value passed to the agent unchanged.
    Literal(Value),
    /// The value of a named field taken from the triggering tuple.
    Field(String),
}

/// Which tuples should fire a trigger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerMatch {
    /// Tuple type that must match; validated at RegisterPlaybook time.
    pub tuple_type: String,
    /// Additional filter conditions beyond the type check.
    #[serde(default)]
    pub filter: Filter,
}

/// What to do when a trigger fires.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerExecution {
    /// Name of the global agent to invoke.
    pub agent: String,
    /// Maps each agent parameter name to its source.
    #[serde(default)]
    pub params: HashMap<String, ParamSource>,
}

/// A trigger: a match condition plus an agent execution specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    pub id: String,
    #[serde(rename = "match")]
    pub match_: TriggerMatch,
    pub execution: TriggerExecution,
}

/// A playbook: a named workflow definition referencing global agents.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Playbook {
    pub name: String,
    pub description: String,
    /// Name of the conductor agent (the workflow entry-point).
    pub conductor: String,
    /// Names of global agents this playbook may use.
    pub agents: Vec<String>,
    pub triggers: Vec<Trigger>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_playbook() -> Playbook {
        Playbook {
            name: "fulfil".to_string(),
            description: "Order fulfilment".to_string(),
            conductor: "processor".to_string(),
            agents: vec!["processor".to_string()],
            triggers: vec![Trigger {
                id: "t1".to_string(),
                match_: TriggerMatch {
                    tuple_type: "order".to_string(),
                    filter: Filter::default(),
                },
                execution: TriggerExecution {
                    agent: "processor".to_string(),
                    params: [(
                        "order_id".to_string(),
                        ParamSource::Field("id".to_string()),
                    )]
                    .into(),
                },
            }],
        }
    }

    #[test]
    fn roundtrip_serialization() {
        let pb = example_playbook();
        let json = serde_json::to_string(&pb).unwrap();
        let back: Playbook = serde_json::from_str(&json).unwrap();
        assert_eq!(pb, back);
    }

    #[test]
    fn trigger_param_field_source() {
        let pb = example_playbook();
        let trigger = &pb.triggers[0];
        assert_eq!(
            trigger.execution.params.get("order_id"),
            Some(&ParamSource::Field("id".to_string()))
        );
        assert_eq!(trigger.execution.agent, "processor");
    }

    #[test]
    fn param_source_literal_roundtrip() {
        let source = ParamSource::Literal(serde_json::json!(42));
        let json = serde_json::to_string(&source).unwrap();
        let back: ParamSource = serde_json::from_str(&json).unwrap();
        assert_eq!(source, back);
    }
}
