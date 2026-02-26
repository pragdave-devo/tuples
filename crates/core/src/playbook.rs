use crate::filter::Filter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An agent definition within a playbook.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Agent {
    pub id: String,
    pub description: String,
    /// Name of the registered Schema for this agent's input.
    pub schema: String,
}

/// A trigger: a filter + which agent to invoke + how to map tuple fields to agent params.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    pub filter: Filter,
    /// ID of the agent within this playbook to invoke.
    pub agent: String,
    /// Maps agent parameter names to tuple field names.
    pub mapping: HashMap<String, String>,
}

/// A playbook: a named workflow definition with agents and triggers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Playbook {
    pub name: String,
    pub description: String,
    /// ID of the conductor agent.
    pub conductor: String,
    pub agents: Vec<Agent>,
    pub triggers: Vec<Trigger>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Filter;
    use serde_json::json;

    fn example_playbook() -> Playbook {
        Playbook {
            name: "fulfil".to_string(),
            description: "Order fulfilment".to_string(),
            conductor: "processor".to_string(),
            agents: vec![Agent {
                id: "processor".to_string(),
                description: "Processes orders".to_string(),
                schema: "order".to_string(),
            }],
            triggers: vec![Trigger {
                filter: Filter {
                    id: "t1".to_string(),
                    exact: [("type".to_string(), json!("order"))].into(),
                    wildcards: vec![],
                    predicates: vec![],
                },
                agent: "processor".to_string(),
                mapping: [("order_id".to_string(), "id".to_string())].into(),
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
    fn trigger_field_mapping() {
        let pb = example_playbook();
        let trigger = &pb.triggers[0];
        assert_eq!(trigger.mapping.get("order_id"), Some(&"id".to_string()));
        assert_eq!(trigger.agent, "processor");
    }
}
