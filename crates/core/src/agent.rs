use serde::{Deserialize, Serialize};

/// A global agent definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Agent {
    pub name: String,
    pub description: String,
    /// Name of the registered Schema for this agent's input parameters.
    pub schema: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_serialization() {
        let agent = Agent {
            name: "processor".to_string(),
            description: "Processes orders".to_string(),
            schema: "order".to_string(),
        };
        let json = serde_json::to_string(&agent).unwrap();
        let back: Agent = serde_json::from_str(&json).unwrap();
        assert_eq!(agent, back);
    }
}
