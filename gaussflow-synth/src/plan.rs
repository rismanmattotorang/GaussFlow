//! The Plan IR — an abstract capability graph produced by the planning stage, *before* lowering
//! to a concrete `WorkflowSpec`. Keeping a distinct IR makes synthesis testable and lets the
//! lowering strategy change without re-prompting.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// An ordered set of capabilities with data dependencies — the planner's output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanIR {
    /// A short name for the workflow.
    #[serde(default)]
    pub name: String,
    /// The steps to run.
    pub steps: Vec<PlanStep>,
}

impl PlanIR {
    /// Get a mutable reference to a step by id.
    pub fn step_mut(&mut self, id: &str) -> Option<&mut PlanStep> {
        self.steps.iter_mut().find(|s| s.id == id)
    }

    /// Set (or insert) a param on a step. Returns `false` if no such step exists.
    pub fn set_param(&mut self, step_id: &str, key: &str, value: Value) -> bool {
        match self.step_mut(step_id) {
            Some(step) => {
                step.params.insert(key.to_string(), value);
                true
            }
            None => false,
        }
    }

    /// Add a step to the plan.
    pub fn add_step(&mut self, step: PlanStep) {
        self.steps.push(step);
    }

    /// Remove a step and scrub all references to it (other steps' `depends_on` and `edges`).
    /// Returns `false` if no such step exists.
    pub fn remove_step(&mut self, id: &str) -> bool {
        let before = self.steps.len();
        self.steps.retain(|s| s.id != id);
        if self.steps.len() == before {
            return false;
        }
        for step in &mut self.steps {
            step.depends_on.retain(|d| d != id);
            step.edges.remove(id);
        }
        true
    }
}

/// A single planned capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanStep {
    /// Unique step id (becomes the node id).
    pub id: String,
    /// The capability to use (must be a supported node type — see [`crate::catalog`]).
    pub capability: String,
    /// Ids of steps this step depends on (become inbound edges).
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Edge labels for this step's *outgoing* edges, keyed by the downstream step id. Used for
    /// conditional/router branches (e.g. `{ "approve": "true", "reject": "false" }`); a label of
    /// `"success"` (the default for any unlabeled edge) means "always".
    #[serde(default)]
    pub edges: Map<String, Value>,
    /// Node params, passed through verbatim to the handler.
    #[serde(default)]
    pub params: Map<String, Value>,
}
