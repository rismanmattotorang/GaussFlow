//! Role-based authorization for API actions.
//!
//! Three roles in increasing privilege: `viewer` ⊂ `operator` ⊂ `admin`. A request is authorized
//! if **any** of the principal's roles grants the requested [`Action`].

use serde::{Deserialize, Serialize};

/// An action a caller may attempt against the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Read workflows / executions / metrics.
    View,
    /// Create or modify a workflow.
    CreateWorkflow,
    /// Execute a workflow.
    ExecuteWorkflow,
    /// Deploy a workflow.
    Deploy,
    /// Manage server configuration / secrets.
    ManageConfig,
}

/// A role granted to a principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Read-only.
    Viewer,
    /// Read + create/execute/deploy workflows.
    Operator,
    /// Full access.
    Admin,
}

impl Role {
    /// Parse a role name (case-insensitive).
    pub fn parse(s: &str) -> Option<Role> {
        match s.to_ascii_lowercase().as_str() {
            "viewer" => Some(Role::Viewer),
            "operator" => Some(Role::Operator),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }

    /// Whether this role grants `action`.
    pub fn allows(self, action: Action) -> bool {
        use Action::*;
        match self {
            Role::Admin => true,
            Role::Operator => matches!(action, View | CreateWorkflow | ExecuteWorkflow | Deploy),
            Role::Viewer => matches!(action, View),
        }
    }
}

/// Authorize `action` given a principal's role names. Unknown role names are ignored.
pub fn authorize(roles: &[String], action: Action) -> bool {
    roles
        .iter()
        .filter_map(|r| Role::parse(r))
        .any(|role| role.allows(action))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_privileges() {
        assert!(Role::Admin.allows(Action::ManageConfig));
        assert!(Role::Operator.allows(Action::ExecuteWorkflow));
        assert!(!Role::Operator.allows(Action::ManageConfig));
        assert!(Role::Viewer.allows(Action::View));
        assert!(!Role::Viewer.allows(Action::ExecuteWorkflow));
    }

    #[test]
    fn authorize_uses_any_role() {
        let roles = vec!["viewer".to_string(), "operator".to_string()];
        assert!(authorize(&roles, Action::ExecuteWorkflow));
        assert!(authorize(&roles, Action::View));
        assert!(!authorize(&roles, Action::ManageConfig));

        // Unknown roles grant nothing.
        assert!(!authorize(&["wizard".to_string()], Action::View));
        assert!(!authorize(&[], Action::View));
    }
}
