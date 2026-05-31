//! Test utilities for GaussFlow core.
//!
//! QUARANTINED (roadmap Phase 0 → Phase 1): shared helpers for the legacy `core::engine` suite
//! (`execution_engine.rs`). Drifted from the current API (`NodeSpec` gained `max_retries`/
//! `timeout_ms`; no `TypeSafeDag::from_spec`). Gated behind `legacy_tests` so default builds / CI
//! stay green; will be revived alongside the Phase 1 engine consolidation.
#![cfg(feature = "legacy_tests")]

use gaussflow_core::{
    model::{EdgeSpec, NodeSpec, NodeType, WorkflowSettings, WorkflowSpec},
    TypeSafeDag,
};
use std::path::Path;

/// Creates a simple linear workflow for testing
pub fn create_linear_workflow(node_count: usize) -> WorkflowSpec {
    let mut nodes = Vec::new();
    let mut connections = Vec::new();

    for i in 0..node_count {
        let node_id = format!("node_{}", i);
        nodes.push(NodeSpec {
            id: node_id.clone(),
            name: Some(format!("Node {}", i)),
            description: None,
            node_type: NodeType::LlmCall,
            config: Default::default(),
            tags: vec![],
            enabled: true,
            retry: None,
            resources: None,
            metadata: Default::default(),
            params: Default::default(),
        });

        if i > 0 {
            connections.push(EdgeSpec {
                from: format!("node_{}", i - 1),
                to: node_id,
                on: "success".to_string(),
            });
        }
    }

    WorkflowSpec {
        name: "test-workflow".to_string(),
        nodes,
        connections,
        settings: WorkflowSettings::default(),
    }
}

/// Asserts that a file exists
#[track_caller]
pub fn assert_file_exists(path: impl AsRef<Path>) {
    assert!(
        path.as_ref().exists(),
        "File does not exist: {:?}",
        path.as_ref()
    );
}

/// Asserts that a directory exists
#[track_caller]
pub fn assert_dir_exists(path: impl AsRef<Path>) {
    assert!(
        path.as_ref().is_dir(),
        "Directory does not exist: {:?}",
        path.as_ref()
    );
}

/// Creates a test workflow with the given nodes and edges
pub fn create_test_workflow(
    name: &str,
    nodes: Vec<(String, NodeType)>,
    edges: Vec<(String, String, String)>,
) -> TypeSafeDag {
    let node_specs = nodes
        .into_iter()
        .map(|(id, node_type)| NodeSpec {
            id: id.clone(),
            name: Some(id.clone()),
            description: None,
            node_type,
            config: Default::default(),
            tags: vec![],
            enabled: true,
            retry: None,
            resources: None,
            metadata: Default::default(),
            params: Default::default(),
        })
        .collect();

    let edge_specs = edges
        .into_iter()
        .map(|(from, to, on)| EdgeSpec { from, to, on })
        .collect();

    let spec = WorkflowSpec {
        name: name.to_string(),
        nodes: node_specs,
        connections: edge_specs,
        settings: WorkflowSettings::default(),
    };

    TypeSafeDag::from_spec(&spec).expect("Failed to create test workflow")
}
