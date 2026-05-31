use crate::dag::{DagEdge, DagNode, DagValidationError, TypeSafeDag};
use petgraph::prelude::NodeIndex;
use std::collections::HashSet;
use std::fmt::Debug;

// Re-export the DagValidationError from dag module

pub trait DagValidator: Debug + Send + Sync + 'static {
    type Node: DagNode;
    type Edge: DagEdge;

    fn validate(&self, dag: &TypeSafeDag<Self::Node, Self::Edge>)
        -> Result<(), DagValidationError>;

    fn detect_cycle(
        &self,
        dag: &TypeSafeDag<Self::Node, Self::Edge>,
        node: NodeIndex,
        visited: &mut HashSet<NodeIndex>,
        rec_stack: &mut HashSet<NodeIndex>,
    ) -> bool;

    fn validate_node(&self, node: &Self::Node) -> Result<(), DagValidationError> {
        node.validate()
    }

    fn validate_edge(&self, edge: &Self::Edge) -> Result<(), DagValidationError> {
        edge.validate()
    }

    fn validate_dependencies(
        &self,
        node: &Self::Node,
        dag: &TypeSafeDag<Self::Node, Self::Edge>,
    ) -> Result<(), DagValidationError> {
        for dep in node.dependencies() {
            if dag.get_node(&dep).is_none() {
                return Err(DagValidationError::MissingDependencies(format!(
                    "Missing dependency: {}",
                    dep
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DefaultDagValidator<N, E> {
    _phantom: std::marker::PhantomData<(N, E)>,
}

impl<N, E> DefaultDagValidator<N, E> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<N, E> Default for DefaultDagValidator<N, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N, E> DagValidator for DefaultDagValidator<N, E>
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    type Node = N;
    type Edge = E;

    fn validate(&self, dag: &TypeSafeDag<N, E>) -> Result<(), DagValidationError> {
        // Check for cycles
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for node_idx in dag.graph.node_indices() {
            if !visited.contains(&node_idx)
                && self.detect_cycle(dag, node_idx, &mut visited, &mut rec_stack)
            {
                return Err(DagValidationError::CycleDetected);
            }
        }

        // Validate nodes
        for node in dag.nodes() {
            if let Err(err) = self.validate_node(node) {
                return Err(DagValidationError::NodeValidation(err.to_string()));
            }

            // Check dependencies
            if let Err(err) = self.validate_dependencies(node, dag) {
                return Err(DagValidationError::MissingDependencies(err.to_string()));
            }
        }

        // Validate edges
        for (_, _, edge) in dag.edges() {
            if let Err(err) = self.validate_edge(edge) {
                return Err(DagValidationError::EdgeValidation(err.to_string()));
            }
        }

        Ok(())
    }

    fn detect_cycle(
        &self,
        dag: &TypeSafeDag<N, E>,
        node: NodeIndex,
        visited: &mut HashSet<NodeIndex>,
        rec_stack: &mut HashSet<NodeIndex>,
    ) -> bool {
        if rec_stack.contains(&node) {
            return true;
        }

        if visited.contains(&node) {
            return false;
        }

        visited.insert(node);
        rec_stack.insert(node);

        for (target, _) in dag.edges_from(node) {
            if self.detect_cycle(dag, target, visited, rec_stack) {
                return true;
            }
        }

        rec_stack.remove(&node);
        false
    }

    fn validate_node(&self, node: &N) -> Result<(), DagValidationError> {
        node.validate()
    }

    fn validate_edge(&self, edge: &E) -> Result<(), DagValidationError> {
        edge.validate()
    }

    fn validate_dependencies(
        &self,
        node: &N,
        dag: &TypeSafeDag<N, E>,
    ) -> Result<(), DagValidationError> {
        for dep in node.dependencies() {
            if dag.get_node(&dep).is_none() {
                return Err(DagValidationError::MissingDependencies(format!(
                    "Missing dependency: {}",
                    dep
                )));
            }
        }
        Ok(())
    }
}
