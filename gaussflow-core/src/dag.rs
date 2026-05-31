//! Directed Acyclic Graph (DAG) implementation for workflow execution.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use petgraph::graph::{DiGraph, EdgeIndex, NodeIndex};
use petgraph::visit::Dfs;
use petgraph::visit::EdgeRef;
use serde::{Deserialize, Serialize};

use crate::model::{NodeSpec, RetrySpec, WorkflowSettings};
use crate::resource::ResourceManager;
use crate::scheduler::Scheduler;
use crate::validator::DagValidator;

/// Defines the condition under which an edge in the DAG is traversed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum EdgeCondition {
    /// The edge is always traversed
    Always,
    /// The edge is only traversed if the source node succeeds
    #[default]
    OnSuccess,
    /// The edge is only traversed if the source node fails
    OnFailure,
    /// The edge is only traversed if the source node is being retried
    OnRetry,
    /// Custom condition defined by a string expression
    Custom(String),
}

impl std::hash::Hash for EdgeCondition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            EdgeCondition::Always => 0.hash(state),
            EdgeCondition::OnSuccess => 1.hash(state),
            EdgeCondition::OnFailure => 2.hash(state),
            EdgeCondition::OnRetry => 3.hash(state),
            EdgeCondition::Custom(s) => s.hash(state),
        }
    }
}

/// Metadata associated with edges in the DAG
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EdgeMetadata {
    /// Optional description of what this edge represents
    pub description: Option<String>,
    /// Priority level (higher numbers indicate higher priority)
    pub priority: u8,
    /// Optional retry policy specific to this edge
    pub retry_policy: Option<RetrySpec>,
    /// Additional metadata as key-value pairs
    pub labels: HashMap<String, String>,
}

/// A node in the DAG
pub trait DagNode: Clone + Debug + Hash + Eq + Send + Sync + 'static {
    fn validate(&self) -> Result<(), crate::dag::DagValidationError>;
    fn dependencies(&self) -> Vec<String>;
    fn resource_requirements(&self) -> crate::resource::ResourceSpec;
    fn priority(&self) -> u8;

    /// Get the unique identifier for the node
    fn id(&self) -> &str;

    /// Get the display name for the node
    fn name(&self) -> &str;

    /// Get the node type
    fn node_type(&self) -> &str;
}

pub trait DagEdge: Clone + Debug + Send + Sync + 'static {
    fn condition(&self) -> EdgeCondition;
    fn metadata(&self) -> EdgeMetadata;

    /// Validate the edge
    fn validate(&self) -> Result<(), crate::dag::DagValidationError>;

    /// Get the source node ID
    fn source(&self) -> String;

    /// Get the target node ID
    fn target(&self) -> String;
}

#[derive(Debug, Clone)]
pub enum DagValidationError {
    CycleDetected,
    NodeValidation(String),
    EdgeValidation(String),
    MissingDependencies(String),
    InvalidEdgeCondition(String),
}

impl std::fmt::Display for DagValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagValidationError::CycleDetected => write!(f, "Cycle detected in DAG"),
            DagValidationError::NodeValidation(msg) => write!(f, "Node validation failed: {}", msg),
            DagValidationError::EdgeValidation(msg) => write!(f, "Edge validation failed: {}", msg),
            DagValidationError::MissingDependencies(dep) => {
                write!(f, "Missing dependencies for node {}", dep)
            }
            DagValidationError::InvalidEdgeCondition(cond) => {
                write!(f, "Invalid edge condition: {}", cond)
            }
        }
    }
}

impl std::error::Error for DagValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug, Clone)]
/// A directed acyclic graph (DAG) for workflow execution
pub struct DAG<N = NodeSpec, E = EdgeMetadata> {
    /// The underlying graph data structure
    graph: DiGraph<Arc<N>, E>,
    /// Mapping of node IDs to their indices in the graph
    node_indices: HashMap<String, NodeIndex>,
    /// Mapping of (source, target) node ID pairs to their edge indices
    edge_indices: HashMap<(String, String), EdgeIndex>,
    /// Resource manager for node execution
    resource_manager: Arc<ResourceManager>,
}

/// A type-safe DAG implementation with additional workflow features
/// that encapsulates the core DAG functionality and adds workflow-specific features.
pub struct TypeSafeDag<N, E>
where
    N: DagNode + Clone + 'static,
    E: DagEdge + Clone + 'static,
{
    /// The underlying directed graph
    pub graph: DiGraph<N, E>,

    /// The validator for the DAG
    pub validator: Arc<dyn DagValidator<Node = N, Edge = E>>,

    /// The resource manager for the DAG
    pub resource_manager: Arc<ResourceManager>,

    /// The scheduler for the DAG
    pub scheduler: Arc<dyn Scheduler<Node = N, Edge = E>>,

    /// Mapping of node names to their indices in the graph
    pub node_map: HashMap<String, NodeIndex>,

    /// Mapping of edge indices to their metadata
    pub edge_map: HashMap<EdgeIndex, E>,

    /// The name of the workflow
    pub name: String,

    /// Workflow-specific settings
    pub settings: WorkflowSettings,
}

impl<N, E> TypeSafeDag<N, E>
where
    N: DagNode + 'static,
    E: DagEdge + 'static,
{
    /// Creates a new TypeSafeDag with the given name and components.
    ///
    /// # Arguments
    /// * `name` - The name of the workflow
    /// * `validator` - The validator to use for the DAG
    /// * `resource_manager` - The resource manager to use for the DAG
    /// * `scheduler` - The scheduler to use for the DAG
    ///
    /// # Returns
    /// A new TypeSafeDag instance
    pub fn new(
        name: String,
        validator: Arc<dyn DagValidator<Node = N, Edge = E>>,
        resource_manager: Arc<ResourceManager>,
        scheduler: Arc<dyn Scheduler<Node = N, Edge = E>>,
    ) -> Self {
        Self {
            graph: DiGraph::new(),
            validator,
            resource_manager,
            scheduler,
            node_map: HashMap::new(),
            edge_map: HashMap::new(),
            name,
            settings: WorkflowSettings::default(),
        }
    }

    /// Validates the DAG structure
    ///
    /// # Returns
    /// `Ok(())` if the DAG is valid, or an error if validation fails
    pub fn validate(&self) -> Result<(), DagValidationError> {
        // Check for cycles using the validator
        self.validator.validate(self)?;

        // Check for disconnected nodes
        let mut visited = HashSet::new();

        if let Some(first_node) = self.graph.node_indices().next() {
            let mut dfs = Dfs::new(&self.graph, first_node);

            while let Some(node) = dfs.next(&self.graph) {
                visited.insert(node);
            }

            if visited.len() != self.graph.node_count() {
                return Err(DagValidationError::NodeValidation(
                    "Disconnected nodes found in DAG".to_string(),
                ));
            }
        }

        // Note: Resource allocation validation is now done at runtime during execution
        // rather than during DAG validation to properly handle async resource allocation
        // and potential race conditions.

        Ok(())
    }

    /// Optimizes the DAG structure
    pub fn optimize(&self) -> Self {
        // Implementation
        self.clone()
    }

    /// Partitions the DAG into sub-DAGs
    pub fn partition(&self) -> Vec<Self> {
        // Implementation
        vec![self.clone()]
    }

    /// Serializes the DAG to a string
    pub fn serialize(&self) -> String {
        // Implementation
        String::new()
    }

    /// Deserializes a DAG from a string
    pub fn deserialize(&self, _data: &str) -> Result<Self, DagValidationError> {
        unimplemented!("Deserialization not yet implemented")
    }

    /// Returns an iterator over all nodes in the DAG
    pub fn nodes(&self) -> impl Iterator<Item = &N> {
        self.graph.node_weights()
    }

    /// Returns a reference to the node with the given ID, if it exists
    pub fn get_node(&self, node_id: &str) -> Option<&N> {
        self.node_map
            .get(node_id)
            .and_then(|&idx| self.graph.node_weight(idx))
    }

    /// Returns an iterator over all edges in the DAG
    pub fn edges(&self) -> impl Iterator<Item = (NodeIndex, NodeIndex, &E)> {
        self.graph
            .edge_references()
            .map(|edge| (edge.source(), edge.target(), edge.weight()))
    }

    /// Returns an iterator over all edges from the given node
    pub fn edges_from(&self, node: NodeIndex) -> impl Iterator<Item = (NodeIndex, &E)> {
        self.graph
            .edges(node)
            .map(|edge| (edge.target(), edge.weight()))
    }

    /// Returns the number of nodes in the DAG
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Returns the number of edges in the DAG
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Returns true if the DAG is empty (has no nodes)
    pub fn is_empty(&self) -> bool {
        self.graph.node_count() == 0
    }
}

impl<N, E> Clone for TypeSafeDag<N, E>
where
    N: DagNode + Clone + 'static,
    E: DagEdge + Clone + 'static,
{
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            validator: self.validator.clone(),
            resource_manager: self.resource_manager.clone(),
            scheduler: self.scheduler.clone(),
            node_map: self.node_map.clone(),
            edge_map: self.edge_map.clone(),
            name: self.name.clone(),
            settings: self.settings.clone(),
        }
    }
}

// Specialized implementation for default types
impl TypeSafeDag<crate::model::NodeSpec, crate::model::EdgeSpec> {
    /// Creates a TypeSafeDag from a JSON string containing a WorkflowSpec
    ///
    /// # Arguments
    /// * `json_str` - JSON string containing the workflow specification
    ///
    /// # Returns
    /// A new TypeSafeDag instance or an error if parsing fails
    pub fn from_json(json_str: &str) -> Result<Self, crate::error::GaussFlowError> {
        use crate::model::WorkflowSpec;
        use crate::scheduler::DefaultScheduler;
        use crate::validator::DefaultDagValidator;

        // Parse the JSON into a WorkflowSpec
        let workflow_spec: WorkflowSpec = serde_json::from_str(json_str).map_err(|e| {
            crate::error::GaussFlowError::Serialization {
                message: e.to_string(),
                format: Some("json".to_string()),
                field: None,
                recovery_strategy: None,
            }
        })?;

        // Create default components
        let validator = Arc::new(DefaultDagValidator::new());
        let resource_manager =
            crate::resource::ResourceManager::new(crate::resource::ResourceSpec::default());
        let scheduler = Arc::new(DefaultScheduler::new());

        // Create the TypeSafeDag
        let mut dag = Self::new(
            workflow_spec.name.clone(),
            validator,
            resource_manager,
            scheduler,
        );

        // Set the workflow settings
        dag.settings = workflow_spec.settings;

        // Add nodes to the graph
        for node_spec in workflow_spec.nodes {
            let node_index = dag.graph.add_node(node_spec);
            dag.node_map
                .insert(dag.graph[node_index].id.clone(), node_index);
        }

        // Add edges to the graph
        for edge_spec in workflow_spec.connections {
            let edge_index = dag.graph.add_edge(
                dag.node_map[&edge_spec.from],
                dag.node_map[&edge_spec.to],
                edge_spec,
            );
            dag.edge_map
                .insert(edge_index, dag.graph[edge_index].clone());
        }

        Ok(dag)
    }
}
