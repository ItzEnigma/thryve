//! Task topology graph for visualization.
//!
//! This module provides the data structures for representing and managing
//! the topology graph of task relationships. The graph is used to visualize
//! how tasks call each other and form execution hierarchies.
//!
//! # Architecture
//!
//! The topology graph uses [`petgraph`](https://docs.rs/petgraph)'s
//! [`StableGraph`](petgraph::stable_graph::StableGraph) to represent
//! the task call hierarchy:
//!
//! - **Nodes** ([`TopologyNode`]): Represent individual call sites where
//!   tasks are created
//! - **Edges**: Represent the relationship between a parent task and a child
//!   task it creates
//! - **Edge weights**: Represent the number of times a particular call path
//!   has been taken
//!
//! # Usage
//!
//! The graph is automatically populated when graph visualization is enabled
//! in the debugging configuration. The [`TopologyHandlerObserver`](crate::topology_handler::TopologyHandlerObserver)
//! receives notifications about new nodes and edges and maintains the graph.
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::graphs::graph::TaskTopology;
//!
//! let mut topology = TaskTopology::default();
//! // Graph is populated automatically during execution
//! topology.print_topology();
//! ```

use std::{collections::HashMap, path::PathBuf};

use petgraph::{dot::{Config, Dot}, graph::{EdgeIndex, NodeIndex}, stable_graph::StableGraph};
use serde::{Serialize, Deserialize};

use crate::graphs::callsite::TaskCallSite;

/// A node in the task topology graph.
///
/// A `TopologyNode` represents a single call site where a task is created.
/// Each node contains the call site information and a unique fingerprint
/// that identifies this node in the graph.
///
/// # Fields
///
/// - `callsite`: The call site information for this node
/// - `fingerprint`: A unique identifier for this node
///
/// # Example
///
/// ```no_run
/// use thryve_utils::graphs::graph::TopologyNode;
/// use thryve_utils::graphs::callsite::TaskCallSite;
/// use std::path::PathBuf;
///
/// let callsite = TaskCallSite::new(
///     PathBuf::from("src/my_module.rs"),
///     "my_crate::my_module".to_string(),
///     42,
///     "MyTask".to_string(),
/// );
///
/// let node = TopologyNode::new(callsite, "fingerprint_123".to_string());
/// ```
#[derive(Clone, Default, Debug, Serialize, Deserialize , PartialEq, Eq)]
pub struct TopologyNode {
    /// The call site information for this node.
    pub callsite: TaskCallSite,
    /// A unique identifier for this node.
    pub fingerprint: String,
}

/// The task topology graph.
///
/// `TaskTopology` maintains a graph of task call relationships.
/// It uses a [`StableGraph`](petgraph::stable_graph::StableGraph) to
/// represent the topology, with nodes representing call sites and
/// edges representing task creation relationships.
///
/// # Fields
///
/// - `enabled`: Whether topology tracking is enabled
/// - `graph`: The underlying petgraph StableGraph
/// - `graph_node_indices`: A map from fingerprints to node indices for O(1) lookup
/// - `workspace_root`: The workspace root path for relative path computation
///
/// # Example
///
/// ```no_run
/// use thryve_utils::graphs::graph::TaskTopology;
///
/// let topology = TaskTopology::default();
/// // Graph is populated automatically during execution
/// ```
#[derive(Default, Serialize, Deserialize)]
pub struct TaskTopology{
    /// Whether topology tracking is enabled.
    pub enabled: bool,
    /// The underlying petgraph StableGraph.
    pub graph: StableGraph<TopologyNode, u64>,
    /// A map from fingerprints to node indices for O(1) lookup.
    pub graph_node_indices: HashMap<String, NodeIndex>,
    /// The workspace root path for relative path computation.
    pub workspace_root: PathBuf,
}

impl TaskTopology {
    /// Checks if a node with the given fingerprint exists in the graph.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - The fingerprint of the node to check
    ///
    /// # Returns
    ///
    /// `true` if the node exists, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// let exists = topology.contains_node("fingerprint_123");
    /// ```
    pub fn contains_node(&self, fingerprint: &str) -> bool {
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint){
            self.graph.contains_node(node_index)
        }

        else {
            false
        }
    }

    /// Checks if an edge exists between two nodes.
    ///
    /// # Arguments
    ///
    /// * `from_fingerprint` - The fingerprint of the source node
    /// * `to_fingerprint` - The fingerprint of the destination node
    ///
    /// # Returns
    ///
    /// `true` if the edge exists, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// let exists = topology.contains_edge("from_fp", "to_fp");
    /// ```
    pub fn contains_edge(&self, from_fingerprint: &str, to_fingerprint: &str) -> bool {
        if let Some(&node_index_1) = self.graph_node_indices.get(from_fingerprint){
            if let Some(&node_index_2) = self.graph_node_indices.get(to_fingerprint){
                self.graph.contains_edge(node_index_1, node_index_2)
            }
            else {
                false
            }
        }


        else {
            false
        }
    }

    /// Retrieves a node by its fingerprint.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - The fingerprint of the node to retrieve
    ///
    /// # Returns
    ///
    /// A clone of the node if it exists, or a default `TopologyNode` otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// let node = topology.get_node("fingerprint_123");
    /// ```
    pub fn get_node(&self, fingerprint: &str) -> TopologyNode {
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint){
            self.graph[node_index].clone()
        }

        else {
            TopologyNode::default()
        }
    }

    /// Retrieves the edge index between two nodes.
    ///
    /// # Arguments
    ///
    /// * `from_fingerprint` - The fingerprint of the source node
    /// * `to_fingerprint` - The fingerprint of the destination node
    ///
    /// # Returns
    ///
    /// The edge index if it exists, or a default `EdgeIndex` otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// let edge_index = topology.get_edge("from_fp", "to_fp");
    /// ```
    pub fn get_edge(&self, from_fingerprint: &str, to_fingerprint: &str) -> EdgeIndex{
        if let Some(edge_index) = self.graph.find_edge(self.get_node_index(from_fingerprint), self.get_node_index(to_fingerprint)) {
            edge_index
        }

        else {
            EdgeIndex::default()
        }
    }

    /// Retrieves the node index for a given fingerprint.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - The fingerprint of the node
    ///
    /// # Returns
    ///
    /// The node index if it exists, or a default `NodeIndex` otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// let node_index = topology.get_node_index("fingerprint_123");
    /// ```
    pub fn get_node_index(&self, fingerprint: &str) -> NodeIndex {
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint){
            node_index
        }

        else {
            NodeIndex::default()
        }
    }

    /// Prints the topology graph to stdout.
    ///
    /// Outputs the number of nodes and a DOT representation of the graph
    /// for debugging purposes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// topology.print_topology();
    /// ```
    pub fn print_topology(&self){
        println!("number of topology nodes = {}", self.graph.node_count());
        println!("{:?}", Dot::with_config(&self.graph, &[Config::EdgeNoLabel]));
    }

    /// Extracts the underlying graph, leaving an empty graph in its place.
    ///
    /// This method consumes the graph data and returns it, replacing it
    /// with an empty graph. This is useful for transferring ownership
    /// of the graph data for serialization or further processing.
    ///
    /// # Returns
    ///
    /// The extracted graph.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let mut topology = TaskTopology::default();
    /// let graph = topology.extract_graph();
    /// ```
    pub fn extract_graph(&mut self) -> StableGraph<TopologyNode, u64> {
        std::mem::take(&mut self.graph)
    }
}

impl TopologyNode{
    /// Creates a new topology node with the specified call site and fingerprint.
    ///
    /// # Arguments
    ///
    /// * `callsite` - The call site information for this node
    /// * `fingerprint` - A unique identifier for this node
    ///
    /// # Returns
    ///
    /// A new `TopologyNode` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::graph::TopologyNode;
    /// use thryve_utils::graphs::callsite::TaskCallSite;
    /// use std::path::PathBuf;
    ///
    /// let callsite = TaskCallSite::new(
    ///     PathBuf::from("src/my_module.rs"),
    ///     "my_crate::my_module".to_string(),
    ///     42,
    ///     "MyTask".to_string(),
    /// );
    ///
    /// let node = TopologyNode::new(callsite, "fingerprint_123".to_string());
    /// ```
    pub fn new(callsite: TaskCallSite, fingerprint: String) -> Self{
        Self {
            callsite,
            fingerprint
        }
    }
}