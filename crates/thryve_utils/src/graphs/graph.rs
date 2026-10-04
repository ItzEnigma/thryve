// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Task topology graph for visualization.
//!
//! This module provides the data structures for representing and managing
//! the topology graph of task relationships. The graph is used to visualize
//! how tasks call each other and form execution hierarchies.
//!
//! # Architecture
//!
//! The topology graph uses [`petgraph`](https://docs.rs/petgraph)'s
//! [`StableGraph`] to represent
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
//! in the debugging configuration. The topology handler (`TopologyHandlerObserver`, in `thryve_observers`)
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

use petgraph::{
    dot::{Config, Dot},
    graph::{EdgeIndex, NodeIndex},
    stable_graph::StableGraph,
};
use serde::{Deserialize, Serialize};

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
/// - `failure`: Why graph mode turned off, set only on the failure node
///
/// # The failure node
///
/// When graph mode fails, the topology handler adds one last node, made by
/// [`TopologyNode::failure`]. It carries the reason in `failure`, and an edge leads to it from the
/// node where the failure happened, when that's known. Nothing is recorded after it, so a
/// viewer can show where the graph ends and why. Normal nodes don't have the field in the
/// exported file, so exports without a failure look exactly as before.
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
///     42,
///     "MyTask".to_string(),
/// );
///
/// let node = TopologyNode::new(callsite, "fingerprint_123".to_string());
/// ```
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyNode {
    /// The call site information for this node.
    pub callsite: TaskCallSite,
    /// A unique identifier for this node.
    pub fingerprint: String,
    /// Why graph mode turned off. Set only on the failure node; see [`TopologyNode::failure`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// The task topology graph.
///
/// `TaskTopology` maintains a graph of task call relationships.
/// It uses a [`StableGraph`] to
/// represent the topology, with nodes representing call sites and
/// edges representing task creation relationships.
///
/// # Fields
///
/// - `enabled`: Whether the graph still takes new nodes and edges
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
pub struct TaskTopology {
    /// Whether the graph still takes new nodes and edges.
    ///
    /// The topology handler starts it on. It turns off once the failure node is recorded, so the
    /// failure is the last thing in the graph.
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
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint) {
            self.graph.contains_node(node_index)
        } else {
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
        if let Some(&node_index_1) = self.graph_node_indices.get(from_fingerprint) {
            if let Some(&node_index_2) = self.graph_node_indices.get(to_fingerprint) {
                self.graph.contains_edge(node_index_1, node_index_2)
            } else {
                false
            }
        } else {
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
    /// A clone of the node if it exists, or a default `TopologyNode` otherwise (also when the
    /// stored index no longer points into the graph).
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
        self.node_index(fingerprint)
            .and_then(|node_index| self.graph.node_weight(node_index))
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the index of the node with the given fingerprint, if that node is in the graph.
    ///
    /// Unlike [`get_node_index`](Self::get_node_index), a missing node, or a stored index that no
    /// longer points into the graph, gives `None` instead of an index that belongs to another
    /// node.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - The fingerprint of the node
    ///
    /// # Example
    ///
    /// ```
    /// use thryve_utils::graphs::graph::TaskTopology;
    ///
    /// let topology = TaskTopology::default();
    /// assert!(topology.node_index("fingerprint_123").is_none());
    /// ```
    pub fn node_index(&self, fingerprint: &str) -> Option<NodeIndex> {
        self.graph_node_indices
            .get(fingerprint)
            .copied()
            .filter(|&node_index| self.graph.contains_node(node_index))
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
    pub fn get_edge(&self, from_fingerprint: &str, to_fingerprint: &str) -> EdgeIndex {
        self.graph
            .find_edge(
                self.get_node_index(from_fingerprint),
                self.get_node_index(to_fingerprint),
            )
            .unwrap_or_default()
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
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint) {
            node_index
        } else {
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
    pub fn print_topology(&self) {
        println!("number of topology nodes = {}", self.graph.node_count());
        println!(
            "{:?}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        );
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

impl TopologyNode {
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
    ///     42,
    ///     "MyTask".to_string(),
    /// );
    ///
    /// let node = TopologyNode::new(callsite, "fingerprint_123".to_string());
    /// ```
    pub fn new(callsite: TaskCallSite, fingerprint: String) -> Self {
        Self {
            callsite,
            fingerprint,
            failure: None,
        }
    }

    /// The fingerprint of the failure node. Real fingerprints are hashes, so it can't collide
    /// with one.
    pub const FAILURE_FINGERPRINT: &'static str = "graph_mode_failure";

    /// Creates the failure node, which records why graph mode turned off.
    ///
    /// # Arguments
    ///
    /// * `reason` - Why graph mode turned off
    ///
    /// # Example
    ///
    /// ```
    /// use thryve_utils::graphs::graph::TopologyNode;
    ///
    /// let node = TopologyNode::failure("the topology lock was poisoned".to_string());
    /// assert_eq!(node.fingerprint, TopologyNode::FAILURE_FINGERPRINT);
    /// assert_eq!(node.failure.as_deref(), Some("the topology lock was poisoned"));
    /// ```
    pub fn failure(reason: String) -> Self {
        Self {
            callsite: TaskCallSite::default(),
            fingerprint: Self::FAILURE_FINGERPRINT.to_string(),
            failure: Some(reason),
        }
    }
}
