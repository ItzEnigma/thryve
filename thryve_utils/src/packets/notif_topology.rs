//! Topology modification notifications.
//!
//! This module provides the [`TopologyModifier`] struct, which is used to
//! send notifications about changes to the task topology graph. These
//! notifications are sent to the [`TopologyHandlerObserver`](crate::topology_handler::TopologyHandlerObserver)
//! to update the graph visualization.
//!
//! # Usage
//!
//! Topology modifiers are created using the `add_node` and `add_edge`
//! factory methods, which return fully constructed notifications ready
//! to be sent to the engine.
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::packets::notif_topology::TopologyModifier;
//! use thryve_utils::graphs::graph::TopologyNode;
//!
//! // Create a notification to add a node
//! let notif = TopologyModifier::add_node(my_node);
//!
//! // Create a notification to add an edge
//! let edge_notif = TopologyModifier::add_edge("from_fp", "to_fp");
//! ```

use crate::{bases::notif::Notif, graphs::graph::TopologyNode, impl_as_any, traits::notif_logic::NotifLogic};

/// A notification that modifies the task topology graph.
///
/// `TopologyModifier` is used to communicate changes to the task topology
/// graph. It supports two types of modifications:
///
/// - **AddNode**: Adds a new node to the graph representing a call site
/// - **AddEdge**: Adds an edge between two nodes representing a task creation relationship
///
/// # Fields
///
/// - `graph_node`: The node to add (used for AddNode action)
/// - `from_fingerprint`: The source node fingerprint (used for AddEdge action)
/// - `to_fingerprint`: The destination node fingerprint (used for AddEdge action)
/// - `action`: The type of modification ("AddNode" or "AddEdge")
///
/// # Example
///
/// ```no_run
/// use thryve_utils::packets::notif_topology::TopologyModifier;
/// use thryve_utils::graphs::graph::TopologyNode;
///
/// let notif = TopologyModifier::add_node(my_node);
/// ```
#[derive(Clone)]
pub struct TopologyModifier {
    /// The graph node to be sent to the graph observer.
    pub graph_node: TopologyNode,
    /// The source node fingerprint (used for AddEdge action).
    pub from_fingerprint: String,
    /// The destination node fingerprint (used for AddEdge action).
    pub to_fingerprint: String,
    /// The type of modification ("AddNode" or "AddEdge").
    pub action: String
}

impl NotifLogic for TopologyModifier {
    impl_as_any!(TopologyNode);
}

impl TopologyModifier {
    /// Creates a notification to add a node to the topology graph.
    ///
    /// # Arguments
    ///
    /// * `node` - The topology node to add
    ///
    /// # Returns
    ///
    /// A new `Notif` containing the topology modifier.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::notif_topology::TopologyModifier;
    /// use thryve_utils::graphs::graph::TopologyNode;
    ///
    /// let notif = TopologyModifier::add_node(my_node);
    /// ```
    pub fn add_node(node: TopologyNode) -> Notif {
        let action = "AddNode";

        let temp = Box::new(TopologyModifier {
            graph_node: node,
            from_fingerprint: String::default(),
            to_fingerprint: String::default(),
            action: action.to_string()
        });

        Notif::new(action, "TopologyHandler", temp)
    }

    /// Creates a notification to add an edge to the topology graph.
    ///
    /// # Arguments
    ///
    /// * `from_node` - The fingerprint of the source node
    /// * `to_node` - The fingerprint of the destination node
    ///
    /// # Returns
    ///
    /// A new `Notif` containing the topology modifier.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::notif_topology::TopologyModifier;
    ///
    /// let notif = TopologyModifier::add_edge("from_fp", "to_fp");
    /// ```
    pub fn add_edge(from_node: String, to_node: String) -> Notif {
        let action = "AddEdge";

        let temp = Box::new(TopologyModifier {
            graph_node: TopologyNode::default(),
            from_fingerprint: from_node,
            to_fingerprint: to_node,
            action: action.to_string()
        });

        Notif::new(action, "TopologyHandler", temp)
    }
}
