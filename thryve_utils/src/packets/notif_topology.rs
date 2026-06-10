use crate::{bases::notif::Notif, graphs::graph::TopologyNode, impl_as_any, traits::notif_logic::NotifLogic};

#[derive(Clone)]
pub struct TopologyModifier {
    /// The graph node to be sent to the graph observer.
    pub graph_node: TopologyNode,
    pub from_fingerprint: String,
    pub to_fingerprint: String,
    pub action: String
}

impl NotifLogic for TopologyModifier {
    impl_as_any!(TopologyNode);
}

impl TopologyModifier {
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

    pub fn add_edge(from_node: String , to_node: String) -> Notif {
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