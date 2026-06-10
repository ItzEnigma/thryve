use std::{collections::HashMap, path::PathBuf};

use petgraph::{dot::{Config, Dot}, graph::{EdgeIndex, NodeIndex}, stable_graph::StableGraph};
use serde::{Serialize, Deserialize};

use crate::graphs::callsite::TaskCallSite;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct TopologyNode {
    pub callsite: TaskCallSite,
    pub fingerprint: String,
}

#[derive(Default, Serialize, Deserialize)]
pub struct TaskTopology{
    pub enabled: bool,
    pub graph: StableGraph<TopologyNode, u64>,
    pub graph_node_indices: HashMap<String , NodeIndex>,
    pub workspace_root: PathBuf,
}

impl TaskTopology {
    pub fn contains_node(&self , fingerprint: &str) -> bool {
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint){
            self.graph.contains_node(node_index)
        }

        else {
            false
        }
    }

    pub fn contains_edge(&self , from_fingerprint: &str , to_fingerprint: &str) -> bool {
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

    pub fn get_node(&self , fingerprint: &str) -> TopologyNode {
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint){
            self.graph[node_index].clone()
        }

        else {
            TopologyNode::default()
        }
    }

    pub fn get_edge(&self, from_fingerprint: &str, to_fingerprint: &str) -> EdgeIndex{
        if let Some(edge_index) = self.graph.find_edge(self.get_node_index(from_fingerprint), self.get_node_index(to_fingerprint)) {
            edge_index
        }

        else {
            EdgeIndex::default()
        }
    }

    pub fn get_node_index(&self , fingerprint: &str) -> NodeIndex {
        if let Some(&node_index) = self.graph_node_indices.get(fingerprint){
            node_index
        }

        else {
            NodeIndex::default()
        }
    }

    pub fn print_topology(&self){
        println!("number of topology nodes = {}", self.graph.node_count());
        println!("{:?}", Dot::with_config(&self.graph, &[Config::EdgeNoLabel]));
    }

    
    pub fn extract_graph(&mut self) -> StableGraph<TopologyNode , u64> {
        std::mem::take(&mut self.graph)
    }
}

impl TopologyNode{
    pub fn new(callsite: TaskCallSite , fingerprint: String) -> Self{
        Self {
            callsite,
            fingerprint
        }
    }
}