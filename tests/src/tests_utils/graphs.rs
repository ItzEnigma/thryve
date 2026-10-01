// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_callsite {
    use std::path::PathBuf;

    use thryve_utils::graphs::callsite::*;

    #[test]
    fn test_task_callsite_new_creates_callsite_with_correct_fields() {
        // Arrange
        let file_path = PathBuf::from("src/my_module.rs");
        let line_num = 42;
        let name = "MyTask".to_string();

        // Act
        let callsite = TaskCallSite::new(file_path.clone(), line_num, name.clone());

        // Assert
        assert_eq!(callsite.file_path, file_path);
        assert_eq!(callsite.line_number, line_num);
        assert_eq!(callsite.callsite_name, name);
        assert_eq!(callsite.this_callsite_fingerprint, String::default());
        assert_eq!(
            callsite.current_executing_task_fingerprint,
            String::default()
        );
    }

    #[test]
    fn test_task_callsite_new_with_empty_strings() {
        // Arrange
        let file_path = PathBuf::from("");
        let line_num = 0;
        let name = String::new();

        // Act
        let callsite = TaskCallSite::new(file_path, line_num, name);

        // Assert
        assert_eq!(callsite.file_path, PathBuf::from(""));
        assert_eq!(callsite.line_number, 0);
        assert_eq!(callsite.callsite_name, String::new());
    }

    #[test]
    fn test_task_callsite_new_with_unicode_characters() {
        // Arrange
        let file_path = PathBuf::from("src/模块.rs");
        let line_num = 100;
        let name = "我的任务".to_string();

        // Act
        let callsite = TaskCallSite::new(file_path.clone(), line_num, name.clone());

        // Assert
        assert_eq!(callsite.file_path, file_path);
        assert_eq!(callsite.line_number, line_num);
        assert_eq!(callsite.callsite_name, name);
    }

    #[test]
    fn test_task_callsite_new_with_large_line_number() {
        // Arrange
        let file_path = PathBuf::from("src/large_file.rs");
        let line_num = u32::MAX;
        let name = "LargeFileTask".to_string();

        // Act
        let callsite = TaskCallSite::new(file_path, line_num, name);

        // Assert
        assert_eq!(callsite.line_number, u32::MAX);
    }

    #[test]
    fn test_task_callsite_fingerprint_returns_non_empty_string() {
        // Arrange
        let callsite =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        // Act
        let fingerprint = callsite.fingerprint();

        // Assert
        assert!(!fingerprint.is_empty());
    }

    #[test]
    fn test_task_callsite_fingerprint_is_deterministic() {
        // Arrange
        let callsite =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        // Act
        let fingerprint1 = callsite.fingerprint();
        let fingerprint2 = callsite.fingerprint();

        // Assert
        assert_eq!(fingerprint1, fingerprint2);
    }

    #[test]
    fn test_task_callsite_fingerprint_different_for_different_callsites() {
        // Arrange
        let callsite1 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        let callsite2 = TaskCallSite::new(
            PathBuf::from("src/other_module.rs"),
            100,
            "OtherTask".to_string(),
        );

        // Act
        let fingerprint1 = callsite1.fingerprint();
        let fingerprint2 = callsite2.fingerprint();

        // Assert
        assert_ne!(fingerprint1, fingerprint2);
    }

    #[test]
    fn test_task_callsite_fingerprint_different_for_different_line_numbers() {
        // Arrange
        let callsite1 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        let callsite2 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 100, "MyTask".to_string());

        // Act
        let fingerprint1 = callsite1.fingerprint();
        let fingerprint2 = callsite2.fingerprint();

        // Assert
        assert_ne!(fingerprint1, fingerprint2);
    }

    #[test]
    fn test_task_callsite_fingerprint_different_for_different_task_names() {
        // Arrange
        let callsite1 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "TaskA".to_string());

        let callsite2 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "TaskB".to_string());

        // Act
        let fingerprint1 = callsite1.fingerprint();
        let fingerprint2 = callsite2.fingerprint();

        // Assert
        assert_ne!(fingerprint1, fingerprint2);
    }

    #[test]
    fn test_task_callsite_default_impl() {
        // Act
        let callsite = TaskCallSite::default();

        // Assert
        assert_eq!(callsite.file_path, PathBuf::default());
        assert_eq!(callsite.line_number, 0);
        assert_eq!(callsite.callsite_name, String::default());
        assert_eq!(callsite.this_callsite_fingerprint, String::default());
        assert_eq!(
            callsite.current_executing_task_fingerprint,
            String::default()
        );
    }

    #[test]
    fn test_task_callsite_clone_impl() {
        // Arrange
        let mut original =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());
        original.this_callsite_fingerprint = "fp123".to_string();
        original.current_executing_task_fingerprint = "parent_fp".to_string();

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.file_path, original.file_path);
        assert_eq!(cloned.line_number, original.line_number);
        assert_eq!(cloned.callsite_name, original.callsite_name);
        assert_eq!(
            cloned.this_callsite_fingerprint,
            original.this_callsite_fingerprint
        );
        assert_eq!(
            cloned.current_executing_task_fingerprint,
            original.current_executing_task_fingerprint
        );
    }

    #[test]
    fn test_task_callsite_partial_eq_same() {
        // Arrange
        let callsite1 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        let callsite2 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        // Assert
        assert_eq!(callsite1, callsite2);
    }

    #[test]
    fn test_task_callsite_partial_eq_different() {
        // Arrange
        let callsite1 =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());

        let callsite2 = TaskCallSite::new(
            PathBuf::from("src/other_module.rs"),
            100,
            "OtherTask".to_string(),
        );

        // Assert
        assert_ne!(callsite1, callsite2);
    }
}

mod tests_graph {
    use petgraph::graph::{EdgeIndex, NodeIndex};
    use std::path::PathBuf;

    use thryve_utils::graphs::callsite::TaskCallSite;
    use thryve_utils::graphs::graph::*;

    // Helper function to create a TopologyNode for tests
    fn create_test_node(fingerprint: &str) -> TopologyNode {
        TopologyNode::new(
            TaskCallSite::new(PathBuf::from("src/test.rs"), 42, "TestTask".to_string()),
            fingerprint.to_string(),
        )
    }

    // ===== TopologyNode tests =====

    #[test]
    fn test_topology_node_new_creates_node_with_correct_fields() {
        // Arrange
        let callsite =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());
        let fingerprint = "fingerprint_123".to_string();

        // Act
        let node = TopologyNode::new(callsite.clone(), fingerprint.clone());

        // Assert
        assert_eq!(node.callsite, callsite);
        assert_eq!(node.fingerprint, fingerprint);
    }

    #[test]
    fn test_topology_node_new_with_empty_fingerprint() {
        // Arrange
        let callsite =
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string());
        let fingerprint = String::new();

        // Act
        let node = TopologyNode::new(callsite, fingerprint.clone());

        // Assert
        assert_eq!(node.fingerprint, fingerprint);
    }

    #[test]
    fn test_topology_node_default_impl() {
        // Act
        let node = TopologyNode::default();

        // Assert
        assert_eq!(node.callsite, TaskCallSite::default());
        assert_eq!(node.fingerprint, String::default());
    }

    #[test]
    fn test_topology_node_clone_impl() {
        // Arrange
        let original = TopologyNode::new(
            TaskCallSite::new(PathBuf::from("src/my_module.rs"), 42, "MyTask".to_string()),
            "fingerprint_123".to_string(),
        );

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.callsite, original.callsite);
        assert_eq!(cloned.fingerprint, original.fingerprint);
    }

    // ===== TaskTopology tests =====

    #[test]
    fn test_task_topology_default_creates_empty_topology() {
        // Act
        let topology = TaskTopology::default();

        // Assert
        assert!(!topology.enabled);
        assert_eq!(topology.graph.node_count(), 0);
        assert_eq!(topology.graph.edge_count(), 0);
        assert_eq!(topology.graph_node_indices.len(), 0);
    }

    #[test]
    fn test_contains_node_returns_false_for_empty_topology() {
        // Arrange
        let topology = TaskTopology::default();

        // Act & Assert
        assert!(!topology.contains_node("nonexistent"));
    }

    #[test]
    fn test_contains_node_returns_true_for_existing_node() {
        // Arrange
        let mut topology = TaskTopology::default();
        let node = create_test_node("node_1");
        let node_index = topology.graph.add_node(node);
        topology
            .graph_node_indices
            .insert("node_1".to_string(), node_index);

        // Act & Assert
        assert!(topology.contains_node("node_1"));
    }

    #[test]
    fn test_contains_node_with_multiple_nodes() {
        // Arrange
        let mut topology = TaskTopology::default();
        let node1 = create_test_node("node_1");
        let node2 = create_test_node("node_2");
        let node3 = create_test_node("node_3");

        topology.graph.add_node(node1);
        topology
            .graph_node_indices
            .insert("node_1".to_string(), NodeIndex::new(0));
        topology.graph.add_node(node2);
        topology
            .graph_node_indices
            .insert("node_2".to_string(), NodeIndex::new(1));
        topology.graph.add_node(node3);
        topology
            .graph_node_indices
            .insert("node_3".to_string(), NodeIndex::new(2));

        // Act & Assert
        assert!(topology.contains_node("node_1"));
        assert!(topology.contains_node("node_2"));
        assert!(topology.contains_node("node_3"));
        assert!(!topology.contains_node("node_4"));
    }

    #[test]
    fn test_contains_edge_returns_false_for_empty_topology() {
        // Arrange
        let topology = TaskTopology::default();

        // Act & Assert
        assert!(!topology.contains_edge("from", "to"));
    }

    #[test]
    fn test_contains_edge_returns_false_when_source_missing() {
        // Arrange
        let mut topology = TaskTopology::default();
        topology.graph.add_node(create_test_node("to"));
        topology
            .graph_node_indices
            .insert("to".to_string(), NodeIndex::new(0));

        // Act & Assert
        assert!(!topology.contains_edge("from", "to"));
    }

    #[test]
    fn test_contains_edge_returns_false_when_dest_missing() {
        // Arrange
        let mut topology = TaskTopology::default();
        topology.graph.add_node(create_test_node("from"));
        topology
            .graph_node_indices
            .insert("from".to_string(), NodeIndex::new(0));

        // Act & Assert
        assert!(!topology.contains_edge("from", "to"));
    }

    #[test]
    fn test_contains_edge_returns_true_for_existing_edge() {
        // Arrange
        let mut topology = TaskTopology::default();
        let from_node = create_test_node("from");
        let to_node = create_test_node("to");

        let from_index = topology.graph.add_node(from_node);
        let to_index = topology.graph.add_node(to_node);
        topology.graph.add_edge(from_index, to_index, 1u64);

        topology
            .graph_node_indices
            .insert("from".to_string(), from_index);
        topology
            .graph_node_indices
            .insert("to".to_string(), to_index);

        // Act & Assert
        assert!(topology.contains_edge("from", "to"));
    }

    #[test]
    fn test_contains_edge_is_directional() {
        // Arrange
        let mut topology = TaskTopology::default();
        let from_node = create_test_node("from");
        let to_node = create_test_node("to");

        let from_index = topology.graph.add_node(from_node);
        let to_index = topology.graph.add_node(to_node);
        topology.graph.add_edge(from_index, to_index, 1u64);

        topology
            .graph_node_indices
            .insert("from".to_string(), from_index);
        topology
            .graph_node_indices
            .insert("to".to_string(), to_index);

        // Act & Assert
        assert!(topology.contains_edge("from", "to"));
        assert!(!topology.contains_edge("to", "from"));
    }

    #[test]
    fn test_get_node_returns_default_for_nonexistent_node() {
        // Arrange
        let topology = TaskTopology::default();

        // Act
        let node = topology.get_node("nonexistent");

        // Assert
        assert_eq!(node, TopologyNode::default());
    }

    #[test]
    fn test_get_node_returns_correct_node() {
        // Arrange
        let mut topology = TaskTopology::default();
        let expected_node = create_test_node("node_1");
        let node_index = topology.graph.add_node(expected_node.clone());
        topology
            .graph_node_indices
            .insert("node_1".to_string(), node_index);

        // Act
        let node = topology.get_node("node_1");

        // Assert
        assert_eq!(node.fingerprint, expected_node.fingerprint);
        assert_eq!(
            node.callsite.callsite_name,
            expected_node.callsite.callsite_name
        );
    }

    #[test]
    fn test_get_edge_returns_default_for_nonexistent_edge() {
        // Arrange
        let topology = TaskTopology::default();

        // Act
        let edge_index = topology.get_edge("from", "to");

        // Assert
        assert_eq!(edge_index, EdgeIndex::default());
    }

    #[test]
    fn test_get_edge_returns_correct_index() {
        // Arrange
        let mut topology = TaskTopology::default();
        let from_node = create_test_node("from");
        let to_node = create_test_node("to");

        let from_index = topology.graph.add_node(from_node);
        let to_index = topology.graph.add_node(to_node);
        let edge_index = topology.graph.add_edge(from_index, to_index, 42u64);

        topology
            .graph_node_indices
            .insert("from".to_string(), from_index);
        topology
            .graph_node_indices
            .insert("to".to_string(), to_index);

        // Act
        let result_edge_index = topology.get_edge("from", "to");

        // Assert
        assert_eq!(result_edge_index, edge_index);
    }

    #[test]
    fn test_get_node_index_returns_default_for_nonexistent_node() {
        // Arrange
        let topology = TaskTopology::default();

        // Act
        let node_index = topology.get_node_index("nonexistent");

        // Assert
        assert_eq!(node_index, NodeIndex::default());
    }

    #[test]
    fn test_get_node_index_returns_correct_index() {
        // Arrange
        let mut topology = TaskTopology::default();
        let expected_index = NodeIndex::new(5);
        let node = create_test_node("node_1");

        // Add enough nodes so we can test a specific index
        for i in 0..5 {
            topology
                .graph
                .add_node(create_test_node(&format!("node_{}", i)));
        }
        topology.graph.add_node(node);
        topology
            .graph_node_indices
            .insert("node_1".to_string(), expected_index);

        // Act
        let node_index = topology.get_node_index("node_1");

        // Assert
        assert_eq!(node_index, expected_index);
    }

    #[test]
    fn test_extract_graph_returns_graph_and_clears_it() {
        // Arrange
        let mut topology = TaskTopology::default();
        let node = create_test_node("node_1");
        topology.graph.add_node(node);
        topology
            .graph_node_indices
            .insert("node_1".to_string(), NodeIndex::new(0));

        // Act
        let extracted_graph = topology.extract_graph();

        // Assert
        assert_eq!(extracted_graph.node_count(), 1);
        assert_eq!(topology.graph.node_count(), 0);
    }

    #[test]
    fn test_extract_graph_preserves_nodes_and_edges() {
        // Arrange
        let mut topology = TaskTopology::default();
        let node1 = create_test_node("node_1");
        let node2 = create_test_node("node_2");

        let index1 = topology.graph.add_node(node1);
        let index2 = topology.graph.add_node(node2);
        topology.graph.add_edge(index1, index2, 10u64);

        topology
            .graph_node_indices
            .insert("node_1".to_string(), index1);
        topology
            .graph_node_indices
            .insert("node_2".to_string(), index2);

        // Act
        let extracted_graph = topology.extract_graph();

        // Assert
        assert_eq!(extracted_graph.node_count(), 2);
        assert_eq!(extracted_graph.edge_count(), 1);
    }

    #[test]
    fn test_print_topology_does_not_panic() {
        // Arrange
        let mut topology = TaskTopology::default();
        let node1 = create_test_node("node_1");
        let node2 = create_test_node("node_2");

        let index1 = topology.graph.add_node(node1);
        let index2 = topology.graph.add_node(node2);
        topology.graph.add_edge(index1, index2, 1u64);

        // Act & Assert - should not panic
        topology.print_topology();
    }
}
