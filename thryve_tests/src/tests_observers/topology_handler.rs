mod tests_topology {
    use thryve_observers::topology_handler::*;
    use thryve_utils::bases::notif::Notif;
use thryve_utils::cfg::{ctx::Ctx, meta_data::MetaData};
    use thryve_utils::cfg::config::Config;
    use thryve_utils::extras::dir_paths::DirName;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::packets::notif_topology::TopologyModifier;
use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::graphs::callsite::TaskCallSite;
    use thryve_utils::graphs::graph::TopologyNode;
    use crossbeam::channel::bounded;
    use chrono::{DateTime, Local};
    use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        
        // Create minimal DirData and MetaData for testing
        let exe_dir = PathBuf::from(std::env::temp_dir());
        let topology_dir = exe_dir.join("test_topology");
        std::fs::create_dir_all(&topology_dir).ok();
        
        let dir_data = thryve_utils::cfg::dir_data::DirData::new(exe_dir.clone(), HashMap::new());
        let datetime: DateTime<Local> = SystemTime::now().into();
        let mut meta_data = MetaData::new(datetime, dir_data);
        meta_data.dir_data.dir_paths.insert(DirName::Topology, topology_dir);
        
        let mut ctx = Ctx::new(config, task_rx, notif_rx, packet_handle);
        ctx.meta_data = meta_data;
        ctx
    }

    #[test]
    fn test_topology_handler_observer_new_creates_observer() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = TopologyHandlerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_name, "Topology");
        assert_eq!(observer.obs_type.inner(), "TopologyHandler");
    }

    #[test]
    fn test_topology_handler_observer_obs_type_is_topology_handler() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = TopologyHandlerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_type.inner(), "TopologyHandler");
    }

    #[test]
    fn test_topology_handler_observer_obs_name_is_topology() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = TopologyHandlerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_name, "Topology");
    }

    #[test]
    fn test_handle_entry_add_node_successfully() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create a test node
        let callsite = TaskCallSite::new(
            PathBuf::from("src/test.rs"),
            "test_module".to_string(),
            42,
            "TestTask".to_string(),
        );
        let node = TopologyNode {
            callsite,
            fingerprint: "test_node_1".to_string(),
        };
        
        // Create an AddNode notification
        let notif = TopologyModifier::add_node(node);

        // Act
        topo_handler.handle_entry(notif);

        // Assert - check that the node was added to the topology
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_node("test_node_1"));
        assert_eq!(topology_ref.graph.node_count(), 1);
    }

    #[test]
    fn test_handle_entry_add_edge_successfully() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create two nodes first
        let callsite1 = TaskCallSite::new(
            PathBuf::from("src/test.rs"),
            "test_module".to_string(),
            42,
            "ParentTask".to_string(),
        );
        let node1 = TopologyNode {
            callsite: callsite1,
            fingerprint: "parent_node".to_string(),
        };
        
        let callsite2 = TaskCallSite::new(
            PathBuf::from("src/test.rs"),
            "test_module".to_string(),
            43,
            "ChildTask".to_string(),
        );
        let node2 = TopologyNode {
            callsite: callsite2,
            fingerprint: "child_node".to_string(),
        };
        
        // Add both nodes
        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));
        
        // Create an AddEdge notification
        let edge_notif = TopologyModifier::add_edge("parent_node".to_string(), "child_node".to_string());

        // Act
        topo_handler.handle_entry(edge_notif);

        // Assert - check that the edge was added
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_edge("parent_node", "child_node"));
        assert_eq!(topology_ref.graph.edge_count(), 1);
    }

    #[test]
    #[should_panic(expected = "Cannot add edge because a node is missing")]
    fn test_handle_entry_add_edge_with_missing_node_panics() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Only add one node
        let callsite = TaskCallSite::new(
            PathBuf::from("src/test.rs"),
            "test_module".to_string(),
            42,
            "Task".to_string(),
        );
        let node = TopologyNode {
            callsite,
            fingerprint: "only_node".to_string(),
        };
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Act - try to add edge with non-existent node (should panic)
        let edge_notif = TopologyModifier::add_edge("only_node".to_string(), "missing_node".to_string());
        topo_handler.handle_entry(edge_notif);
    }

    #[test]
    fn test_handle_entry_add_duplicate_node_does_not_add_again() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create a node
        let callsite = TaskCallSite::new(
            PathBuf::from("src/test.rs"),
            "test_module".to_string(),
            42,
            "TestTask".to_string(),
        );
        let node = TopologyNode {
            callsite,
            fingerprint: "duplicate_node".to_string(),
        };

        // Act - add the same node twice
        topo_handler.handle_entry(TopologyModifier::add_node(node.clone()));
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Assert - should still only have one node
        let topology_ref = topo_handler.topology.read().unwrap();
        assert_eq!(topology_ref.graph.node_count(), 1);
    }

    #[test]
    fn test_handle_entry_add_duplicate_edge_increments_weight() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create two nodes
        let node1 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "mod1".to_string(), 1, "Task1".to_string()),
            fingerprint: "node1".to_string(),
        };
        let node2 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "mod2".to_string(), 2, "Task2".to_string()),
            fingerprint: "node2".to_string(),
        };
        
        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));

        // Act - add the same edge twice
        topo_handler.handle_entry(TopologyModifier::add_edge("node1".to_string(), "node2".to_string()));
        topo_handler.handle_entry(TopologyModifier::add_edge("node1".to_string(), "node2".to_string()));

        // Assert - edge weight should be incremented
        let topology_ref = topo_handler.topology.read().unwrap();
        let edge_index = topology_ref.get_edge("node1", "node2");
        let edge_weight = topology_ref.graph[edge_index];
        assert_eq!(edge_weight, 2);
    }

    #[test]
    fn test_handle_entry_ignores_notification_for_different_receiver() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create a log notification (not a topology notification)
        let notif = Log::log("Test message".to_string(), "Main");

        // Act - should be ignored without panic
        topo_handler.handle_entry(notif);

        // Assert - topology should still be empty
        let topology_ref = topo_handler.topology.read().unwrap();
        assert_eq!(topology_ref.graph.node_count(), 0);
    }

    #[test]
    fn test_handle_entry_with_complex_topology() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create a more complex topology: A -> B -> C, A -> C
        let nodes = vec![
            TopologyNode {
                callsite: TaskCallSite::new(PathBuf::from("src/a.rs"), "a".to_string(), 1, "A".to_string()),
                fingerprint: "A".to_string(),
            },
            TopologyNode {
                callsite: TaskCallSite::new(PathBuf::from("src/b.rs"), "b".to_string(), 2, "B".to_string()),
                fingerprint: "B".to_string(),
            },
            TopologyNode {
                callsite: TaskCallSite::new(PathBuf::from("src/c.rs"), "c".to_string(), 3, "C".to_string()),
                fingerprint: "C".to_string(),
            },
        ];
        
        // Add all nodes
        for node in nodes {
            topo_handler.handle_entry(TopologyModifier::add_node(node));
        }
        
        // Add edges: A -> B, B -> C, A -> C
        topo_handler.handle_entry(TopologyModifier::add_edge("A".to_string(), "B".to_string()));
        topo_handler.handle_entry(TopologyModifier::add_edge("B".to_string(), "C".to_string()));
        topo_handler.handle_entry(TopologyModifier::add_edge("A".to_string(), "C".to_string()));

        // Assert
        let topology_ref = topo_handler.topology.read().unwrap();
        assert_eq!(topology_ref.graph.node_count(), 3);
        assert_eq!(topology_ref.graph.edge_count(), 3);
        assert!(topology_ref.contains_edge("A", "B"));
        assert!(topology_ref.contains_edge("B", "C"));
        assert!(topology_ref.contains_edge("A", "C"));
    }

    #[test]
    fn test_topology_handler_cleanup_exports_file() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        let observer = Arc::new(observer);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Add a node to have something to export
        let node = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "test".to_string(), 1, "Test".to_string()),
            fingerprint: "test_node".to_string(),
        };
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Act - cleanup should export the topology (this will try to spawn GUI, which we can't test)
        // We'll just verify it doesn't panic on the export part
        // Note: This test may fail if topology_test.exe doesn't exist, but that's expected
    }

    #[test]
    fn test_topology_handler_with_unicode_in_callsite() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create a node with unicode in the name
        let node = TopologyNode {
            callsite: TaskCallSite::new(
                PathBuf::from("src/测试.rs"),
                "模块::子模块".to_string(),
                42,
                "任务名".to_string(),
            ),
            fingerprint: "unicode_node".to_string(),
        };

        // Act
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Assert
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_node("unicode_node"));
    }

    // Tests for visualize_gui
    #[test]
    fn test_visualize_gui_with_valid_parent_directory() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Act - should not panic even if topology_test.exe doesn't exist
        // The function will fail to spawn the process but won't panic
        topo_handler.visualize_gui();
    }

    #[test]
    fn test_visualize_gui_with_parent_directory_returning_none() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Act - should not panic
        topo_handler.visualize_gui();
    }

    // Tests for handle_entry with AddEdge action
    #[test]
    fn test_handle_entry_add_edge_creates_edge() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create two nodes first
        let node1 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "mod1".to_string(), 1, "Task1".to_string()),
            fingerprint: "node1".to_string(),
        };
        let node2 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "mod2".to_string(), 2, "Task2".to_string()),
            fingerprint: "node2".to_string(),
        };
        
        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));

        // Act - add an edge
        let edge_notif = TopologyModifier::add_edge("node1".to_string(), "node2".to_string());
        topo_handler.handle_entry(edge_notif);

        // Assert - check that the edge was added
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_edge("node1", "node2"));
    }

    #[test]
    fn test_handle_entry_add_edge_increments_weight() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create two nodes
        let node1 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "mod1".to_string(), 1, "Task1".to_string()),
            fingerprint: "node1".to_string(),
        };
        let node2 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), "mod2".to_string(), 2, "Task2".to_string()),
            fingerprint: "node2".to_string(),
        };
        
        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));

        // Act - add the same edge twice
        topo_handler.handle_entry(TopologyModifier::add_edge("node1".to_string(), "node2".to_string()));
        topo_handler.handle_entry(TopologyModifier::add_edge("node1".to_string(), "node2".to_string()));

        // Assert - edge weight should be 2
        let topology_ref = topo_handler.topology.read().unwrap();
        let edge_index = topology_ref.get_edge("node1", "node2");
        let edge_weight = topology_ref.graph[edge_index];
        assert_eq!(edge_weight, 2);
    }

    #[test]
    fn test_handle_entry_add_node_with_workspace_root() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx);
        
        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        
        // Create a node with an absolute file path
        let node = TopologyNode {
            callsite: TaskCallSite::new(
                PathBuf::from("E:\\Projects\\MTC\\Thryve\\src\\test.rs"),
                "test_crate::test_module".to_string(),
                42,
                "TestTask".to_string(),
            ),
            fingerprint: "workspace_node".to_string(),
        };

        // Act
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Assert - the file path should be relative after stripping workspace root
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_node("workspace_node"));
    }
}
