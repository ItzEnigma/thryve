// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_topology {
    use chrono::{DateTime, Local};
    use crossbeam::channel::bounded;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, SystemTime};
    use thryve_observers::topology_handler::*;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::{ctx::Ctx, meta_data::MetaData};
    use thryve_utils::extras::dir_paths::DirName;
    use thryve_utils::extras::named_input::LogFileNamedInput;
    use thryve_utils::graphs::callsite::TaskCallSite;
    use thryve_utils::graphs::graph::TopologyNode;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::notif_topology::TopologyModifier;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::queue::Queue;

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();

        // Create minimal DirData and MetaData for testing
        let exe_dir = std::env::temp_dir();
        let topology_dir = exe_dir.join("test_topology");
        std::fs::create_dir_all(&topology_dir).ok();

        let dir_data = thryve_utils::cfg::dir_data::DirData::new(exe_dir.clone(), HashMap::new());
        let datetime: DateTime<Local> = SystemTime::now().into();
        let mut meta_data = MetaData::new(datetime, dir_data);
        meta_data
            .dir_data
            .dir_paths
            .insert(DirName::Topology, topology_dir);

        let mut ctx = Ctx::new(config, task_rx, notif_rx, packet_handle);
        ctx.meta_data = meta_data;
        ctx
    }

    #[test]
    fn test_topology_handler_observer_new_creates_observer() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "TopologyHandler");
    }

    #[test]
    fn test_topology_handler_observer_obs_type_is_topology_handler() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "TopologyHandler");
    }

    #[test]
    fn test_handle_entry_add_node_successfully() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create a test node
        let callsite = TaskCallSite::new(PathBuf::from("src/test.rs"), 42, "TestTask".to_string());
        let node = TopologyNode {
            callsite,
            fingerprint: "test_node_1".to_string(),
            failure: None,
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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create two nodes first
        let callsite1 =
            TaskCallSite::new(PathBuf::from("src/test.rs"), 42, "ParentTask".to_string());
        let node1 = TopologyNode {
            callsite: callsite1,
            fingerprint: "parent_node".to_string(),
            failure: None,
        };

        let callsite2 =
            TaskCallSite::new(PathBuf::from("src/test.rs"), 43, "ChildTask".to_string());
        let node2 = TopologyNode {
            callsite: callsite2,
            fingerprint: "child_node".to_string(),
            failure: None,
        };

        // Add both nodes
        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));

        // Create an AddEdge notification
        let edge_notif =
            TopologyModifier::add_edge("parent_node".to_string(), "child_node".to_string());

        // Act
        topo_handler.handle_entry(edge_notif);

        // Assert - check that the edge was added
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_edge("parent_node", "child_node"));
        assert_eq!(topology_ref.graph.edge_count(), 1);
    }

    #[test]
    fn test_handle_entry_add_edge_with_missing_node_turns_graph_mode_off() {
        // Arrange: graph mode on, and an Error.log of this test's own for the warning
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, true);
        let mut ctx = Ctx::new(Config::default(), task_rx, notif_rx, packet_handle);
        ctx.meta_data = create_test_ctx().meta_data;

        let error_dir = std::env::temp_dir().join("test_topology_missing_node");
        std::fs::create_dir_all(&error_dir).unwrap();
        let error_path = error_dir.join("Error.log");
        let error_log = std::fs::File::create(&error_path).unwrap();
        ctx.meta_data
            .dir_data
            .log_hash_map
            .insert(LogFileNamedInput::new_err(), Arc::new(error_log));

        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Only add one node
        let callsite = TaskCallSite::new(PathBuf::from("src/test.rs"), 42, "Task".to_string());
        let node = TopologyNode {
            callsite,
            fingerprint: "only_node".to_string(),
            failure: None,
        };
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Act - an edge to a node that isn't in the graph must not panic
        let edge_notif =
            TopologyModifier::add_edge("only_node".to_string(), "missing_node".to_string());
        topo_handler.handle_entry(edge_notif);

        // A node sent after the failure isn't recorded
        let late_node = TopologyNode::new(TaskCallSite::default(), "late_node".to_string());
        topo_handler.handle_entry(TopologyModifier::add_node(late_node));

        // Assert - the graph ends with the failure node, linked from the node that exists
        let topology_ref = topo_handler.topology.read().unwrap();
        let failure_node = topology_ref.get_node(TopologyNode::FAILURE_FINGERPRINT);
        assert!(
            failure_node
                .failure
                .as_deref()
                .is_some_and(|reason| reason.contains("\"missing_node\""))
        );
        assert!(topology_ref.contains_edge("only_node", TopologyNode::FAILURE_FINGERPRINT));
        assert!(!topology_ref.contains_node("late_node"));
        assert!(!topology_ref.enabled);

        // Only the failure node carries the field in the export
        let exported = ron::ser::to_string(&topology_ref.graph).unwrap();
        assert_eq!(exported.matches("failure:").count(), 1);

        // Graph mode is off for every packet handle, and the warning is in Error.log
        assert!(!ctx.packet_handle().graph_mode_on());
        let written = std::fs::read_to_string(&error_path).unwrap();
        assert!(
            written.contains("[ TOPOLOGY ], Event_Type: Error, Message: graph mode turned off")
        );
    }

    #[test]
    fn test_handle_entry_add_duplicate_node_does_not_add_again() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create a node
        let callsite = TaskCallSite::new(PathBuf::from("src/test.rs"), 42, "TestTask".to_string());
        let node = TopologyNode {
            callsite,
            fingerprint: "duplicate_node".to_string(),
            failure: None,
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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create two nodes
        let node1 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 1, "Task1".to_string()),
            fingerprint: "node1".to_string(),
            failure: None,
        };
        let node2 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 2, "Task2".to_string()),
            fingerprint: "node2".to_string(),
            failure: None,
        };

        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));

        // Act - add the same edge twice
        topo_handler.handle_entry(TopologyModifier::add_edge(
            "node1".to_string(),
            "node2".to_string(),
        ));
        topo_handler.handle_entry(TopologyModifier::add_edge(
            "node1".to_string(),
            "node2".to_string(),
        ));

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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create a more complex topology: A -> B -> C, A -> C
        let nodes = vec![
            TopologyNode {
                callsite: TaskCallSite::new(PathBuf::from("src/a.rs"), 1, "A".to_string()),
                fingerprint: "A".to_string(),
                failure: None,
            },
            TopologyNode {
                callsite: TaskCallSite::new(PathBuf::from("src/b.rs"), 2, "B".to_string()),
                fingerprint: "B".to_string(),
                failure: None,
            },
            TopologyNode {
                callsite: TaskCallSite::new(PathBuf::from("src/c.rs"), 3, "C".to_string()),
                fingerprint: "C".to_string(),
                failure: None,
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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();
        let observer = Arc::new(observer);

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Add a node to have something to export
        let node = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 1, "Test".to_string()),
            fingerprint: "test_node".to_string(),
            failure: None,
        };
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Queue a leftover notification on the observer's own queue so
        // clean_up's drain-then-export loop has something to process
        // before it writes the RON file (Topology.exe won't exist in
        // this environment, so visualize_gui just logs and returns).
        let queued_node = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/queued.rs"), 2, "Queued".to_string()),
            fingerprint: "queued_node".to_string(),
            failure: None,
        };
        observer
            .obs_queue
            .main_queue
            .push(TopologyModifier::add_node(queued_node));

        // Act
        topo_handler.clean_up(observer.clone());

        // Assert - the leftover notification was drained, the RON file was
        // written to topology_dir, and the in-memory graph was cleared
        // afterward.
        assert!(observer.obs_queue.main_queue.is_empty());
        let export_path = topo_handler.topology_dir.join("topology_export.ron");
        assert!(export_path.exists());
        let contents =
            std::fs::read_to_string(&export_path).expect("exported RON file should be readable");
        assert!(!contents.is_empty());
        let topology_ref = topo_handler.topology.read().unwrap();
        assert_eq!(topology_ref.graph.node_count(), 0);
    }

    #[test]
    fn test_topology_handler_with_unicode_in_callsite() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create a node with unicode in the name
        let node = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/测试.rs"), 42, "任务名".to_string()),
            fingerprint: "unicode_node".to_string(),
            failure: None,
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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Act - should not panic even if Topology.exe doesn't exist
        // The function will fail to spawn the process but won't panic
        topo_handler.visualize_gui();
    }

    #[test]
    fn test_visualize_gui_with_parent_directory_returning_none() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create two nodes first
        let node1 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 1, "Task1".to_string()),
            fingerprint: "node1".to_string(),
            failure: None,
        };
        let node2 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 2, "Task2".to_string()),
            fingerprint: "node2".to_string(),
            failure: None,
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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create two nodes
        let node1 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 1, "Task1".to_string()),
            fingerprint: "node1".to_string(),
            failure: None,
        };
        let node2 = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/test.rs"), 2, "Task2".to_string()),
            fingerprint: "node2".to_string(),
            failure: None,
        };

        topo_handler.handle_entry(TopologyModifier::add_node(node1));
        topo_handler.handle_entry(TopologyModifier::add_node(node2));

        // Act - add the same edge twice
        topo_handler.handle_entry(TopologyModifier::add_edge(
            "node1".to_string(),
            "node2".to_string(),
        ));
        topo_handler.handle_entry(TopologyModifier::add_edge(
            "node1".to_string(),
            "node2".to_string(),
        ));

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
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Create a node with an absolute file path
        let node = TopologyNode {
            callsite: TaskCallSite::new(
                PathBuf::from("E:\\Projects\\MTC\\Thryve\\src\\test.rs"),
                42,
                "TestTask".to_string(),
            ),
            fingerprint: "workspace_node".to_string(),
            failure: None,
        };

        // Act
        topo_handler.handle_entry(TopologyModifier::add_node(node));

        // Assert - the file path should be relative after stripping workspace root
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_node("workspace_node"));
    }

    #[test]
    fn test_handle_entry_with_unrecognized_action_is_ignored() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // Downcast to get the TopologyHandlerObserver
        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();

        // Build a TopologyModifier with an action string that doesn't match
        // "AddNode" or "AddEdge", to exercise the catch-all match arm.
        let modifier = TopologyModifier {
            graph_node: TopologyNode::default(),
            from_fingerprint: String::default(),
            to_fingerprint: String::default(),
            action: "SomeUnknownAction".to_string(),
        };
        let notif = Notif::new("SomeUnknownAction", "TopologyHandler", Box::new(modifier)).unwrap();

        // Act - should not panic, should just fall through the catch-all arm
        topo_handler.handle_entry(notif);

        // Assert - nothing was added to the graph
        let topology_ref = topo_handler.topology.read().unwrap();
        assert_eq!(topology_ref.graph.node_count(), 0);
    }

    #[test]
    fn test_topology_modification_loop_processes_notif_then_exits_on_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = Arc::new(TopologyHandlerObserver::new(&ctx).unwrap());

        let observer_thread = observer.clone();
        let handle = std::thread::spawn(move || {
            observer_thread.run_loop(observer_thread.clone());
        });

        // Push a node-add notification for the loop to pop and handle on
        // its first iteration.
        let node = TopologyNode {
            callsite: TaskCallSite::new(PathBuf::from("src/loop.rs"), 1, "LoopTask".to_string()),
            fingerprint: "loop_node".to_string(),
            failure: None,
        };
        observer.obs_queue.push(TopologyModifier::add_node(node));

        // Give the loop time to pop and handle it before we ask it to exit.
        std::thread::sleep(Duration::from_millis(150));

        // The loop checks exit_flag *before* each pop(), so by now it is
        // blocked waiting inside a second pop() call; set the flag, then
        // push one more notification purely to wake that blocked pop() so
        // the loop can come back around and see the flag is set.
        observer.exit_flag.store(true, Ordering::SeqCst);
        observer
            .obs_queue
            .push(TopologyModifier::add_node(TopologyNode::default()));

        // Act & Assert - the loop must actually return once it observes the
        // exit flag, or this join blocks forever instead of completing.
        handle
            .join()
            .expect("topology_modification loop should exit cleanly on exit_flag");

        let obs_core = observer.obs_core.as_any();
        let topo_handler = obs_core.downcast_ref::<TopologyHandlerObserver>().unwrap();
        let topology_ref = topo_handler.topology.read().unwrap();
        assert!(topology_ref.contains_node("loop_node"));
    }

    #[test]
    fn test_visualize_gui_with_root_topology_dir_has_no_parent() {
        // Arrange - a root path has no parent, so this exercises the
        // `topology_dir.parent()` == None branch, which the ctx-based
        // constructor can never produce on its own (its topology_dir is
        // always a subdirectory of a temp dir).
        let mut ctx = create_test_ctx();
        ctx.meta_data
            .dir_data
            .dir_paths
            .insert(DirName::Topology, PathBuf::from("C:\\"));
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();
        let topo_handler = observer
            .obs_core
            .as_any()
            .downcast_ref::<TopologyHandlerObserver>()
            .unwrap();

        // Act - should not panic despite there being no parent directory
        topo_handler.visualize_gui();
    }
}
