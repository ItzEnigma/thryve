use std::{path::PathBuf, process::Command, sync::{Arc, RwLock, atomic::Ordering}, io::Write};

use ron::ser::PrettyConfig;

use thryve_utils::{bases::{notif::Notif, observer::Observer}, extras::dir_paths::get_workspace_root};
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::extras::{dir_paths::DirName, jobs::ObsJob};
use thryve_utils::graphs::graph::TaskTopology;
use thryve_utils::impl_as_any;
use thryve_utils::packets::{notif_log::Log, notif_topology::TopologyModifier};
use thryve_utils::traits::{observer_logic::ObsLogic, queue::Queue};



pub struct TopologyHandlerObserver{
    pub topology: RwLock<TaskTopology>,
    pub topology_dir: PathBuf,
}

impl ObsLogic for TopologyHandlerObserver {
    fn clean_up(&self, obs: Arc<Observer>) {
        while !obs.obs_queue.main_queue.is_empty() {
            if let Some(notif) = obs.obs_queue.main_queue.pop(){
                self.handle_entry(notif);
            }
        }

        let current_dir = self.topology_dir.clone();

        // Export topology to RON file
        let mut topology_write = self.topology.write().unwrap();

        // Configure pretty-printing options
        let pretty_config = PrettyConfig::new()
            .depth_limit(8)
            .indentor(String::from("    "))
            .new_line(String::from("\n"));

        // Serialize to RON string
        let ron_string = ron::ser::to_string_pretty(&topology_write.graph, pretty_config)
            .expect("Failed to serialize topology to RON");
        
        // Write to file
        let file_path = current_dir
            .join("topology_export.ron");

        std::fs::write(&file_path, ron_string)
            .expect("Failed to write topology file");

        topology_write.graph.clear();

        self.visualize_gui();
    }

    impl_as_any!(TopologyHandlerObserver);
}

impl TopologyHandlerObserver{
    pub fn new(ctx: &Ctx) -> Observer{
        let topo_handler = Arc::new(Self {
            topology: RwLock::new(TaskTopology::default()),
            topology_dir: ctx.meta_data.dir_data.get_dir_path(&DirName::Topology),
        });

        let topo_handler_job: ObsJob = Box::new(move |topo_handler: Arc<Observer>| {

            let topo_handler_as_any = topo_handler.obs_core.as_any();
            if let Some(topo_handler_core) = topo_handler_as_any.downcast_ref::<TopologyHandlerObserver>() {
                topo_handler_core.topology_modification(topo_handler.clone());
            }
        });

        // Return a new observer instance with the specified type and name.
        Observer::new("Topology", "TopologyHandler", topo_handler, topo_handler_job, ctx.packet_handle())

    }

    pub fn topology_modification(&self, observer: Arc<Observer>){
        loop {
            if observer.exit_flag.load(Ordering::SeqCst){
                let notif = Log::log("Topology Handler Obs Thread Exited...".to_string(), "Main");
                observer.tx.send_notification(notif);

                return;
            }

            let notif = observer.obs_queue.pop();

            self.handle_entry(notif);
        }
    }

    pub fn handle_entry(&self , notif: Notif){
        if notif.notif_receiver.inner() != "TopologyHandler" {
            return;
        }

        // Attempt to downcast the notification's core logic to a `TopologyModifier` type
        // to access its specific fields and functionality.
        let notif_as_any = notif.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<TopologyModifier>();

        // If the notification is successfully downcasted to a `TopologyModifier` type,
        // extract the log file name from the notification and log the message
        // to the appropriate file.
        let mut notif_core = notif_casted.expect(&format!("Notification Sent to TopologyHandler but is meant for someone else. Intended receiver: {}", notif.notif_receiver.inner())).clone();

        let from_fingerprint = notif_core.from_fingerprint.as_str();
        let to_fingerprint = notif_core.to_fingerprint.as_str();

        let mut topology_ref = self.topology.write().unwrap();

        if topology_ref.workspace_root == PathBuf::default() {
            topology_ref.workspace_root = get_workspace_root();
        }

        match notif_core.action.as_str() {
            "AddNode" => {
                notif_core.graph_node.callsite.file_path = notif_core.graph_node.callsite.file_path
                    .strip_prefix(topology_ref.workspace_root.clone())
                    .unwrap_or(&notif_core.graph_node.callsite.file_path).to_path_buf();

                let node_fingerprint = notif_core.graph_node.fingerprint.clone();

                if !topology_ref.contains_node(&node_fingerprint){
                    let node_index = topology_ref.graph.add_node(notif_core.graph_node);
                    topology_ref.graph_node_indices.insert(node_fingerprint,node_index);
                }
            },

            "AddEdge" => {
                let temp_from = topology_ref.contains_node(from_fingerprint);
                let temp_to = topology_ref.contains_node(to_fingerprint);

                if !temp_from || !temp_to{
                    panic!("Cannot add edge because a node is missing");
                }

                let temp_from = topology_ref.get_node_index(from_fingerprint);
                let temp_to = topology_ref.get_node_index(to_fingerprint);
                
                if !topology_ref.contains_edge(notif_core.from_fingerprint.as_str(), notif_core.to_fingerprint.as_str()){
                    topology_ref.graph.add_edge(temp_from, temp_to, 1);
                }

                else{
                    let edge_index = topology_ref.get_edge(from_fingerprint, to_fingerprint);
                    topology_ref.graph[edge_index] += 1;
                }

                
            },
            _ => {}
        }

    }
}

impl TopologyHandlerObserver {
    fn visualize_gui(&self) {
        if let Some(current_execution_dir) = self.topology_dir.clone().parent(){
            let command = format!("{}\\topology_test.exe" , current_execution_dir.to_str().unwrap());
            println!("Command: {}" , command);
            
            std::io::stdout().flush().unwrap();

            Command::new(command).spawn().expect("Failed to spawn GUI");

            println!("Process spawned and detached!");
        }
        else {
            println!("{}\\topology_test.exe" , self.topology_dir.clone().parent().unwrap().to_str().unwrap());
        }

    }
}