use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use serde::{Serialize , Deserialize};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TaskCallSite {
    pub file_path: PathBuf,
    pub module_path: String,
    pub line_number: u32,
    pub callsite_name: String,
    pub this_callsite_fingerprint: String,
    pub current_executing_task_fingerprint: String,
}

impl TaskCallSite {
    pub fn new(file_path: PathBuf , module_path: String , line_num: u32 , name: String) -> Self {
        let absolute_path = file_path;

        Self {
            file_path: absolute_path,
            module_path: module_path,
            line_number: line_num,
            callsite_name: name,
            this_callsite_fingerprint: String::default(),
            current_executing_task_fingerprint: String::default(),
        }
    }

    pub fn fingerprint(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);

        let fingerprint = hasher.finish().to_string();

        fingerprint
    }
}