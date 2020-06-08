use crate::pipelines::pipeline::Config;
use std::collections::HashMap;

pub fn one_instance() -> usize {
    1
}

pub fn empty_hash() -> String {
    String::from("")
}

pub fn empty_config() -> Config {
    Config {
        config_files: HashMap::new(),
        config: HashMap::new(),
    }
}

pub fn none_usize() -> Option<usize> {
    None
}

pub fn none_config_files_all() -> Option<HashMap<String, Vec<String>>> {
    None
}
