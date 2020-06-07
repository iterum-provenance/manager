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
        config: HashMap::new(),
    }
}

pub fn empty_array() -> Vec<String> {
    Vec::new()
}
