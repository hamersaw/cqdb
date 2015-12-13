extern crate ruzzy;

use std::collections::{HashMap,HashSet,LinkedList};

pub fn query_field(field_name: &str, filter_type: &str, params: Vec<&str>, field_value: &str, fields: &HashMap<String,HashMap<String,LinkedList<u64>>>) -> HashSet<u64> {
    let mut entity_keys = HashSet::new();
    if fields.contains_key(&field_name[..]) {
        let field_values = fields.get(&field_name[..]).unwrap();

        //match comparator type
        match filter_type {
            "damerau_levenshtein" => {
                let max_distance = params[0].parse::<u16>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::damerau_levenshtein::compare(value, field_value) <= max_distance {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "equality" => {
                for (value, entity_key_list) in field_values.iter() {
                    if value == field_value {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "jaro" => {
                let min_score = params[0].parse::<f64>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::jaro::compare(value, field_value) >= min_score {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "jaro_winkler" => {
                let scaling_factor = params[0].parse::<f32>().unwrap();
                let min_score = params[1].parse::<f64>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::jaro_winkler::compare(value, field_value, scaling_factor) >= min_score {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "levenshtein" => {
                let max_distance = params[0].parse::<u16>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::levenshtein::compare(value, field_value) <= max_distance {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "ngram" => {
                let ngram_size = params[0].parse::<usize>().unwrap();
                let min_score = params[1].parse::<f64>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::ngram::compare(value, field_value, ngram_size) >= min_score {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "soundex" => {
                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::soundex::compare(value, field_value) {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            _ => println!("Unknown filter type {}", filter_type),
        }
    }

    entity_keys
}
