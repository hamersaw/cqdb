extern crate strsim;

use std::collections::{HashMap,HashSet,LinkedList};

pub fn query_field(field_name: &str, filter_type: &str, params: Vec<&str>, field_value: &str, fields: &HashMap<String,HashMap<String,LinkedList<u64>>>) -> HashSet<u64> {
    let mut entity_keys = HashSet::new();
    if fields.contains_key(&field_name[..]) {
        let field_values = fields.get(&field_name[..]).unwrap();

        //match comparator type
        match filter_type {
            "damerau_levenshtein" => {
                let max_distance = params[0].parse::<usize>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if strsim::damerau_levenshtein(value, field_value) <= max_distance {
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
            "hamming" => {
                let max_distance = params[0].parse::<usize>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    match strsim::hamming(value, field_value) {
                        Ok(hamming_distance) => {
                            if hamming_distance <= max_distance {
                                for entity_key in entity_key_list {
                                    entity_keys.insert(*entity_key);
                                }
                            }
                        },
                        Err(_) => {},
                    }
                }
            }
            "jaro" => {
                let min_score = params[0].parse::<f64>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if strsim::jaro(value, field_value) >= min_score {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "jaro_winkler" => {
                let min_score = params[0].parse::<f64>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if strsim::jaro_winkler(value, field_value) >= min_score {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "levenshtein" => {
                let max_distance = params[0].parse::<usize>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if strsim::levenshtein(value, field_value) <= max_distance {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            "ngram" => {
                let ngram_size = params[0].parse::<usize>().unwrap();
                let min_score = params[1].parse::<f32>().unwrap();

                for (value, entity_key_list) in field_values.iter() {
                    if ngram(value, field_value, ngram_size) >= min_score {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            /*"soundex" => {
                for (value, entity_key_list) in field_values.iter() {
                    if ruzzy::soundex::soundex(value, field_value) {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },*/
            _ => println!("Unknown filter type {}", filter_type),
        }
    }

    entity_keys
}

fn ngram(s1: &str, s2: &str, size: usize) -> f32 {
    if s1.len() == 0 || s2.len() == 0 {
        return 0.0
    }

    //loop through first string add unique ngrams to vec
    let mut ngrams = Vec::new();
    for ngram in compute_ngram_tokens(s1, size) {
        if !ngrams.contains(&ngram) {
            ngrams.push(ngram);
        }
    }

    //loop through second string
    let mut intersection = 0;
    let mut difference = 0;
    for ngram in compute_ngram_tokens(s2, size) {
        if ngrams.contains(&ngram) {
            intersection += 1
        } else {
            difference += 1;
        }
    }

    intersection as f32 / ((ngrams.len() as i32 + difference) as f32)
}

fn compute_ngram_tokens(s: &str, size: usize) -> Vec<&str> {
    let mut tokens = Vec::new();
    for i in 0..(s.len() - size + 1) {
        unsafe {
            tokens.push(s.slice_unchecked(i, i + size));
        }
    }

    tokens
}
