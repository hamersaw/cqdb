use std::collections::{HashMap,HashSet,LinkedList};

pub fn query_field(filter_type: &str, field_name: &str, field_value: &str, fields: &HashMap<String,HashMap<String,LinkedList<u64>>>) -> HashSet<u64> {
    let mut entity_keys = HashSet::new();
    if fields.contains_key(&field_name[..]) {
        let field_values = fields.get(&field_name[..]).unwrap();

        //match comparator type
        match filter_type {
            "equality" => {
                for (value, entity_key_list) in field_values.iter() {
                    if value == field_value {
                        for entity_key in entity_key_list {
                            entity_keys.insert(*entity_key);
                        }
                    }
                }
            },
            _ => panic!("Unknown filter type {}", filter_type),
        }
    }

    entity_keys
}
