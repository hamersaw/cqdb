extern crate argparse;
use argparse::{ArgumentParser,Store};

extern crate capnp;

extern crate cqdb;
use cqdb::message_capnp;
use cqdb::message_capnp::message::msg_type::{InsertEntityMsg,EntityMsg,EntityTokensMsg,QueryMsg,QueryEntityMsg,QueryFieldMsg,WriteEntityMsg,WriteFieldMsg};

extern crate rustp2p;
use rustp2p::omniscient::event::Event;

use std::collections::{BTreeMap,HashMap,HashSet,LinkedList};
use std::hash::{Hash,Hasher,SipHasher};
use std::io::{Read,Write};
use std::net::{Ipv4Addr,SocketAddrV4,TcpListener,TcpStream};
use std::str::FromStr;
use std::sync::{Arc,RwLock};
use std::thread;

pub fn main() {
    let mut id = "World".to_string();
    let mut token: u64 = 0;
    let mut app_ip: String = "127.0.0.1".to_string();
    let mut app_port: u16 = 0;
    let mut service_port: u16 = 0;
    let mut seed_ip: String = "127.0.0.1".to_string();
    let mut seed_port: u16 = 0;
    {    //solely to limit scope of parser variable
        let mut parser = ArgumentParser::new();
        parser.set_description("start up a echo server");
        parser.refer(&mut id).add_option(&["-i", "--id"], Store, "id of node").required();
        parser.refer(&mut token).add_option(&["-t", "--token"], Store, "token of node").required();
        parser.refer(&mut app_ip).add_option(&["-l", "--listen-ip"], Store, "ip address for application and service to listen on").required();
        parser.refer(&mut app_port).add_option(&["-a", "--app-port"], Store, "port for application to listen on").required();
        parser.refer(&mut service_port).add_option(&["-p", "--service-port"], Store, "port for the p2p service listen on").required();
        parser.refer(&mut seed_ip).add_option(&["-s", "--seed-ip"], Store, "p2p service seed node ip address");
        parser.refer(&mut seed_port).add_option(&["-e", "--seed-port"], Store, "p2p service seed node port");
        parser.parse_args_or_exit();
    }

    //create application and service addresses
    let ip = Ipv4Addr::from_str(&app_ip[..]).unwrap();
    let app_addr = SocketAddrV4::new(ip, app_port);
    let service_addr = SocketAddrV4::new(ip, service_port);

    //create seed address
    let seed_addr = match seed_port {
        0 => None,
        _ => {
            let seed_ip = Ipv4Addr::from_str(&seed_ip[..]).unwrap();
            Some(SocketAddrV4::new(seed_ip, seed_port))
        }
    };

    //create application specific variables
    let lookup_table = Arc::new(RwLock::new(BTreeMap::new()));
    let entities: Arc<RwLock<HashMap<u64,HashMap<String,String>>>> = Arc::new(RwLock::new(HashMap::new()));
    let fields: Arc<RwLock<HashMap<String,HashMap<String,LinkedList<u64>>>>> = Arc::new(RwLock::new(HashMap::new()));

    //start up the p2p service
    let rx = rustp2p::omniscient::service::start(id, token, app_addr, service_addr, seed_addr, lookup_table.clone());

    //start listening on the application
    let lookup_table = lookup_table.clone();
    let entities = entities.clone();
    let fields = fields.clone();
    let listener = TcpListener::bind(app_addr).unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let lookup_table = lookup_table.clone();
            let entities = entities.clone();
            let fields = fields.clone();

            thread::spawn(move || {
                let mut stream = stream.unwrap();

                //read capnproto message
                let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                //parse out message
                match msg.get_msg_type().which() {
                    Ok(InsertEntityMsg(insert_entity_msg)) => {
                        //compute hash over all fields
                        let mut concat_string = String::new();
                        for field in insert_entity_msg.get_fields().unwrap().iter() {
                            concat_string = concat_string + field.get_value().unwrap();
                        }
                        let mut hasher = SipHasher::new();
                        concat_string.hash(&mut hasher);
                        let entity_token = hasher.finish();

                        //lookup into peer table
                        let lookup_table = lookup_table.read().unwrap();
                        let socket_addr = match rustp2p::omniscient::service::lookup(&lookup_table, entity_token) {
                            Some(socket_addr) => socket_addr,
                            None => panic!("error in looking up token in the peer table"),
                        };

                        //create write entity message
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            let mut write_entity_msg = msg.get_msg_type().init_write_entity_msg();
                            write_entity_msg.set_entity_token(entity_token);
                            write_entity_msg.set_fields(insert_entity_msg.get_fields().unwrap()).unwrap();
                        }

                        //send write entity message
                        let mut stream = TcpStream::connect(socket_addr).unwrap();
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                        //send write field value message
                        for field in insert_entity_msg.get_fields().unwrap().iter() {
                            //compute hash of field value
                            let mut hasher = SipHasher::new();
                            field.get_value().unwrap().hash(&mut hasher);
                            let field_token = hasher.finish();

                            //lookup into peer table
                            let socket_addr = match rustp2p::omniscient::service::lookup(&lookup_table, field_token) {
                                Some(socket_addr) => socket_addr,
                                None => panic!("error in looking up token in the peer table"),
                            };

                            //create write field message
                            let mut msg_builder = capnp::message::Builder::new_default();
                            {
                                let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                let mut write_field_msg = msg.get_msg_type().init_write_field_msg();
                                write_field_msg.set_entity_token(entity_token);
                                write_field_msg.set_field(field).unwrap();
                            }

                            //send write field message
                            let mut stream = TcpStream::connect(socket_addr).unwrap();
                            capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                        }
                    },
                    Ok(QueryMsg(query_msg)) => {
                        let filter_tokens: Arc<RwLock<HashSet<u64>>> = Arc::new(RwLock::new(HashSet::new()));
                        let mut entity_token_set = HashSet::new();
                        let mut first_iteration = true;

                        //submit filter queries
                        for filter in query_msg.get_filters().unwrap().iter() {
                            //clear filter tokens
                            {
                                let mut filter_tokens = filter_tokens.write().unwrap();
                                filter_tokens.clear();
                            }

                            //send messages to all peers - TODO start a new thread for each request to improve speed - send simultaneous requests
                            {
                                //create query field message
                                let mut msg_builder = capnp::message::Builder::new_default();
                                {
                                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                    let mut query_field_msg = msg.get_msg_type().init_query_field_msg();
                                    query_field_msg.set_filter(filter).unwrap();
                                }

                                let lookup_table = lookup_table.read().unwrap();
                                for (_, peer_socket_addr) in lookup_table.iter() {
                                    let mut stream = TcpStream::connect(peer_socket_addr).unwrap();
                                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                                    //read entity tokens message
                                    let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                                    let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                                    //parse out message
                                    match msg.get_msg_type().which() {
                                        Ok(EntityTokensMsg(entity_tokens_msg)) => {
                                            //add to entity tokens list
                                            let mut filter_tokens = filter_tokens.write().unwrap();
                                            let entity_tokens = entity_tokens_msg.get_entity_tokens().unwrap();
                                            println!("query filter found {} token(s)", entity_tokens.len());
                                            for i in 0..entity_tokens.len() {
                                                filter_tokens.insert(entity_tokens.get(i));
                                            }
                                        },
                                        Ok(_) => panic!("Unknown message type"),
                                        Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                                    }
                                }
                            }

                            //update entity token set
                            if first_iteration {
                                let filter_tokens = filter_tokens.read().unwrap();
                                for token in filter_tokens.iter() {
                                    entity_token_set.insert(*token);
                                }
                                first_iteration = false;
                            } else {
                                let filter_tokens = filter_tokens.read().unwrap();
                                let diff: HashSet<u64> = entity_token_set.difference(&filter_tokens).cloned().collect();
                                for token in diff {
                                    entity_token_set.remove(&token);
                                }
                            }

                            //if no tokens then no need to loop through more filters
                            if entity_token_set.is_empty() {
                                break;
                            }
                        }

                        //create entities message
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            let entities_msg = msg.get_msg_type().init_entities_msg();
                            let mut entities_msg_list = entities_msg.init_entities(entity_token_set.len() as u32);

                            //send requests for each entity - TODO spawn a separate thread for each to improve performance
                            let mut count = 0;
                            for token in entity_token_set {
                                println!("TODO return entity with key {}", token);
                                //lookup token
                                let lookup_table = lookup_table.read().unwrap();
                                let socket_addr = match rustp2p::omniscient::service::lookup(&lookup_table, token) {
                                    Some(socket_addr) => socket_addr,
                                    None => panic!("Unable to find token in peer table"),
                                };

                                //create query entity message
                                let mut msg_bldr = capnp::message::Builder::new_default();
                                {
                                    let msg = msg_bldr.init_root::<message_capnp::message::Builder>();
                                    let mut query_entity_msg = msg.get_msg_type().init_query_entity_msg();
                                    query_entity_msg.set_entity_token(token);
                                }
                            
                                //send query entity message
                                let mut stream = TcpStream::connect(socket_addr).unwrap();
                                capnp::serialize::write_message(&mut stream, &msg_bldr).unwrap();

                                //read entity tokens message
                                let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                                let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                                //parse out message
                                match msg.get_msg_type().which() {
                                    Ok(EntityMsg(entity_msg)) => {
                                        let fields = entity_msg.get_fields().unwrap();
                                        let mut entity = entities_msg_list.borrow().get(count);
                                        entity.set_fields(fields).unwrap();

                                        for field in fields.iter() {
                                            println!("{}: {}", field.get_key().unwrap(), field.get_value().unwrap());
                                        }
                                    },
                                    Ok(_) => panic!("Unknown message type"),
                                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                                }

                                count += 1;
                            }
                        }

                        //send entities message
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                    },
                    Ok(QueryEntityMsg(query_entity_msg)) => {
                        //search for entity
                        let entities = entities.read().unwrap();
                        let entity_fields = entities.get(&query_entity_msg.get_entity_token()).unwrap();

                        //create entity message
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            let entity_msg = msg.get_msg_type().init_entity_msg();
                            let mut fields = entity_msg.init_fields(entity_fields.len() as u32);
                            let mut count = 0;
                            for (key, value) in entity_fields {
                                let mut field = fields.borrow().get(count);
                                field.set_key(key);
                                field.set_value(value);
                                count += 1;
                            }
                        }

                        //send entity message
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                    },
                    Ok(QueryFieldMsg(query_field_msg)) => {
                        let filter = query_field_msg.get_filter().unwrap();

                        //execute matching to fields
                        let fields = fields.read().unwrap();
                        let fieldname = filter.get_field_key().unwrap();
                        let field_value = filter.get_value().unwrap();
                        let mut entity_keys = HashSet::new();

                        if fields.contains_key(&fieldname[..]) {
                            let field_values = fields.get(&fieldname[..]).unwrap();

                            //match comparator type
                            match filter.get_comparator().unwrap() {
                                "equality" => {
                                    for (value, list) in field_values.iter() {
                                        if value == field_value {
                                            for key in list {
                                                entity_keys.insert(key);
                                            }
                                        }
                                    }
                                },
                                _ => panic!("Unknown comparator type {}", filter.get_comparator().unwrap()),
                            }
                        }
                        
                        //create entity tokens
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            let entity_tokens_msg = msg.get_msg_type().init_entity_tokens_msg();
                            let mut entity_tokens = entity_tokens_msg.init_entity_tokens(entity_keys.len() as u32);

                            let mut count = 0;
                            for token in entity_keys {
                                entity_tokens.set(count, *token);
                                count+=1;
                            }
                        }

                        //send entity tokens message
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                    },
                    Ok(WriteEntityMsg(write_entity_msg)) => {
                        //create entity hash map
                        let mut entity = HashMap::new();
                        for field in  write_entity_msg.get_fields().unwrap().iter() {
                            entity.insert(field.get_key().unwrap().to_string(), field.get_value().unwrap().to_string());
                        }

                        //insert entity into entities
                        let mut entities = entities.write().unwrap();
                        entities.insert(write_entity_msg.get_entity_token(), entity);
                    },
                    Ok(WriteFieldMsg(write_field_msg)) => {
                        //get field_values HashMap<String,[]u64>
                        let mut fields = fields.write().unwrap();
                        let field = write_field_msg.get_field().unwrap();
                        let fieldname = field.get_key().unwrap();
                        
                        if !fields.contains_key(&fieldname[..]) {
                            fields.insert(fieldname.to_string(), HashMap::new());
                        }
                        
                        let mut field_values = fields.get_mut(&fieldname[..]).unwrap();

                        //add token for message
                        let value = field.get_value().unwrap();
                        if !field_values.contains_key(&value[..]) {
                            field_values.insert(value.to_string(), LinkedList::new());
                        }

                        let mut entity_tokens = field_values.get_mut(&value[..]).unwrap();
                        entity_tokens.push_back(write_field_msg.get_entity_token());
                    },
                    Ok(_) => panic!("Unknown message type"),
                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                };
            });
        }
    });

    //listen for events from the p2p service
    while let Ok(event) = rx.recv() {
        match event {
            Event::LookupMsgEvent(token) => {
                println!("recv LookupMsgEvent({})", token);
            },
            Event::LookupTableMsgEvent(lookup_table) => {
                println!("PeerTableMsgEvent");
                for (token, socket_addr) in lookup_table.iter() {
                    println!("{}: {}", token, socket_addr);
                }
            },
            Event::RegisterTokenMsgEvent(token, socket_addr) => {
                println!("recv RegisterTokenMsgEvent({}, {})", token, socket_addr);
            },
            //_ => println!("not processing this event type"),
        }
    }
}
