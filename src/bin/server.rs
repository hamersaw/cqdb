extern crate argparse;
use argparse::{ArgumentParser,Store,StoreTrue};

extern crate capnp;

extern crate fuzzydb;
use fuzzydb::message_capnp;
use fuzzydb::message_capnp::message::msg_type::{CloseWriteStreamMsg,InsertEntitiesMsg,EntityMsg,EntityKeysMsg,OpenWriteStreamMsg,QueryMsg,QueryEntityMsg,QueryFilterMsg,WriteEntityMsg,WriteFieldMsg};

extern crate rustdht;
use rustdht::zero_hop::event::Event;

use std::collections::{BTreeMap,HashMap,HashSet,LinkedList};
use std::hash::{Hash,Hasher,SipHasher};
use std::io::{Read,Write};
use std::net::{Ipv4Addr,SocketAddrV4,Shutdown,TcpListener,TcpStream};
use std::str::FromStr;
use std::sync::{Arc,Mutex,RwLock};
use std::sync::mpsc::channel;
use std::thread;

pub fn main() {
    let mut id = "World".to_string();
    let mut token: u64 = 0;
    let mut app_ip: String = "127.0.0.1".to_string();
    let mut app_port: u16 = 0;
    let mut service_port: u16 = 0;
    let mut seed_ip: String = "127.0.0.1".to_string();
    let mut seed_port: u16 = 0;
    let mut debug = false;
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
        parser.refer(&mut debug).add_option(&["-d", "--debug"], StoreTrue, "print debug output");
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
    let (debug_tx, debug_rx) = channel::<String>();
    let arc_debug_tx = Arc::new(Mutex::new(debug_tx));

    //listen on debug channel
    thread::spawn(move || {
        while let Ok(msg) = debug_rx.recv() {
            if debug {
                println!("debug: {}", msg);
            }
        }
    });

    //start up the p2p service
    let dht_rx = rustdht::zero_hop::service::start(id, token, app_addr, service_addr, seed_addr, lookup_table.clone());

    //start listening on the application
    let (lookup_table, entities, fields, arc_debug_tx_closure) = (lookup_table.clone(), entities.clone(), fields.clone(), arc_debug_tx.clone());
    let listener = TcpListener::bind(app_addr).unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let (lookup_table, entities, fields, arc_debug_tx) = (lookup_table.clone(), entities.clone(), fields.clone(), arc_debug_tx_closure.clone());

            thread::spawn(move || {
                let mut stream = stream.unwrap();

                //read capnproto message
                let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                //parse out message
                match msg.get_msg_type().which() {
                    Ok(InsertEntitiesMsg(insert_entities_msg)) => {
                        let mut streams = HashMap::new();
                        for entity in insert_entities_msg.unwrap().iter() {
                            //compute hash over all fields
                            let mut hasher = SipHasher::new();
                            for field in entity.get_fields().unwrap().iter() {
                                field.get_value().unwrap().hash(&mut hasher);
                            }
                            let entity_key = hasher.finish();

                            //lookup into peer table
                            let lookup_table = lookup_table.read().unwrap();
                            let socket_addr = rustdht::zero_hop::service::lookup(&lookup_table, entity_key).unwrap();

                            //create write entity message
                            let mut msg_builder = capnp::message::Builder::new_default();
                            {
                                let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                let mut write_entity_msg = msg.get_msg_type().init_write_entity_msg();
                                write_entity_msg.set_entity_key(entity_key);
                                write_entity_msg.set_fields(entity.get_fields().unwrap()).unwrap();
                            }

                            //send write entity message
                            {
                                let stream = streams.entry(socket_addr).or_insert_with(|| {
                                    let mut stream = TcpStream::connect(socket_addr).unwrap();

                                    let mut msg_builder = capnp::message::Builder::new_default();
                                    {
                                        let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                        msg.get_msg_type().set_open_write_stream_msg(());
                                    }
                                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                                    stream
                                });
                                capnp::serialize::write_message(stream, &msg_builder).unwrap();
                            }

                            //send write field value message
                            for field in entity.get_fields().unwrap().iter() {
                                //compute hash of field value
                                let mut hasher = SipHasher::new();
                                field.get_value().unwrap().hash(&mut hasher);
                                let field_token = hasher.finish();

                                //lookup into peer table
                                let socket_addr = rustdht::zero_hop::service::lookup(&lookup_table, field_token).unwrap();

                                //create write field message
                                let mut msg_builder = capnp::message::Builder::new_default();
                                {
                                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                    let mut write_field_msg = msg.get_msg_type().init_write_field_msg();
                                    write_field_msg.set_entity_key(entity_key);
                                    write_field_msg.set_field(field).unwrap();
                                }

                                //send write field message
                                let stream = streams.entry(socket_addr).or_insert_with(|| {
                                    let mut stream = TcpStream::connect(socket_addr).unwrap();

                                    let mut msg_builder = capnp::message::Builder::new_default();
                                    {
                                        let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                        msg.get_msg_type().set_open_write_stream_msg(());
                                    }
                                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                                    stream
                                });
                                capnp::serialize::write_message(stream, &msg_builder).unwrap();
                            }
                        }

                        //return result message
                        {
                            let mut msg_builder = capnp::message::Builder::new_default();
                            {
                                let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                msg.get_msg_type().set_result_msg(true);
                            }
                            capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                        }

                        //close all streams
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            msg.get_msg_type().set_close_write_stream_msg(());
                        }

                        for (_, stream) in streams.iter_mut() {
                            capnp::serialize::write_message(stream, &msg_builder).unwrap();
                            stream.shutdown(Shutdown::Both).unwrap();
                        }
                    },
                    Ok(OpenWriteStreamMsg(_)) => {
                        //read from stream untill close write stream message recieved
                        loop {
                            //read capnproto message
                            let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                            let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                            //check message type
                            match msg.get_msg_type().which() {
                                Ok(CloseWriteStreamMsg(_)) => break,
                                Ok(WriteEntityMsg(write_entity_msg)) => {
                                    //create entity hash map
                                    let mut entity = HashMap::new();
                                    for field in write_entity_msg.get_fields().unwrap().iter() {
                                        entity.insert(field.get_name().unwrap().to_string(), field.get_value().unwrap().to_string());
                                    }

                                    //insert entity into entities
                                    let mut entities = entities.write().unwrap();
                                    entities.insert(write_entity_msg.get_entity_key(), entity);

                                    //send debug information
                                    let debug_tx = arc_debug_tx.lock().unwrap();
                                    debug_tx.send(format!("wrote entity with key {}", write_entity_msg.get_entity_key())).unwrap();
                                },
                                Ok(WriteFieldMsg(write_field_msg)) => {
                                    //get field_values HashMap<String,[]u64>
                                    let mut fields = fields.write().unwrap();
                                    let field = write_field_msg.get_field().unwrap();
                                    let fieldname = field.get_name().unwrap();
                                    
                                    if !fields.contains_key(&fieldname[..]) {
                                        fields.insert(fieldname.to_string(), HashMap::new());
                                    }
                                    
                                    let mut field_values = fields.get_mut(&fieldname[..]).unwrap();

                                    //add key for message
                                    let value = field.get_value().unwrap();
                                    if !field_values.contains_key(&value[..]) {
                                        field_values.insert(value.to_string(), LinkedList::new());
                                    }

                                    let mut entity_tokens = field_values.get_mut(&value[..]).unwrap();
                                    entity_tokens.push_back(write_field_msg.get_entity_key());

                                    //send debug information
                                    let debug_tx = arc_debug_tx.lock().unwrap();
                                    debug_tx.send(format!("wrote field value {} for field name {} and entity key {}", value, fieldname, write_field_msg.get_entity_key())).unwrap();
                                },
                                Ok(_) => panic!("Unknown message type on write stream"),
                                Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                            }
                        }
                    },
                    Ok(QueryMsg(query_msg)) => {
                        let filter_keyset: Arc<RwLock<HashSet<u64>>> = Arc::new(RwLock::new(HashSet::new()));
                        let mut entity_keyset = HashSet::new();
                        let mut first_iteration = true;

                        //submit filter queries
                        for filter in query_msg.unwrap().iter() {
                            //clear filter keyset
                            {
                                let mut filter_keyset = filter_keyset.write().unwrap();
                                filter_keyset.clear();
                            }

                            let filter_params = filter.get_params().unwrap();
                            let mut params = Vec::new();
                            for i in 0..filter_params.len() {
                                params.push(filter_params.get(i).unwrap().to_string());
                            }

                            //send query field messages to all peers
                            let mut thread_handles = Vec::new();
                            let lookup_table = lookup_table.read().unwrap();
                            for (_, peer_socket_addr) in lookup_table.iter() {
                                //create variables for query filter message
                                let field_name = filter.get_field_name().unwrap().to_string();
                                let filter_type = filter.get_filter_type().unwrap().to_string();
                                let value = filter.get_value().unwrap().to_string();
                                let (params, filter_keyset, peer_socket_addr) = (params.clone(), filter_keyset.clone(), peer_socket_addr.clone());

                                let handle = thread::spawn(move || {
                                    //create query filter message
                                    let mut msg_builder = capnp::message::Builder::new_default();
                                    {
                                        let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                        let mut query_filter_msg = msg.get_msg_type().init_query_filter_msg();
                                        query_filter_msg.set_field_name(&field_name[..]);
                                        query_filter_msg.set_filter_type(&filter_type[..]);
                                        query_filter_msg.set_value(&value[..]);

                                        let mut filter_params = query_filter_msg.init_params(params.len() as u32);
                                        let mut param_index = 0;
                                        for param in params {
                                            filter_params.set(param_index, &param[..]);
                                            param_index += 1;
                                        }
                                    }

                                    //send query filter message
                                    let mut stream = TcpStream::connect(peer_socket_addr).unwrap();
                                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                                    //read entity tokens message
                                    let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                                    let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                                    //parse out message
                                    match msg.get_msg_type().which() {
                                        Ok(EntityKeysMsg(entity_keys_msg)) => {
                                            //add to entity tokens list
                                            let mut filter_keyset = filter_keyset.write().unwrap();
                                            let entity_keys = entity_keys_msg.unwrap();
                                            for i in 0..entity_keys.len() {
                                                filter_keyset.insert(entity_keys.get(i));
                                            }
                                        },
                                        Ok(_) => panic!("Unknown message type"),
                                        Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                                    }

                                });

                                thread_handles.push(handle);
                            }

                            //wait for all threads to join
                            for handle in thread_handles {
                                handle.join().unwrap();
                            }

                            //update entity token set
                            if first_iteration {
                                let filter_keyset = filter_keyset.read().unwrap();
                                for entity_key in filter_keyset.iter() {
                                    entity_keyset.insert(*entity_key);
                                }
                                first_iteration = false;
                            } else {
                                let filter_keyset = filter_keyset.read().unwrap();
                                let diff_keyset: HashSet<u64> = entity_keyset.difference(&filter_keyset).cloned().collect();
                                for entity_key in diff_keyset {
                                    entity_keyset.remove(&entity_key);
                                }
                            }

                            //if no tokens then no need to loop through more filters
                            if entity_keyset.is_empty() {
                                break;
                            }
                        }

                        //create entities message
                        {
                            let mut thread_handles = vec!();
                            let entities = Arc::new(RwLock::new(vec!()));
                            for entity_key in entity_keyset {
                                let (entities, lookup_table) = (entities.clone(), lookup_table.clone());

                                let handle = thread::spawn(move || {
                                    let lookup_table = lookup_table.read().unwrap();
                                    let socket_addr = rustdht::zero_hop::service::lookup(&lookup_table, entity_key).unwrap();
                                    
                                    //create query entity message
                                    let mut msg_builder = capnp::message::Builder::new_default();
                                    {
                                        let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                        msg.get_msg_type().set_query_entity_msg(entity_key);
                                    }
                                
                                    //send query entity message
                                    let mut stream = TcpStream::connect(socket_addr).unwrap();
                                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                                    //read entity message
                                    let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                                    let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                                    //create map and add all fields to it
                                    let mut entity = HashMap::new();

                                    //parse out message
                                    match msg.get_msg_type().which() {
                                        Ok(EntityMsg(entity_msg)) => {
                                            let fields = entity_msg.unwrap();
                                            for field in fields.iter() {
                                                entity.insert(field.get_name().unwrap().to_string(), field.get_value().unwrap().to_string());
                                            }
                                        },
                                        Ok(_) => panic!("Unknown message type"),
                                        Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                                    }

                                    let mut entities = entities.write().unwrap();
                                    entities.push(entity);
                                });

                                thread_handles.push(handle);
                            }
                            
                            //wait for all threads to join
                            for handle in thread_handles {
                                handle.join().unwrap();
                            }

                            //create entities message
                            let mut msg_builder = capnp::message::Builder::new_default();
                            {
                                let entities = entities.read().unwrap();

                                let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                let mut entities_msg = msg.get_msg_type().init_entities_msg(entities.len() as u32);

                                let mut index = 0;
                                for entity in entities.iter() {
                                    let entity_msg = entities_msg.borrow().get(index);
                                    let mut fields = entity_msg.init_fields(entity.len() as u32);
                                    let mut field_index = 0;
                                    for (name, value) in entity {
                                        let mut field = fields.borrow().get(field_index);
                                        field.set_name(name);
                                        field.set_value(value);
                                        field_index += 1;
                                    }

                                    index += 1;
                                }
                            }

                            //send entities message
                            capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                        }
                    },
                    Ok(QueryEntityMsg(query_entity_msg)) => {
                        //search for entity
                        let entities = entities.read().unwrap();
                        let entity_fields = entities.get(&query_entity_msg).unwrap();

                        //create entity message
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            let mut entity_msg = msg.get_msg_type().init_entity_msg(entity_fields.len() as u32);
                            let mut index = 0;
                            for (name, value) in entity_fields {
                                let mut field = entity_msg.borrow().get(index);
                                field.set_name(name);
                                field.set_value(value);
                                index += 1;
                            }
                        }

                        //send entity message
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                        //send debug information
                        let debug_tx = arc_debug_tx.lock().unwrap();
                        debug_tx.send(format!("query entity for key '{}'", query_entity_msg)).unwrap();
                    },
                    Ok(QueryFilterMsg(query_filter_msg)) => {
                        let filter = query_filter_msg.unwrap();

                        //create values for query
                        let fields = fields.read().unwrap();
                        let mut params = Vec::new();
                        let filter_params = filter.get_params().unwrap();
                        for i in 0..filter_params.len() {
                            params.push(filter_params.get(i).unwrap());
                        }

                        //query
                        let entity_keys = fuzzydb::query::query_field(filter.get_field_name().unwrap(), filter.get_filter_type().unwrap(), params, filter.get_value().unwrap(), &fields);
                        let keys = entity_keys.iter().map(|x| { format!("{}", *x) } ).collect::<Vec<String>>().join(",");

                        //create entity keys message
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                            let mut entity_keys_msg = msg.get_msg_type().init_entity_keys_msg(entity_keys.len() as u32);

                            let mut count = 0;
                            for entity_key in entity_keys {
                                entity_keys_msg.set(count, entity_key);
                                count+=1;
                            }
                        }

                        //send entity keys message
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                        //send debug information
                        let debug_tx = arc_debug_tx.lock().unwrap();
                        debug_tx.send(
                            format!(
                                "filter type '{}' on field '{}' for value '{}' results in entity keys '{}'",
                                filter.get_filter_type().unwrap(),
                                filter.get_field_name().unwrap(),
                                filter.get_value().unwrap(),
                                keys
                            )
                        ).unwrap();
                    },
                    Ok(_) => panic!("Unknown message type"),
                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                };

                stream.shutdown(Shutdown::Both).unwrap();
            });
        }
    });

    //listen for events from the p2p service
    while let Ok(event) = dht_rx.recv() {
        match event {
            Event::LookupTableMsgEvent(lookup_table) => {
                let debug_tx = arc_debug_tx.lock().unwrap();
                debug_tx.send(format!("recv PeerTableMsgEvent with {} entries", lookup_table.len())).unwrap();
            },
            Event::RegisterTokenMsgEvent(token, socket_addr) => {
                let debug_tx = arc_debug_tx.lock().unwrap();
                debug_tx.send(format!("recv RegisterTokenMsgEvent({}, {})", token, socket_addr)).unwrap();
            },
            _ => {
                let debug_tx = arc_debug_tx.lock().unwrap();
                debug_tx.send("recv event from dht - not processing this type of event".to_string()).unwrap();
            },
        }
    }
}
