extern crate argparse;
use argparse::{ArgumentParser,Store,StoreTrue};

extern crate capnp;

extern crate fuzzydb;
use fuzzydb::message_capnp;
use fuzzydb::message_capnp::message::msg_type::{CloseWriteStreamMsg,InsertEntitiesMsg,EntityMsg,EntityKeysMsg,OpenWriteStreamMsg,QueryMsg,QueryEntityMsg,QueryFilterMsg,WriteEntityMsg,WriteFieldMsg};

extern crate rustdht;
use rustdht::event::Event;

use std::collections::{BTreeMap,HashMap};
use std::hash::{Hash,Hasher,SipHasher};
use std::io::{Read,Write};
use std::net::{Ipv4Addr,SocketAddrV4,Shutdown,TcpListener,TcpStream};
use std::str::FromStr;
use std::sync::{Arc,Mutex,RwLock};
use std::sync::mpsc::{channel,Sender};
use std::thread;

pub fn main() {
    let mut token: u64 = 0;
    let mut app_ip: String = "127.0.0.1".to_string();
    let mut app_port: u16 = 0;
    let mut service_port: u16 = 0;
    let mut seed_ip: String = "127.0.0.1".to_string();
    let mut seed_port: u16 = 0;
    let mut debug = false;
    {    //solely to limit scope of parser variable
        let mut parser = ArgumentParser::new();
        parser.set_description("start an instance of fuzzydb server");
        parser.refer(&mut token).add_option(&["-t", "--token"], Store, "token of node").required();
        parser.refer(&mut app_ip).add_option(&["-i", "--listen-ip"], Store, "ip address for application and service to listen on").required();
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
    let fields: Arc<RwLock<HashMap<String,HashMap<String,Vec<u64>>>>> = Arc::new(RwLock::new(HashMap::new()));
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
    let dht_rx = rustdht::service::start(token, app_addr, service_addr, seed_addr, lookup_table.clone());

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
                            let socket_addr = rustdht::service::lookup(&lookup_table, entity_key).unwrap();

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
                                let stream = streams.entry(socket_addr).or_insert_with(|| { open_write_stream(socket_addr) });
                                capnp::serialize::write_message(stream, &msg_builder).unwrap();
                            }

                            //send write field value message
                            for field in entity.get_fields().unwrap().iter() {
                                //compute hash of field value
                                let mut hasher = SipHasher::new();
                                field.get_value().unwrap().hash(&mut hasher);
                                let field_hash = hasher.finish();

                                //create write field message
                                let mut msg_builder = capnp::message::Builder::new_default();
                                {
                                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                    let mut write_field_msg = msg.get_msg_type().init_write_field_msg();
                                    write_field_msg.set_entity_key(entity_key);
                                    write_field_msg.set_field(field).unwrap();
                                }

                                //send write field message
                                let socket_addr = rustdht::service::lookup(&lookup_table, field_hash).unwrap();
                                let stream = streams.entry(socket_addr).or_insert_with(|| { open_write_stream(socket_addr) });
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
                                    let (fieldname, field_value) = (field.get_name().unwrap(), field.get_value().unwrap());
                                    
                                    //search for and create entry in fields if necessary
                                    let mut field_values = fields.entry(fieldname.to_string()).or_insert(HashMap::new());
                                    let mut entity_keys = field_values.entry(field_value.to_string()).or_insert(vec!());
                                    entity_keys.push(write_field_msg.get_entity_key());

                                    //send debug information
                                    let debug_tx = arc_debug_tx.lock().unwrap();
                                    debug_tx.send(format!("wrote field value {} for field name {} and entity key {}", field_value, fieldname, write_field_msg.get_entity_key())).unwrap();
                                },
                                Ok(_) => panic!("Unknown message type on write stream"),
                                Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                            }
                        }
                    },
                    Ok(QueryMsg(query_msg)) => {
                        //get entity keys
                        let entity_keys = get_entity_keys(query_msg.unwrap(), &lookup_table);
                        
                        //create entities message
                        {
                            //poll for entities
                            let (entities_tx, entities_rx) = channel::<HashMap<String,String>>();
                            let entity_keys_len = entity_keys.len();
                            get_entities(entity_keys, lookup_table, entities_tx);

                            let mut entity_vec = vec!();
                            for _ in 0..entity_keys_len {
                                entity_vec.push(entities_rx.recv().unwrap());
                            }

                            //create entities message
                            let mut msg_builder = capnp::message::Builder::new_default();
                            {
                                let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                let mut entities_msg = msg.get_msg_type().init_entities_msg(entity_vec.len() as u32);

                                for (i, entity) in entity_vec.iter().enumerate() {
                                    let entity_msg = entities_msg.borrow().get(i as u32);
                                    let mut fields = entity_msg.init_fields(entity.len() as u32);

                                    for (j, (name, value)) in entity.iter().enumerate() {
                                        let mut field = fields.borrow().get(j as u32);
                                        field.set_name(name);
                                        field.set_value(value);
                                    }
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

                            for (i, (name, value)) in entity_fields.iter().enumerate() {
                                let mut field = entity_msg.borrow().get(i as u32);
                                field.set_name(name);
                                field.set_value(value);
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

                        //perform actual query
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
            Event::RemoveNodeEvent(token, socket_addr) => {
                let debug_tx = arc_debug_tx.lock().unwrap();
                debug_tx.send(format!("recv RemoveNodeEvent({}, {})", token, socket_addr)).unwrap();
            },
            Event::RegisterNodeEvent(token, socket_addr) => {
                let debug_tx = arc_debug_tx.lock().unwrap();
                debug_tx.send(format!("recv RegisterNodeEvent({}, {})", token, socket_addr)).unwrap();
            },
            /*_ => {
                let debug_tx = arc_debug_tx.lock().unwrap();
                debug_tx.send("recv event from dht - not processing this type of event".to_string()).unwrap();
            },*/
        }
    }
}

fn open_write_stream(socket_addr: SocketAddrV4) -> TcpStream {
    let mut stream = TcpStream::connect(socket_addr).unwrap();

    let mut msg_builder = capnp::message::Builder::new_default();
    {
        let msg = msg_builder.init_root::<message_capnp::message::Builder>();
        msg.get_msg_type().set_open_write_stream_msg(());
    }
    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

    stream
}

fn get_entity_keys(filters: capnp::struct_list::Reader<fuzzydb::message_capnp::filter::Owned>, lookup_table: &Arc<RwLock<BTreeMap<u64,SocketAddrV4>>>) -> Vec<u64> {
    let mut entity_keys = vec!();

    //submit filter queries
    for (i, filter) in filters.iter().enumerate() {
        let filter_params = filter.get_params().unwrap();
        let mut params = Vec::new();
        for j in 0..filter_params.len() {
            params.push(filter_params.get(j).unwrap().to_string());
        }

        let (keys_tx, keys_rx) = channel::<Vec<u64>>();
        let keys_tx = Arc::new(Mutex::new(keys_tx));

        //send query field messages to all peers
        let lookup_table = lookup_table.read().unwrap();
        for (_, peer_socket_addr) in lookup_table.iter() {
            //create variables for query filter message
            let field_name = filter.get_field_name().unwrap().to_string();
            let filter_type = filter.get_filter_type().unwrap().to_string();
            let value = filter.get_value().unwrap().to_string();
            let (params, peer_socket_addr, keys_tx) = (params.clone(), peer_socket_addr.clone(), keys_tx.clone());

            thread::spawn(move || {
                //create query filter message
                let mut msg_builder = capnp::message::Builder::new_default();
                {
                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                    let mut query_filter_msg = msg.get_msg_type().init_query_filter_msg();
                    query_filter_msg.set_field_name(&field_name[..]);
                    query_filter_msg.set_filter_type(&filter_type[..]);
                    query_filter_msg.set_value(&value[..]);

                    let mut filter_params = query_filter_msg.init_params(params.len() as u32);
                    for (i, param) in params.iter().enumerate() {
                        filter_params.set(i as u32, &param[..]);
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
                        let mut keys = vec!();
                        let entity_keys = entity_keys_msg.unwrap();
                        for i in 0..entity_keys.len() {
                            keys.push(entity_keys.get(i));
                        }

                        let keys_tx = keys_tx.lock().unwrap();
                        keys_tx.send(keys).unwrap();
                    },
                    Ok(_) => panic!("Unknown message type"),
                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                }

            });
        }

        //compile set of keys for filter
        let mut filter_keys = vec!();
        for _ in 0..lookup_table.len() {
            let keys = keys_rx.recv().unwrap();

            for key in keys {
                filter_keys.push(key);
            }
        }

        if i == 0 {
            //first filter
            for key in filter_keys.iter() {
                entity_keys.push(*key);
            }
        } else {
            //compute intersection with our running entity keys
            entity_keys = entity_keys.iter().filter(|x| filter_keys.contains(x)).map(|x| *x).collect();
        }

        //if no tokens then no need to loop through more filters
        if entity_keys.is_empty() {
            break;
        }
    }

    entity_keys
}

fn get_entities(entity_keyset: Vec<u64>, lookup_table: Arc<RwLock<BTreeMap<u64,SocketAddrV4>>>, entity_tx: Sender<HashMap<String,String>>) {
    let entity_tx = Arc::new(Mutex::new(entity_tx));
    for entity_key in entity_keyset {
        let (lookup_table, entity_tx) = (lookup_table.clone(), entity_tx.clone());

        thread::spawn(move || {
            let lookup_table = lookup_table.read().unwrap();
            let socket_addr = rustdht::service::lookup(&lookup_table, entity_key).unwrap();
            
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

            let entity_tx = entity_tx.lock().unwrap();
            entity_tx.send(entity).unwrap();
        });
    }
}
