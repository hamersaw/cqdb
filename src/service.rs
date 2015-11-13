extern crate capnp;

use message_capnp;
use message_capnp::message::msg_type::{InsertEntityMsg,EntityMsg,EntityTokensMsg,LookupMsg,PeerTableMsg,QueryMsg,QueryEntityMsg,QueryFieldMsg,RegisterTokenMsg,WriteEntityMsg,WriteFieldMsg};

use event::Event;

use std::collections::{BTreeMap,HashMap,HashSet,LinkedList};
use std::hash::{Hash,Hasher,SipHasher};
use std::io::{Read,Write};
use std::net::{Ipv4Addr,SocketAddrV4,TcpListener,TcpStream};
use std::str::FromStr;
use std::sync::{Arc,RwLock};
use std::sync::mpsc::{channel,Receiver};
use std::thread;

pub struct OmniscientService {
    id: String,
    token: u64,
    listen_addr: SocketAddrV4,
    seed_addr: Option<SocketAddrV4>,
    peer_table: Arc<RwLock<BTreeMap<u64,SocketAddrV4>>>,
    entities: Arc<RwLock<HashMap<u64,HashMap<String,String>>>>,
    fields: Arc<RwLock<HashMap<String,HashMap<String,LinkedList<u64>>>>>,
}

impl OmniscientService {
    pub fn new(id: String, token: u64, listen_addr: SocketAddrV4, seed_addr: Option<SocketAddrV4>) -> Self {
        OmniscientService {
            id: id,
            token: token,
            listen_addr: listen_addr,
            seed_addr: seed_addr,
            peer_table: Arc::new(RwLock::new(BTreeMap::new())),
            entities: Arc::new(RwLock::new(HashMap::new())),
            fields: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn start(&self) -> Receiver<Event> {
        //add your token to peer_table
        {
            let mut peer_table = self.peer_table.write().unwrap();
            peer_table.insert(self.token, self.listen_addr);
        }

        //create listener
        let listener = match TcpListener::bind(self.listen_addr) {
            Ok(listener) => listener,
            Err(e) => panic!("{}", e),
        };

        //print socket addr
        match listener.local_addr() {
            Ok(local_addr) => println!("Server {} listening at {}", self.id, local_addr),
            Err(e) => panic!("{}", e),
        };

        //create event channels
        let (tx, rx) = channel::<Event>();

        //start listening
        let peer_table = self.peer_table.clone();
        let entities = self.entities.clone();
        let fields = self.fields.clone();
        let token = self.token.clone();
        let listen_addr = self.listen_addr.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let peer_table = peer_table.clone();
                let entities = entities.clone();
                let fields = fields.clone();
                let tx = tx.clone();

                thread::spawn(move || {
                    let mut stream = stream.unwrap();

                    //read capnproto message
                    let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                    let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                    //parse out message
                    match msg.get_msg_type().which() {
                        Ok(InsertEntityMsg(insert_entity_msg)) => {
                            //TODO send event to channel

                            //compute hash over all fields
                            let mut concat_string = String::new();
                            for field in insert_entity_msg.get_fields().unwrap().iter() {
                                concat_string = concat_string + field.get_value().unwrap();
                            }
                            let mut hasher = SipHasher::new();
                            concat_string.hash(&mut hasher);
                            let entity_token = hasher.finish();

                            //lookup into peer table
                            let peer_table = peer_table.read().unwrap();
                            let socket_addr = match lookup(&peer_table, entity_token) {
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
                                let socket_addr = match lookup(&peer_table, field_token) {
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
                        Ok(LookupMsg(lookup_msg)) => {
                            tx.send(Event::LookupMsgEvent(lookup_msg.get_token())).unwrap();

                            //create result message
                            let mut msg_builder = capnp::message::Builder::new_default();
                            {
                                let msg = msg_builder.init_root::<message_capnp::message::Builder>();

                                //lookup token in peer table and create return message
                                let peer_table = peer_table.read().unwrap();
                                match lookup(&peer_table, lookup_msg.get_token()) {
                                    Some(socket_addr) => {
                                        let addr_msg = msg.get_msg_type().init_addr_msg();
                                        let mut msg_socket_addr = addr_msg.get_socket_addr().unwrap();
                                        msg_socket_addr.set_ip(&socket_addr.ip().to_string()[..]);
                                        msg_socket_addr.set_port(socket_addr.port());
                                    },
                                    None => {
                                        let mut result_msg = msg.get_msg_type().init_result_msg();
                                        result_msg.set_success(true);
                                        result_msg.set_err_msg("");
                                    },
                                };
                            }

                            //send result message
                            capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                        },
                        Ok(PeerTableMsg(peer_table_msg)) => {
                            let mut map: BTreeMap<u64,SocketAddrV4> = BTreeMap::new();
                            for peer in peer_table_msg.get_peers().unwrap().iter() {
                                let ip_addr = Ipv4Addr::from_str(&peer.get_ip().unwrap()[..]).unwrap();
                                let socket_addr = SocketAddrV4::new(ip_addr, peer.get_port());
                                
                                //add token and socket address to peer table
                                let mut peer_table = peer_table.write().unwrap();
                                let _ = add_token(&mut peer_table, peer.get_token(), socket_addr);

                                map.insert(peer.get_token(), socket_addr);
                            }

                            //send event
                            tx.send(Event::PeerTableMsgEvent(map)).unwrap();
                        },
                        Ok(QueryMsg(query_msg)) => {
                            //TODO send event
                            
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

                                    let peer_table = peer_table.read().unwrap();
                                    for (_, peer_socket_addr) in peer_table.iter() {
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
                                    let peer_table = peer_table.read().unwrap();
                                    let socket_addr = match lookup(&peer_table, token) {
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
                            //TODO send event
                            
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
                            //println!("recv query_field_msg with comparator:{} field_key:{} value:{}", filter.get_comparator().unwrap(), filter.get_field_key().unwrap(), filter.get_value().unwrap());
                            //TODO send event

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
                        Ok(RegisterTokenMsg(register_token_msg)) => {
                            let msg_socket_addr = register_token_msg.get_socket_addr().unwrap();
                            let ip_addr = Ipv4Addr::from_str(&msg_socket_addr.get_ip().unwrap()[..]).unwrap();
                            let socket_addr = SocketAddrV4::new(ip_addr, msg_socket_addr.get_port());
                            tx.send(Event::RegisterTokenMsgEvent(register_token_msg.get_token(), socket_addr.clone())).unwrap();

                            {
                                //add token and socket address to peer table
                                let mut peer_table = peer_table.write().unwrap();
                                let _ = add_token(&mut peer_table, register_token_msg.get_token(), socket_addr);
                            }

                            if register_token_msg.get_join_ind() {
                                //create peer table message
                                let mut msg_builder = capnp::message::Builder::new_default();
                                {
                                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                    let peer_table_msg = msg.get_msg_type().init_peer_table_msg();

                                    let peer_table = peer_table.read().unwrap();
                                    let mut peers = peer_table_msg.init_peers(peer_table.len() as u32);

                                    let mut index = 0;
                                    for (peer_token, peer_socket_addr) in peer_table.iter() {
                                        if register_token_msg.get_token() == *peer_token {
                                            continue;
                                        }

                                        let mut peer = peers.borrow().get(index); 
                                        peer.set_token(*peer_token);
                                        peer.set_ip(&peer_socket_addr.ip().to_string()[..]);
                                        peer.set_port(peer_socket_addr.port());

                                        index += 1;
                                    }

                                    //add yourself to the peer table
                                    let mut peer = peers.borrow().get(index);
                                    peer.set_token(token);
                                    peer.set_ip(&listen_addr.ip().to_string()[..]);
                                    peer.set_port(listen_addr.port());
                                }

                                //send peer table message to joining node
                                let mut stream = TcpStream::connect(socket_addr).unwrap();
                                capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                                //create register token message
                                let mut msg_builder = capnp::message::Builder::new_default();
                                {
                                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                                    let mut rt_msg = msg.get_msg_type().init_register_token_msg();
                                    rt_msg.set_token(register_token_msg.get_token());
                                    rt_msg.set_socket_addr(register_token_msg.get_socket_addr().unwrap()).unwrap();
                                }

                                //send register token message to all peers
                                let peer_table = peer_table.read().unwrap();
                                for (peer_token, peer_socket_addr) in peer_table.iter() {
                                    if *peer_token == register_token_msg.get_token() || *peer_token == token {
                                        continue;
                                    }

                                    let mut stream = TcpStream::connect(peer_socket_addr).unwrap();
                                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                                }
                            }
                        },
                        Ok(WriteEntityMsg(write_entity_msg)) => {
                            //TODO send write entity message event

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
                            //TODO send write field value message event

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

        //send join message to seed_addr
        match self.seed_addr {
            Some(seed_addr) => {
                //create join message
                let mut msg_builder = capnp::message::Builder::new_default();
                {
                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                    let mut register_token_msg = msg.get_msg_type().init_register_token_msg();
                    register_token_msg.set_token(self.token.clone());
                    register_token_msg.set_join_ind(true);
                    let mut msg_socket_addr = register_token_msg.get_socket_addr().unwrap();
                    msg_socket_addr.set_ip(&self.listen_addr.ip().to_string()[..]);
                    msg_socket_addr.set_port(self.listen_addr.port());
                }

                //send join message
                let mut stream = TcpStream::connect(seed_addr).unwrap();
                capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
            },
            None => {},
        }

        rx
    }

    /*pub fn lookup(&self, token: u64) -> Option<SocketAddrV4> {
        let peer_table = self.peer_table.clone();
        let peer_table = peer_table.read().unwrap();

        lookup(&peer_table, token)
    }

    pub fn add_token(&mut self, token: u64, socket_addr: SocketAddrV4) -> Result<bool,String> {
        let peer_table = self.peer_table.clone();
        let mut peer_table = peer_table.write().unwrap();

        add_token(&mut peer_table, token, socket_addr)
    }
    
    //open_stream - open_broadcast
    pub fn open_stream(&self, token: u64) -> Option<TcpStream> {
        let peer_table = self.peer_table.read().unwrap();
        let socket_addr = match lookup(&peer_table, token) {
            Some(socket_addr) => socket_addr,
            None => return None,
        };

        match TcpStream::connect(socket_addr) {
            Ok(stream) => Some(stream),
            Err(_) => None,
        }
    }

    //pub fn send_msg(&self, token: u64, msg: Vec<u8>) -> Result<(), String> {
    pub fn send_msg(&self, _: u64, _: Vec<u8>) -> Result<(), String> {
        unimplemented!();
    }

    //pub fn broadcast_msg(&self, msg: Vec<u8>) -> Result<(), String> {
    pub fn broadcast_msg(&self, _: Vec<u8>) -> Result<(), String> {
        unimplemented!();
    }*/

    pub fn print(&self) {
        println!("ID:{}\ntoken:{}", self.id, self.token);
        
        let peer_table = self.peer_table.clone();
        let peer_table = peer_table.read().unwrap();

        println!("----TOKEN TABLE----");
        for (peer_token, peer_socket_addr) in peer_table.iter() {
            println!("\t{}: {}", peer_token, peer_socket_addr);
        }
    }
}

fn lookup(peer_table: &BTreeMap<u64,SocketAddrV4>, token: u64) -> Option<SocketAddrV4> {
    //get first (smallest) token from the peer table
    let mut iter = peer_table.iter();
    let first_tuple = match iter.next() {
        Some(current_tuple) => current_tuple,
        None => return None,
    };

    //if search token is smaller than first
    if token < *first_tuple.0 {
        return Some(*first_tuple.1);
    };

    //search in between every set of concurrent tokens
    let mut last_token = *first_tuple.0;
    for (current_token, socket_addr) in iter {
        if last_token < token && *current_token >= token {
            return Some(*socket_addr);
        }

        last_token = *current_token;
    }

    Some(*first_tuple.1)
}

fn add_token(peer_table: &mut BTreeMap<u64,SocketAddrV4>, token: u64, socket_addr: SocketAddrV4) -> Result<bool,String> {
    let token_added = match peer_table.contains_key(&token) {
        false => {
            peer_table.insert(token, socket_addr);
            true
        },
        true => false,
    };

    Ok(token_added)
}
