extern crate argparse;
use argparse::{ArgumentParser,Store};

extern crate csv;

extern crate capnp;

extern crate cqdb;
use cqdb::message_capnp;
use cqdb::message_capnp::message::msg_type::{EntitiesMsg};
use cqdb::parser::Command::{Exit,Help,Load,Query};

extern crate nom;

use std::collections::BTreeMap;
use std::io;
use std::io::prelude::*; //needed for flushing stdout
use std::net::{Ipv4Addr,SocketAddrV4,TcpStream};
use std::str::FromStr;

fn main() {
    let mut host_ip: String = "127.0.0.1".to_string();
    let mut host_port: u16 = 0 as u16;
    {    //solely to limit scope of parser variable
        let mut parser = ArgumentParser::new();
        parser.set_description("Start up a cqdb client session");
        parser.refer(&mut host_ip).add_option(&["-i", "--host-ip"], Store, "Ip address of the host to connect to").required();
        parser.refer(&mut host_port).add_option(&["-p", "--host-port"], Store, "Port of the host to connect to").required();
        parser.parse_args_or_exit();
    }

    //parse the host address
    let host_ip = Ipv4Addr::from_str(&host_ip[..]).unwrap();
    let host_addr = SocketAddrV4::new(host_ip, host_port);

    //loop read user input
    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("Enter input: ");
        std::io::stdout().flush().ok(); //future versions of rust will fix this need

        line.clear();
        stdin.read_line(&mut line).ok();
        line = line.trim().to_string();

        //parse command
        let cmd = match cqdb::parser::cmd(&line.clone().into_bytes()[..]) {
            nom::IResult::Done(bytes, cmd) => {
                if bytes.len() != 0 {
                    Help
                } else {
                    cmd
                }
            },
            _ => {
                println!("Invalid input command");
                Help
            },
        };

        //execute command
        match cmd {
            Exit => {
                break;
            },
            Help => {
                println!("\tEXIT => exit the session");
                println!("\tHELP => print this menu");
                println!("\tLOAD <filename> => load csv file into cluster");
                println!("\tSELECT [ * | <field> ( , <field> )* ] WHERE <field> ~<type> <value> (AND <field> ~<type> <value>)* => perfrom query on cluster");
            },
            Load(filename) => {
                //open csv file reader and read header
                let reader = csv::Reader::from_file(filename.clone());
                if !reader.is_ok() {
                    println!("file '{}' does not exist or cannot be opened", filename);
                    continue;
                }
                let mut reader = reader.unwrap();
                let header = reader.headers().unwrap();

                //loop through all records in the reader
                let mut record_count = 0;
                let mut record_buffer = Vec::new();
                for record in reader.records() {
                    let record = record.unwrap();
                    record_buffer.push(record.clone());

                    if record_buffer.len() == 50 {
                        let rb_clone = record_buffer.clone();
                        let mut msg_builder = capnp::message::Builder::new_default();
                        {
                            let msg = msg_builder.init_root::<message_capnp::message::Builder>(); 
                            let insert_entities_msg = msg.get_msg_type().init_insert_entities_msg();
                            let mut entities = insert_entities_msg.init_entities(rb_clone.len() as u32);
                        
                            let mut index = 0;
                            for record in rb_clone {
                                let entity = entities.borrow().get(index);
                                let mut fields = entity.init_fields(header.len() as u32);

                                for i in 0..header.len() {
                                    let mut field = fields.borrow().get(i as u32);
                                    field.set_name(&header[i][..]);
                                    field.set_value(&record[i].to_lowercase()[..]);
                                }

                                index += 1;
                            }
                        }

                        //send insert entity message
                        let mut stream = TcpStream::connect(host_addr).unwrap();
                        capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                        record_buffer.clear();
                    }

                    record_count += 1;
                }

                //send remaining records in the buffer
                if record_buffer.len() != 0 {
                    let mut msg_builder = capnp::message::Builder::new_default();
                    {
                        let msg = msg_builder.init_root::<message_capnp::message::Builder>(); 
                        let insert_entities_msg = msg.get_msg_type().init_insert_entities_msg();
                        let mut entities = insert_entities_msg.init_entities(record_buffer.len() as u32);
                    
                        let mut index = 0;
                        for record in record_buffer {
                            let entity = entities.borrow().get(index);
                            let mut fields = entity.init_fields(header.len() as u32);

                            for i in 0..header.len() {
                                let mut field = fields.borrow().get(i as u32);
                                field.set_name(&header[i][..]);
                                field.set_value(&record[i].to_lowercase()[..]);
                            }

                            index += 1;
                        }
                    }

                    //send insert entity message
                    let mut stream = TcpStream::connect(host_addr).unwrap();
                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();
                }

                println!("\t{}: {} records", filename, record_count);
            },
            Query(field_names, filters) => {
                //create query message
                let mut msg_builder = capnp::message::Builder::new_default();
                {
                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                    let query_msg = msg.get_msg_type().init_query_msg();
                    let mut query_filters = query_msg.init_filters(filters.len() as u32);
                    let mut idx = 0;
                    for filter in filters {
                        let mut query_filter = query_filters.borrow().get(idx);
                        query_filter.set_field_name(&filter.field_name[..]);
                        query_filter.set_filter_type(&filter.filter_type[..]);
                        query_filter.set_value(&filter.value[..]);
                        idx += 1;
                    }
                }

                //send query message
                let mut stream = TcpStream::connect(host_addr).unwrap();
                capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                //read entities tokens message
                let msg_reader = capnp::serialize::read_message(&mut stream, ::capnp::message::ReaderOptions::new()).unwrap();
                let msg = msg_reader.get_root::<message_capnp::message::Reader>().unwrap();

                //parse out message
                match msg.get_msg_type().which() {
                    Ok(EntitiesMsg(entities_msg)) => {
                        let entities = entities_msg.get_entities().unwrap();
                        let mut field_lengths = BTreeMap::new();

                        //find lengths of fields
                        for entity in entities.iter() {
                            let fields = entity.get_fields().unwrap();
                            for field in fields.iter() {
                                let field_name = field.get_name().unwrap();
                                let value = field.get_value().unwrap();

                                //if the field name is not required in output continue
                                if !field_names.contains(&field_name.to_string()) && field_names.len() != 0 {
                                    continue;
                                }

                                //if field name hasn't been inserted in field lengths yet insert
                                if !field_lengths.contains_key(field_name) {
                                    field_lengths.insert(field_name, field_name.len() as u32);
                                }

                                //if length is greater than previous insert
                                if value.len() as u32 > *field_lengths.get(field_name).unwrap() {
                                    field_lengths.insert(field_name, value.len() as u32);
                                }
                            }
                        }
                        
                        //print out fields
                        let mut total_length = 1;
                        print!("|");
                        for (field_name, length) in field_lengths.iter() {
                            print!(" ");
                            for _ in 0..(length - field_name.len() as u32) {
                                print!(" ");
                            }
                            print!("{} |", field_name);

                            total_length += 3 + length;
                        }
                        println!("");

                        //print separating line
                        for _ in 0..total_length {
                            print!("-");
                        }
                        println!("");

                        //print out entities
                        for entity in entities.iter() {
                            print!("|");
                            for (field_name, length) in field_lengths.iter() {
                                let fields = entity.get_fields().unwrap();
                                
                                for field in fields.iter() {
                                    let name = field.get_name().unwrap();
                                    if &name != field_name {
                                        continue;
                                    }

                                    let value = field.get_value().unwrap();
                                    print!(" ");
                                    for _ in 0..(length - value.len() as u32) {
                                        print!(" ");
                                    }
                                    print!("{} |", value);
                                }
                            }
                            println!("");
                        }
                    },
                    Ok(_) => panic!("Unknown message type"),
                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                }
            },
        }
    }
}
