extern crate argparse;
use argparse::{ArgumentParser,Store};

extern crate csv;

extern crate capnp;

extern crate cqdb;
use cqdb::message_capnp;
use cqdb::message_capnp::message::msg_type::{EntitiesMsg};
use cqdb::parser::Command::{Help,Load,Query};

extern crate nom;

use std::io;
use std::io::prelude::*; //needed for flushing stdout
use std::net::{Ipv4Addr,SocketAddrV4,TcpStream};
use std::path::Path;
use std::str::FromStr;

fn main() {
    let mut host_ip: String = "127.0.0.1".to_string();
    let mut host_port: u16 = 0 as u16;
    {    //solely to limit scope of parser variable
        let mut parser = ArgumentParser::new();
        parser.set_description("Start up a cqdb node");
        parser.refer(&mut host_ip).add_option(&["-i", "--host-ip"], Store, "Ip address of the host to connect to").required();
        parser.refer(&mut host_port).add_option(&["-p", "--host-port"], Store, "Port of the host to connect to").required();
        parser.parse_args_or_exit();
    }
   
    let host_ip = match Ipv4Addr::from_str(&host_ip[..]) {
        Ok(host_ip) => host_ip,
        Err(_) => panic!("Unable to parse ip '{}'", host_ip),
    };

    let host_addr = SocketAddrV4::new(host_ip, host_port);

    //read user input
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
            nom::IResult::Done(bytes, query) => {
                if bytes.len() != 0 {
                    Help
                } else {
                    query
                }
            },
            _ => {
                println!("Invalid input command");
                Help
            },
        };

        //execute command
        match cmd {
            Help => {
                println!("\tLOAD <filename> => load csv file into cluster");
                println!("\tSELECT [ * | <field> ( , <field> )* ] WHERE <field> ~<type> <value> (AND <field> ~<type> <value>)* => perfrom query on cluster");
                println!("\tHELP => print this menu");
            },
            Load(file_name) => {
                //parse out filename
                /*let path = Path::new(&file_name[..]);
                let filename = match path.file_name().unwrap().to_str() {
                    Some(filename) => filename,
                    None => panic!("invalid filename"),
                };*/

                //open csv file reader and read header
                let reader = csv::Reader::from_file(file_name.clone());
                if !reader.is_ok() {
                    println!("file '{}' does not exist or cannot be opened", file_name);
                    continue;
                }
                let mut reader = reader.unwrap();
                let header = reader.headers().unwrap();

                //read records
                let mut record_count = 0;
                for record in reader.records() {
                    let record = record.unwrap();

                    //create insert entity message
                    let mut msg_builder = capnp::message::Builder::new_default();
                    {
                        let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                        let insert_entity_msg = msg.get_msg_type().init_insert_entity_msg();
                        let mut fields = insert_entity_msg.init_fields(header.len() as u32);
                        for i in 0..header.len() {
                            let mut field = fields.borrow().get(i as u32);                            
                            field.set_name(&header[i][..]);
                            field.set_value(&record[i].to_lowercase()[..]);
                        }
                    }

                    //send insert entity message
                    let mut stream = TcpStream::connect(host_addr).unwrap();
                    capnp::serialize::write_message(&mut stream, &msg_builder).unwrap();

                    record_count += 1;
                }

                println!("\t{}: {} records", file_name, record_count);

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
                        query_filter.set_type(&filter.filter_type[..]);
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
                        //loop through result entities
                        let entities = entities_msg.get_entities().unwrap();
                        for entity in entities.iter() {
                            println!("Entity");

                            let fields = entity.get_fields().unwrap();
                            for field in fields.iter() {
                                //TODO only print off it it's in field_names
                                println!("{}: {}", field.get_name().unwrap(), field.get_value().unwrap());
                            }
                        }
                    },
                    Ok(_) => panic!("Unknown message type"),
                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                }
            },
        }
    }
}
