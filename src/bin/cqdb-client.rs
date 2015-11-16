extern crate argparse;
use argparse::{ArgumentParser,Store};

extern crate csv;

extern crate capnp;

extern crate cqdb;
use cqdb::message_capnp;
use cqdb::message_capnp::message::msg_type::{EntitiesMsg};

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
        let str_vec: Vec<&str> = line.trim().split(' ').collect();

        //parse command
        match str_vec[0] {
            "load_file" => {
                if str_vec.len() != 2 {
                    println!("load_file command requires exaclty 1 argument. {} were given.", str_vec.len() - 1);
                    continue;
                }

                //parse out filename
                let path = Path::new(str_vec[1]);
                let filename = match path.file_name().unwrap().to_str() {
                    Some(filename) => filename,
                    None => panic!("invalid filename"),
                };

                //open csv file reader and read header
                let mut reader = csv::Reader::from_file(str_vec[1]).unwrap();
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

                println!("\t{}: {} records", filename, record_count);
            },
            "query" => {
                println!("TODO process query with db at {}", host_addr);
                //TODO read in user input and parse query for filters

                //create query message
                let mut msg_builder = capnp::message::Builder::new_default();
                {
                    let msg = msg_builder.init_root::<message_capnp::message::Builder>();
                    let query_msg = msg.get_msg_type().init_query_msg();
                    let mut filters = query_msg.init_filters(1);
                    {
                        let mut filter = filters.borrow().get(0);
                        filter.set_field_name("first_name");
                        filter.set_type("soundex");
                        filter.set_value("doniel");
                    }
                    /*{
                        let mut filter = filters.borrow().get(1);
                        filter.set_field_name("last_name");
                        filter.set_type("ngram");
                        filter.set_value("rammor");
                    }*/
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
                                println!("{}: {}", field.get_name().unwrap(), field.get_value().unwrap());
                            }
                        }
                    },
                    Ok(_) => panic!("Unknown message type"),
                    Err(capnp::NotInSchema(e)) => panic!("Error capnp::NotInSchema: {}", e),
                }
            },
            "help" => {
                println!("\tload_file <filename> => load csv file into cluster");
                println!("\tquery <query>        => execute query on cluster");
                println!("\thelp                 => print this menu");
                println!("\tquit                 => exit the program");
            },
            "quit" => break,
            _ => println!("Unknown command '{}' issue command 'help' for command usage", str_vec[0]),
        }
    }
}
