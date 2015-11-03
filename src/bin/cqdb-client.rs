extern crate argparse;
use argparse::{ArgumentParser,Store};

extern crate cqdb;
//use cqdb::message_capnp;

use std::io;
use std::io::prelude::*;
use std::net::{Ipv4Addr,SocketAddrV4};
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
                println!("TODO load_file into db at {}", host_addr);
            },
            "query" => {
                println!("TODO process query with db at {}", host_addr);
            },
            "quit" => break,
            _ => println!("Unknown command '{}'", str_vec[0]),
        }
    }
}
