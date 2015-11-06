extern crate argparse;
use argparse::{ArgumentParser,Store};

fn main() {
    let mut size: u16 = 0u16;
    {    //solely to limit scope of parser variable
        let mut parser = ArgumentParser::new();
        parser.set_description("Compute tokens for cluster of size n");
        parser.refer(&mut size).add_option(&["-s", "--cluster_size"], Store, "Size of the cluster").required();
        parser.parse_args_or_exit();
    }

    println!("Tokens for cluster of size {}", size);
    let delta = u64::max_value() / (size as u64);
    println!("delta: {}", delta);

    for i in (0..size) {
        println!("{}: {}", i, delta * (i as u64));
    }
}
