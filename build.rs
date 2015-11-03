extern crate capnpc;

fn main() {
    ::capnpc::compile(".", &["src/message.capnp"]).unwrap();
    println!("Succesfully compiled capnproto files");
}
