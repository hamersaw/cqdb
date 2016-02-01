extern crate capnpc;

fn main() {
    ::capnpc::compile(".", &["capnproto/message.capnp"]).unwrap();
    println!("Succesfully compiled capnproto files");
}
