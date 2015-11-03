use std::collections::BTreeMap;
use std::net::{SocketAddrV4,TcpStream};

pub enum Event {
    GenericMsgEvent(Vec<u8>, TcpStream),
    LookupMsgEvent(u64),
    PeerTableMsgEvent(BTreeMap<u64, SocketAddrV4>),
    RegisterTokenMsgEvent(u64, SocketAddrV4),
}
