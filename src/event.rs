use std::collections::BTreeMap;
use std::net::SocketAddrV4;

pub enum Event {
    InsertEntityMsgEvent(BTreeMap<String,String>),
    LookupMsgEvent(u64),
    PeerTableMsgEvent(BTreeMap<u64, SocketAddrV4>),
    RegisterTokenMsgEvent(u64, SocketAddrV4),
}
