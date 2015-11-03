@0xf25f36c02ae1cd9d;

struct Message {
	msgType :union {
		addrMsg :group {
			socketAddr @0 :SocketAddr;
		}
		genericMsg :group {
			data @1 :Data;
		}
		lookupMsg :group {
			token @2 :UInt64;
		}
		peerTableMsg :group {
			peers @3 :List(PeerAddr);
		}
		registerTokenMsg :group {
			token @4 :UInt64;
			socketAddr @5 :SocketAddr;
			joinInd @6 :Bool;
		}
		resultMsg :group {
			success @7 :Bool;
			errMsg @8 :Text;
		}
	}
}

struct PeerAddr {
	token @0 :UInt64;
	ip @1 :Text;
	port @2 :UInt16;
}

struct SocketAddr {
	ip @0 :Text;
	port @1 :UInt16;
}
