@0xf25f36c02ae1cd9d;

struct Field {
	key @0 :Text;
	value @1 :Text;
}

struct Filter {
	fieldKey @0 :Text;
	comparator @1 :Text;
	value @2 :Text;
}

struct Message {
	msgType :union {
		addrMsg :group {
			socketAddr @0 :SocketAddr;
		}
		entityTokensMsg :group {
			entityTokens @16 :List(UInt64);
		}
		insertEntityMsg :group {
			fields @1 :List(Field);
		}
		lookupMsg :group {
			token @2 :UInt64;
		}
		peerTableMsg :group {
			peers @3 :List(PeerAddr);
		}
		queryMsg :group {
			filters @13 :List(Filter);
		}
		queryEntityMsg :group {
			entityToken @14 :UInt64;
		}
		queryFieldMsg :group {
			filter @15 :Filter;
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
		writeEntityMsg :group {
			entityToken @9 :UInt64;
			fields @10 :List(Field);
		}
		writeFieldMsg: group {
			entityToken @11 :UInt64;
			field @12 :Field;
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
