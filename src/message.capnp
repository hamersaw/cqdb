@0xf25f36c02ae1cd9d;

struct Entity {
	fields @0 :List(Field);
}

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
		entitiesMsg :group {
			entities @0 :List(Entity);
		}
		entityMsg :group {
			fields @1 :List(Field);
		}
		entityTokensMsg :group {
			entityTokens @2 :List(UInt64);
		}
		insertEntityMsg :group {
			fields @3 :List(Field);
		}
		queryMsg :group {
			filters @4 :List(Filter);
		}
		queryEntityMsg :group {
			entityToken @5 :UInt64;
		}
		queryFieldMsg :group {
			filter @6 :Filter;
		}
		writeEntityMsg :group {
			entityToken @7 :UInt64;
			fields @8 :List(Field);
		}
		writeFieldMsg: group {
			entityToken @9 :UInt64;
			field @10 :Field;
		}
	}
}
