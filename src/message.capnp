@0xf25f36c02ae1cd9d;

struct Entity {
	fields @0 :List(Field);
}

struct Field {
	name @0 :Text;
	value @1 :Text;
}

struct Filter {
	fieldName @0 :Text;
	filterType @1 :Text;
	params @2 :List(Text);
	value @3 :Text;
}

struct Message {
	msgType :union {
		closeWriteStreamMsg @0 :Void;
		entitiesMsg @1 :List(Entity);
		entityMsg @2 :List(Field);
		entityKeysMsg @3 :List(UInt64);
		insertEntitiesMsg @4 :List(Entity);
		openWriteStreamMsg @5 :Void;
		queryMsg @6 :List(Filter);
		queryEntityMsg @7 :UInt64;
		queryFilterMsg @8:Filter;
		writeEntityMsg :group {
			entityKey @9 :UInt64;
			fields @10 :List(Field);
		}
		writeFieldMsg: group {
			entityKey @11 :UInt64;
			field @12 :Field;
		}
	}
}
