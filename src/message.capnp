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
		entityKeysMsg :group {
			entityKeys @2 :List(UInt64);
		}
		insertEntitiesMsg :group {
			entities @3 :List(Entity);
		}
		queryMsg :group {
			filters @4 :List(Filter);
		}
		queryEntityMsg :group {
			entityKey @5 :UInt64;
		}
		queryFilterMsg :group {
			filter @6 :Filter;
		}
		writeEntityMsg :group {
			entityKey @7 :UInt64;
			fields @8 :List(Field);
		}
		writeFieldMsg: group {
			entityKey @9 :UInt64;
			field @10 :Field;
		}
	}
}
