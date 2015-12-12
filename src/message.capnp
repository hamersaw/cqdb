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
		resultMsg :group {
			success @7 :Bool;
		}
		writeEntityMsg :group {
			entityKey @8 :UInt64;
			fields @9 :List(Field);
		}
		writeFieldMsg: group {
			entityKey @10 :UInt64;
			field @11 :Field;
		}
	}
}
