#cqdb - Complex Query Database 

##Overview
A distributed database for fuzzy matching entities.

##Examples
####Server
```bash
./cqdb-server -i coeus -t 0 -l 127.0.0.1 -a 15605 -p 15705
./cqdb-server -i crius -t 1537228672809129301 -l 127.0.0.1 -a 15606 -p 15706 -s 127.0.0.1 -e 15705
./cqdb-server -i cronus -t 3074457345618258602 -l 127.0.0.1 -a 15607 -p 15707 -s 127.0.0.1 -e 15705
./cqdb-server -i hyperion -t 4611686018427387903 -l 127.0.0.1 -a 15608 -p 15708 -s 127.0.0.1 -e 15705
./cqdb-server -i lapetus -t 6148914691236517204 -l 127.0.0.1 -a 15609 -p 15709 -s 127.0.0.1 -e 15705
./cqdb-server -i mnemosyne -t 7686143364045646505 -l 127.0.0.1 -a 15610 -p 15710 -s 127.0.0.1 -e 15705
./cqdb-server -i oceanus -t 9223372036854775806 -l 127.0.0.1 -a 15611 -p 15711 -s 127.0.0.1 -e 15705
./cqdb-server -i phoebe -t 10760600709663905107 -l 127.0.0.1 -a 15612 -p 15712 -s 127.0.0.1 -e 15705
./cqdb-server -i rhea -t 12297829382473034408 -l 127.0.0.1 -a 15613 -p 15713 -s 127.0.0.1 -e 15705
./cqdb-server -i tethys -t 13835058055282163709 -l 127.0.0.1 -a 15614 -p 15714 -s 127.0.0.1 -e 15705
./cqdb-server -i theia -t 15372286728091293010 -l 127.0.0.1 -a 15615 -p 15715 -s 127.0.0.1 -e 15705
./cqdb-server -i themis -t 16909515400900422311 -l 127.0.0.1 -a 15616 -p 15716 -s 127.0.0.1 -e 15705
```

####Client
```bash
./cqdb-client -i 127.0.0.1 -p 15605
````

##Storage Architecture Concepts
- All of the fields of an entity are hashed to compute a record key
- A entities record key determines which node the entities full set of field values is stored on
- Each field value of an entity is hashed to compute a field value key
- A pointer to the record key is stored on the appropriate machine for each field value

##Storage Architecture Example
For this example the token space is 0-99. All hash values are fictional.

```
node1 - token:33
	entities:
	fields:
		first_name:
			daniel -> 43,58
		middle_name:
			peter -> 43
		last_name:
node2 - token:66
	entities:
		43 -> "first_name:daniel middle_name:peter last_name:rammer"
		58 -> "first_name:daniel middle_name:wroughton last_name:craig"
	fields:
		first_name:
		middle_name:
			wroughton -> 58
		last_name:
node3 - token:99
	entities:
	fields:
		first_name:
		middle_name:
		last_name:
			craig -> 58
			rammer -> 43
```

####Insertions
- "first_name:daniel middle_name:peter last_name:rammer" -> hash -> 43
- daniel -> hash -> 23
- peter -> hash -> 27
- rammer -> hash -> 68
- "first_name:daniel middle_name:wroughton last_name:craig" -> hash -> 58
- daniel -> hash -> 23
- wroughton -> hash -> 54
- craig -> hash -> 83

##Query Order of Events
1. Queries are parsed and each individual filter is sent to every node
2. A set of entity keys is returned from each node for each filter
3. The union of those sets is determined to be entities that match the query
4. Nodes that are responsible for those entity keys are contacted to get the full set of field values for each entity

##TODO
- 2. need quoted fields in parser
- 1. pass arguments to the filter types (edit distance count, ngram min score, etc..)
- potentially work more with channels - server query filter/entity requests
