#cqdb - Complex Query Database 

##Overview
A distributed database for fuzzy matching entities.

##Execution Examples
####Server
```bash
./cqdb-server -i coeus -t 0 -l 127.0.0.1 -p 15605
./cqdb-server -i crius -t 1537228672809129301 -l 127.0.0.1 -p 15606 -s 127.0.0.1 -e 15605
./cqdb-server -i cronus -t 3074457345618258602 -l 127.0.0.1 -p 15607 -s 127.0.0.1 -e 15605
./cqdb-server -i hyperion -t 4611686018427387903 -l 127.0.0.1 -p 15608 -s 127.0.0.1 -e 15605
./cqdb-server -i lapetus -t 6148914691236517204 -l 127.0.0.1 -p 15609 -s 127.0.0.1 -e 15605
./cqdb-server -i mnemosyne -t 7686143364045646505 -l 127.0.0.1 -p 15610 -s 127.0.0.1 -e 15605
./cqdb-server -i oceanus -t 9223372036854775806 -l 127.0.0.1 -p 15616 -s 127.0.0.1 -e 15605
./cqdb-server -i phoebe -t 10760600709663905107 -l 127.0.0.1 -p 15611 -s 127.0.0.1 -e 15605
./cqdb-server -i rhea -t 12297829382473034408 -l 127.0.0.1 -p 15612 -s 127.0.0.1 -e 15605
./cqdb-server -i tethys -t 13835058055282163709 -l 127.0.0.1 -p 15613 -s 127.0.0.1 -e 15605
./cqdb-server -i theia -t 15372286728091293010 -l 127.0.0.1 -p 15614 -s 127.0.0.1 -e 15605
./cqdb-server -i themis -t 16909515400900422311 -l 127.0.0.1 -p 15615 -s 127.0.0.1 -e 15605
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

##Query Order of Events
1. Queries are parsed and each individual filter is sent to every node
2. A set of entity keys is returned from each node for each filter
3. The union of those sets is determined to be entities that match the query
4. Nodes that are responsible for those entity keys are contacted to get the full set of field values for each entity

##Storage Architecture Example
For this example the token space is 0-99

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

##TODO
- transfer to using rustp2p omnscient framework - going to make the code much cleaner

- use Entity struct in message passing
- remove all the Event stuff - i don't think the crust developers had that right

- start a new thread for each query field message sent - improves performance over iteratively sending to peers
- implement a bunch of "comparators" (maybe change the name, it sucks) - use fuzzy matching algorithsm below
- split up messages into different types - currently struct is Message, go to ClusterManagementMsg, QueryMsg, InsertMsg ...
- use cjqed/rs-natural for fuzzy matching algorithms - already implemented, it's beautiful
- figure out how to parse sql input - maybe a library?
- need to allow bulk loading of values - don't create a connection for each insert
- error handling could use all kinds of work - everywhere
