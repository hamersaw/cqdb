cqdb - Complex Query Database 
=============================

Overview
--------
A distributed database for fuzzy matching entities.

Examples
--------
Server
```bash
./cqdb-server -i croeus -t 15605 -l 127.0.0.1 -p 15605
./cqdb-server -i crius -t 15606 -l 127.0.0.1 -p 15606 -s 127.0.0.1 -e 15605
./cqdb-server -i cronus -t 15607 -l 127.0.0.1 -p 15607 -s 127.0.0.1 -e 15605
./cqdb-server -i hyperion -t 15608 -l 127.0.0.1 -p 15608 -s 127.0.0.1 -e 15605
./cqdb-server -i lapetus -t 15609 -l 127.0.0.1 -p 15609 -s 127.0.0.1 -e 15605
./cqdb-server -i mnemosyne -t 15610 -l 127.0.0.1 -p 15610 -s 127.0.0.1 -e 15605
./cqdb-server -i phoebe -t 15611 -l 127.0.0.1 -p 15611 -s 127.0.0.1 -e 15605
./cqdb-server -i rhea -t 15612 -l 127.0.0.1 -p 15612 -s 127.0.0.1 -e 15605
./cqdb-server -i tethys -t 15613 -l 127.0.0.1 -p 15613 -s 127.0.0.1 -e 15605
./cqdb-server -i theia -t 15614 -l 127.0.0.1 -p 15614 -s 127.0.0.1 -e 15605
./cqdb-server -i themis -t 15615 -l 127.0.0.1 -p 15615 -s 127.0.0.1 -e 15605
```

Client
```bash
./cqdb-client -i 127.0.0.1 -p 15605
````
Storage Architecture Concepts
-----------------------------
* All of the fields of an entity are hashed to compute a record key
* A entities record key determines which node the entities full set of field values is stored on
* Each field value of an entity is hashed to compute a field value key
* A pointer to the record key is stored on the appropriate machine for each field value

Query Order of Events
---------------------
1. Queries are parsed and each individual filter is sent to every node
2. A set of entity keys is returned from each node for each filter
3. The union of those sets is determined to be entities that match the query
4. Nodes that are responsible for those entity keys are contacted to get the full set of field values for each entity

Storage Architecture Example
----------------------------
For this example the token space is 0-99

node1 - token:33
	entities:
	fields:
		first_name:
			daniel -> 23,43
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

Insertions
	"first_name:daniel middle_name:peter last_name:rammer" -> hash -> 43
	daniel -> hash -> 23
	peter -> hash -> 27
	rammer -> hash -> 68

	"first_name:daniel middle_name:wroughton last_name:craig" -> hash -> 58
	daniel -> hash -> 23
	wroughton -> hash -> 54
	craig -> hash -> 83

TODO
----
* add robustness to client error handling
* need to allow bulk loading of values - don't create a connection for each insert
