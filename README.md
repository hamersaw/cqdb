cqdb - Complex Query Database 
=============================

Overview
--------
A distributed database for fuzzy matching string fields of records.

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

TODO
----
* parse out csv file and load into db
* add robustness to client error handling
