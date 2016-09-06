# autobahn-oplog

`autobahn-oplog` is a Go package used to tail MongoDB replication oplogs.
It can be used to create a real-time stream of immutable events in [Apache Kafka](http://kafka.apache.org), trigger specific events directly, or something else.

See also [autobahn-binlog](https://github.com/vsco/autobahn-binlog), which does something similar for MySQL.

MongoDB relies on a special capped collection called an oplog (for operations log) to keep data in sync between primary and secondary hosts. `autobahn-oplog` watches the oplog and throws database change events for desired collections into a Go channel.

## Installation

```
go get github.com/vsco/autobahn-oplog
```

## Requirements

- [Go](http://golang.org/doc/install)
- MongoDB with oplog enabled, either
  - by running it in a replica set, or
  - by starting `mongod` with argument `--master`
