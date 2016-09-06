package oplog

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const tailTimeout = 100 * time.Second

// Timestamp represents a parsed version of a bson.MongoTimestamp. Within a single
// mongod instance, timestamp values are always unique.
// (Note that bson.MongoTimestamp is different from the usual MongoDB date type;
// this is an internal format used only in places like the oplog, which is why
// parsing isn't included in standard Mongo drivers.)
type Timestamp struct {
	sec     int32 // seconds since UNIX epoch
	ordinal int32 // an incrementing ordinal for operations within a given second.
}

// Parses a Timestamp out from a bson.MongoTimestamp.
func NewTimestamp(bts bson.MongoTimestamp) *Timestamp {
	ts := new(Timestamp)
	ts.ordinal = int32((bts << 32) >> 32)
	ts.sec = int32(bts >> 32)
	return ts
}

// Returns a bson.MongoTimestamp from Go time.
func NewBsonMongoTimestamp(t time.Time) bson.MongoTimestamp {
	ut := t.Unix()
	if ut < 0 {
		ut = 0
	}
	return bson.MongoTimestamp(ut << 32)
}

// Returns the last bson.MongoTimestamp recorded.
func LastBsonMongoTimestamp(cfg *MongoConfig, session *mgo.Session) bson.MongoTimestamp {
	var oplogDoc OplogDoc
	session.DB(cfg.OplogDatabase).C(cfg.OplogCollection).Find(nil).Sort("-$natural").One(&oplogDoc)
	return oplogDoc.Timestamp
}

type MongoConfig struct {
	Host            string
	Port            uint16
	User            string
	Password        string
	Database        string
	OplogDatabase   string
	OplogCollection string
}
