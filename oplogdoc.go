// Package oplog tails MongoDB oplogs.
package oplog

import (
	"gopkg.in/mgo.v2/bson"
)

// OplogDoc represents one document in the oplog collection.
type OplogDoc struct {
	Timestamp    bson.MongoTimestamp `bson:"ts"` // time the operation occurred
	HistoryID    int64               `bson:"h"`  // a unique ID for this operation
	MongoVersion int                 `bson:"v"`
	Operation    string              `bson:"op"` // "i" for insert, "u" for update, "d" for delete, "n" for no-op
	Namespace    string              `bson:"ns"` // database.collection affected by the operation
	Object       *bson.M             `bson:"o"`  // actual document representing the op
	UpdateObject *bson.M             `bson:"o2"` // present for update ops; represents the update criteria
}
