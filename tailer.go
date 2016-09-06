package oplog

import (
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// An OplogTailer manages tailing one mongod (either a single-mongod deployment or
// one shard of a sharded mongo cluster). The mongod we want to tail is expected
// to be a secondary (to avoid impacting performance).
type OplogTailer struct {
	baseCfg   *MongoConfig
	session   *mgo.Session
	coll      *mgo.Collection
	baseQuery []bson.DocElem
	DbName    string
	Host      string
	InitialTs bson.MongoTimestamp
	OutChan   chan *OplogDoc
	DoneChan  chan bool
}

// NewOplogTailer creates an oplog tailer for a given Mongo configuration and
// specified host to tail.
func NewOplogTailer(baseCfg *MongoConfig, baseQuery []bson.DocElem, host string) *OplogTailer {
	connStr := mongoConnectionStringForHostAndConfig(host, baseCfg)

	session, err := mgo.Dial(connStr)
	if err != nil {
		return nil
	}

	t := &OplogTailer{
		baseCfg:   baseCfg,
		session:   session,
		coll:      session.DB(baseCfg.OplogDatabase).C(baseCfg.OplogCollection),
		baseQuery: baseQuery,
		Host:      host,
		OutChan:   make(chan *OplogDoc),
		DoneChan:  make(chan bool),
	}

	return t
}

func (t *OplogTailer) Start(startSpecifier string) error {
	t.InitialTs = t.timestampForStartSpecifier(t.baseCfg, t.session, startSpecifier)

	go t.tail()

	return nil
}

// timestampForStartSpecifier returns an actual Mongo timestamp given a (string)
// start specifier and pointers to Mongo.
func (t *OplogTailer) timestampForStartSpecifier(baseCfg *MongoConfig, session *mgo.Session, startSpecifier string) bson.MongoTimestamp {
	switch {
	case strings.ToLower(startSpecifier) == "now":
		return LastBsonMongoTimestamp(baseCfg, session)
	case startSpecifier[0] == '-':
		offset, err := strconv.Atoi(startSpecifier[1:])
		if err != nil {
			offset = 0
		}
		return NewBsonMongoTimestamp(time.Now().Add(time.Duration(-offset) * time.Minute))
	}
	return NewBsonMongoTimestamp(time.Now())
}

// tail creates a tailable cursor for the Mongo capped collection in our tailer
// object, tails it, and recreates it if/when it expires.
func (t *OplogTailer) tail() error {
	var iter *mgo.Iter
	lastTs := t.InitialTs

	// Stores the unique op IDs that have been reported for lastTs
	// (keeps us from re-reporting oplog entries when we restart the iterator)
	// Note that timestamps are only unique for a given mongod, so if there are
	// multiple replicaset members there may be multiple oplog entries for a given ts.
	var hidsForLastTs []int64

	for {
		if iter == nil {
			// If we recreate the iterator (e.g. if the cursor's been invalidated)
			// need to move up ts to avoid reporting entries that have already been processed.
			sel := append(t.baseQuery,
				bson.DocElem{
					Name: "ts",
					Value: []bson.DocElem{
						bson.DocElem{
							Name:  "$gte",
							Value: lastTs,
						},
					},
				},
				bson.DocElem{
					Name: "h",
					Value: []bson.DocElem{
						bson.DocElem{
							Name:  "$nin",
							Value: hidsForLastTs,
						},
					},
				},
			)
			// Ref: https://github.com/go-mgo/mgo/issues/121
			query := t.coll.Find(sel)
			query = query.LogReplay()
			iter = query.Tail(tailTimeout)
		}

		var o OplogDoc
		if iter.Next(&o) {
			select {
			case <-t.DoneChan:
				iter.Close()
				return nil
			case t.OutChan <- &o:
			}

			if o.Timestamp > lastTs {
				lastTs = o.Timestamp
				hidsForLastTs = nil
			}
			hidsForLastTs = append(hidsForLastTs, o.HistoryID)
		} else {
			err := iter.Err()
			if err != nil && err != mgo.ErrCursor {
				return err
			}
			if iter.Timeout() {
				continue
			}
			iter = nil
		}
	}
}

func (t *OplogTailer) Stop() error {
	close(t.DoneChan)
	close(t.OutChan)

	return nil
}
