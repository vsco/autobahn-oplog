package oplog

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// A coordinator handles connection to a mongos or mongod and returns a collection
// of tailers as needed: either one per shard (if isMongos = true) or just one (if
// this is a mongod).
type Coordinator struct {
	Tailers       []*OplogTailer
	mongosSession *mgo.Session
	config        *MongoConfig
	tailerQuery   []bson.DocElem
	shards        map[string]string
	isMongos      bool
}

// NewCoordinator returns a new Coordinator with tailers made.
func NewCoordinator(cfg *MongoConfig, mongosSession *mgo.Session, tailerQuery []bson.DocElem) *Coordinator {
	c := &Coordinator{
		mongosSession: mongosSession.Copy(),
		config:        cfg,
		tailerQuery:   tailerQuery,
	}

	c.isMongos = c.sourceIsMongos()
	if c.isMongos {
		err := c.makeShardMap()
		if err != nil {
			return nil
		}
	}

	err := c.makeTailers()
	if err != nil {
		return nil
	}

	return c
}

// makeTailers creates an oplog tailer for each shard (if we're connecting to a
// mongos) or a single tailer for the individual mongod (if we're connecting to a
// mongod).
func (c *Coordinator) makeTailers() error {
	c.Tailers = []*OplogTailer{}

	if c.isMongos {
		if len(c.shards) == 0 {
			return nil
		}

		for _, host := range c.shards {
			tailer := NewOplogTailer(c.config, c.tailerQuery, host)
			c.Tailers = append(c.Tailers, tailer)
		}

		return nil
	}

	tailer := NewOplogTailer(
		c.config,
		c.tailerQuery,
		fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
	)
	c.Tailers = append(c.Tailers, tailer)

	return nil
}

// sourceIsMongos returns true if we're connecting to a mongos.
func (c *Coordinator) sourceIsMongos() bool {
	command := bson.M{
		"isMaster": 1,
	}

	result := bson.M{}
	c.mongosSession.Run(command, &result)

	return result["msg"] == "isdbgrid"
}

// shardNodeForHost returns the node ID from a full hostname.
func (c *Coordinator) shardNodeForHost(extendedHost string) string {
	var (
		err          error
		selectedHost string
		nodeSession  *mgo.Session
	)

	host := strings.Split(strings.Split(extendedHost, "/")[1], ",")[1]
	connectionString := mongoConnectionStringForHostAndConfig(host, c.config)

	nodeSession, err = mgo.Dial(connectionString)
	if err != nil {
		log.Fatalln("could not dial node", err)
	}
	defer nodeSession.Close()

	var (
		rsConfig bson.M
		rsStatus bson.M
	)

	rsConfigMap := make(map[interface{}]bson.M)
	rsStatusMap := make(map[interface{}]interface{})
	command := bson.M{
		"replSetGetStatus": 1,
	}

	nodeSession.DB("admin").Run(command, &rsStatus)
	nodeSession.DB("local").C("system.replset").Find(nil).One(&rsConfig)

	configMembers, ok := rsConfig["members"].([]interface{})
	if ok {
		for _, configMember := range configMembers {
			var bsonConfigMember bson.M
			bsonConfigMember, ok = configMember.(bson.M)
			if ok {
				rsConfigMap[bsonConfigMember["_id"]] = bson.M{
					"host":       bsonConfigMember["host"],
					"slaveDelay": bsonConfigMember["slaveDelay"],
				}
			}
		}
	}

	statusMembers, ok := rsStatus["members"].([]interface{})
	if ok {
		for _, statusMember := range statusMembers {
			bsonStatusMember, ok := statusMember.(bson.M)
			if ok {
				rsStatusMap[bsonStatusMember["_id"]] = bsonStatusMember["state"]
			}
		}
	}

	for id, state := range rsStatusMap {
		if state == 1 || state == 2 {
			if rsConfigMap[id]["slaveDelay"] == 0 || rsConfigMap[id]["slaveDelay"] == nil {
				host, ok := rsConfigMap[id]["host"].(string)
				if ok {
					selectedHost = host
				}
				if state == 2 {
					break
				}
			}
		}
	}

	return selectedHost
}

func mongoConnectionStringForHostAndConfig(host string, cfg *MongoConfig) string {
	if cfg.User == "" && cfg.Password == "" {
		return fmt.Sprintf("%s/%s",
			host,
			cfg.Database)
	}

	return fmt.Sprintf("%s:%s@%s/%s?authSource=admin",
		cfg.User,
		cfg.Password,
		host,
		cfg.Database)
}

// makeShardMap creates a map of shard names to node hostnames and stores the map
// in the coordinator.
func (c *Coordinator) makeShardMap() error {
	shardMap := make(map[string]string)
	result := bson.M{}

	shardIter := c.mongosSession.DB("config").C("shards").Find(nil).Iter()
	defer shardIter.Close()

	var (
		host string
		id   string
		ok   bool
	)

	for shardIter.Next(&result) {
		host, ok = result["host"].(string)
		if !ok {
			return errors.New("could not parse host")
		}

		if strings.Contains(host, "/") {
			id, ok = result["_id"].(string)
			if !ok {
				return errors.New("could not parse id")
			}

			node := c.shardNodeForHost(host)
			shardMap[id] = node
		}
	}

	c.shards = shardMap
	return nil
}
