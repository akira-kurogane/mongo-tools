package mongodocgen

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/util"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"gopkg.in/tomb.v2"
	//"io"
	//"os"
	//"path/filepath"
	//"strings"
	//"sync"
)

type MongoDocGen struct {
	// generic mongo tool options
	ToolOptions *options.ToolOptions

	// InputOptions defines options used to read data to be ingested
	GenerationOptions *GenerationOptions

	// IngestOptions defines options used to ingest data into MongoDB
	IngestOptions *IngestOptions

	// SessionProvider is used for connecting to the database
	SessionProvider *db.SessionProvider

	// type of node the SessionProvider is connected to
	nodeType db.NodeType
}

// ValidateSettings ensures that the tool specific options supplied for
// MongoDocGen are valid.
func (imp *MongoDocGen) ValidateSettings(args []string) error {

	if imp.ToolOptions.DB == "" {
		imp.ToolOptions.DB = "test"
	}
	err := util.ValidateDBName(imp.ToolOptions.DB)
	if err != nil {
		return fmt.Errorf("invalid database name: %v", err)
	}

	// set the number of insertion workers to use for imports
	if imp.IngestOptions.NumInsertionWorkers <= 0 {
		imp.IngestOptions.NumInsertionWorkers = 1
	}

	log.Logf(log.DebugLow, "using %v insert workers", imp.IngestOptions.NumInsertionWorkers)

	// ensure no more than one positional argument is supplied
	if len(args) > 1 {
		return fmt.Errorf("only one positional argument is allowed")
	}

	// ensure either a positional argument is supplied or an argument is passed
	// to the --file flag - and not both
	if imp.GenerationOptions.File != "" && len(args) != 0 {
		return fmt.Errorf("incompatible options: --file and positional argument(s)")
	}

	if imp.GenerationOptions.File == "" {
		if len(args) != 0 {
			// if --file is not supplied, use the positional argument supplied
			imp.GenerationOptions.File = args[0]
		}
	}

	return nil
}

// configureSession takes in a session and modifies it with properly configured
// settings. It does the following configurations:
//
// 1. Sets the session to not timeout
// 2. Sets the write concern on the session
//
// returns an error if it's unable to set the write concern
func (imp *MongoDocGen) configureSession(session *mgo.Session) error {
	// sockets to the database will never be forcibly closed
	session.SetSocketTimeout(0)
	sessionSafety, err := db.BuildWriteConcern(imp.IngestOptions.WriteConcern, imp.nodeType)
	if err != nil {
		return fmt.Errorf("write concern error: %v", err)
	}
	session.SetSafe(sessionSafety)
	return nil
}

func (imp* MongoDocGen) GenerateDocuments() (int, error) {
	session, err := imp.SessionProvider.GetSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()

	connURL := imp.ToolOptions.Host
	if connURL == "" {
		connURL = util.DefaultHost
	}
	if imp.ToolOptions.Port != "" {
		connURL = connURL + ":" + imp.ToolOptions.Port
	}
	log.Logf(log.Always, "connected to: %v", connURL)

	log.Logf(log.Info, "ns: %v.%v",
		imp.ToolOptions.Namespace.DB,
		imp.ToolOptions.Namespace.Collection)

	// check if the server is a replica set, mongos, or standalone
	imp.nodeType, err = imp.SessionProvider.GetNodeType()
	if err != nil {
		return 0, fmt.Errorf("error checking connected node type: %v", err)
	}
	log.Logf(log.Info, "connected to node type: %v", imp.nodeType)

	if err = imp.configureSession(session); err != nil {
		return 0, fmt.Errorf("error configuring session: %v", err)
	}

	// drop the collection if necessary
	if imp.IngestOptions.Drop {
		log.Logf(log.Always, "dropping: %v.%v",
			imp.ToolOptions.DB,
			imp.ToolOptions.Collection)
		collection := session.DB(imp.ToolOptions.DB).
			C(imp.ToolOptions.Collection)
		if err := collection.DropCollection(); err != nil {
			if err.Error() != db.ErrNsNotFound.Error() {
				return 0, err
			}
		}
	}

	/*** Begin rubbish section to test mgo functionality can be used ***/
	documents := make([]bson.D, 1000)
	numInserted := 0
	collection := session.DB(imp.ToolOptions.DB).C(imp.ToolOptions.Collection)

	var i uint
	for i = 0; i < imp.GenerationOptions.Num; i++ {
		document := bson.D{}
		documents[i] = document
		//Add to bson objects to documents
	}

	bulk := collection.Bulk()
	for _, document := range documents[:imp.GenerationOptions.Num] {
		bulk.Insert(document)
	}

	// mgo.Bulk doesn't currently implement write commands so mgo.BulkResult
	// isn't informative
	_, err = bulk.Run()

	// TOOLS-349: Note that this count may not be entirely accurate if some
	// ingester workers insert when another errors out.
	if err == nil {
		numInserted += len(documents[:imp.GenerationOptions.Num])
	}
	/*** End rubbish section ***/

	return numInserted, nil
}
