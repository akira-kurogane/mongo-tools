package mongodocgen

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/text"
	"github.com/mongodb/mongo-tools/common/util"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
	//"io"
	//"os"
	//"path/filepath"
	//"strings"
	"sync"
)

const (
	maxBSONSize         = 16 * (1024 * 1024)
	maxMessageSizeBytes = 2 * maxBSONSize
	workerBufferSize    = 16
	progressBarLength = 24
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

	// insertionLock is used to prevent race conditions in incrementing
	// the insertion count
	insertionLock sync.Mutex

	// insertionCount keeps track of how many documents have successfully
	// been inserted into the database
	insertionCount uint64

	// the tomb is used to synchronize ingestion goroutines and causes
	// other sibling goroutines to terminate immediately if one errors out
	tomb.Tomb

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

type DocumentGenerator struct {
	Template string //A dummy member as a placeholder for the template generator type to come later
}

func (imp *DocumentGenerator) StreamDocument(read chan bson.D) error {
	//DEV in progress. Generates only empty BSON docs
	go func() {
		for i := 0; i < 3; i++ {
			read <- bson.D{}
		}
		close(read)
	}()
	return nil
}

// getDocumentGenerator returns the DocumentGenerator instantiated with the template
func (imp *MongoDocGen) getDocumentGenerator( /*TODO: pass template*/ ) (DocumentGenerator, error) {
	template := "" //to fill the dummy string member
	return DocumentGenerator{template}, nil
}

// GenerateDocuments is used to create the new docs according to the template,
// and then insert them to the database. It returns the number of documents 
// successfully imported to the appropriate namespace and any error encountered
// in doing this
func (imp *MongoDocGen) GenerateDocuments() (uint64, error) {
	/*template, err := imp.getParsedTemplate() //Maybe this should have already been created during option validation?
	if err != nil {
		return 0, err
	}*/

	docGenerator, err := imp.getDocumentGenerator( /*TODO: pass template*/ )
	if err != nil {
		return 0, err
	}

	// DEVNOTE: currently this Progressor does get updated and is thus useless
	// TODO: make it part of MongoDocGen or make a Progress() func and pass imp instead
	watchProgressor := progress.NewCounter(int64(imp.GenerationOptions.Num))
	bar := &progress.Bar{
		Name:      fmt.Sprintf("%v.%v", imp.ToolOptions.DB, imp.ToolOptions.Collection),
		Watching:  watchProgressor,
		Writer:    log.Writer(0),
		BarLength: progressBarLength,
		IsBytes:   true,
	}
	bar.Start()
	defer bar.Stop()
	return imp.generateDocuments(docGenerator)
}

func (imp *MongoDocGen) generateDocuments(documentGenerator DocumentGenerator) (numInserted uint64, retErr error) {
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

	readDocs := make(chan bson.D, workerBufferSize)
	processingErrChan := make(chan error)

	// read and process from the input reader
	go func() {
		processingErrChan <- documentGenerator.StreamDocument(readDocs)
	}()

	// insert documents into the target database
	go func() {
		processingErrChan <- imp.ingestDocuments(readDocs)
	}()

	return imp.insertionCount, channelQuorumError(processingErrChan, 2)
}

// ingestDocuments accepts a channel from which it reads documents to be inserted
// into the target collection. It spreads the insert/upsert workload across one
// or more workers.
func (imp *MongoDocGen) ingestDocuments(readDocs chan bson.D) (retErr error) {
	numInsertionWorkers := imp.IngestOptions.NumInsertionWorkers
	if numInsertionWorkers <= 0 {
		numInsertionWorkers = 1
	}

	// Each ingest worker will return an error which will
	// be set in the following cases:
	//
	// 1. There is a problem connecting with the server
	// 2. The server becomes unreachable
	// 3. There is an insertion/update error - e.g. duplicate key
	//    error - and stopOnError is set to true

	wg := &sync.WaitGroup{}
	mt := &sync.Mutex{}
	for i := 0; i < numInsertionWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// only set the first insertion error and cause sibling goroutines to terminate immediately
			err := imp.runInsertionWorker(readDocs)
			mt.Lock()
			defer mt.Unlock()
			if err != nil && retErr == nil {
				retErr = err
				imp.Kill(err)
			}
		}()
	}
	wg.Wait()
	return
}

// runInsertionWorker is a helper to InsertDocuments - it reads document off
// the read channel and prepares then in batches for insertion into the databas
func (imp *MongoDocGen) runInsertionWorker(readDocs chan bson.D) (err error) {
	session, err := imp.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to mongod: %v", err)
	}
	defer session.Close()
	if err = imp.configureSession(session); err != nil {
		return fmt.Errorf("error configuring session: %v", err)
	}
	collection := session.DB(imp.ToolOptions.DB).C(imp.ToolOptions.Collection)
	var documentBytes []byte
	var documents []bson.Raw
	numMessageBytes := 0

readLoop:
	for {
		select {
		case document, alive := <-readDocs:
			if !alive {
				break readLoop
			}
			// the mgo driver doesn't currently respect the maxBatchSize
			// limit so we self impose a limit by using maxMessageSizeBytes
			// and send documents over the wire when we hit the batch size
			// or when we're at/over the maximum message size threshold
			if len(documents) == imp.ToolOptions.BulkBufferSize || numMessageBytes >= maxMessageSizeBytes {
				if err = imp.insert(documents, collection); err != nil {
					return err
				}
				documents = documents[:0]
				numMessageBytes = 0
			}

			if documentBytes, err = bson.Marshal(document); err != nil {
				return err
			}
			if len(documentBytes) > maxBSONSize {
				log.Logf(log.Always, "warning: attempting to insert document with size %v (exceeds %v limit)",
					text.FormatByteAmount(int64(len(documentBytes))), text.FormatByteAmount(maxBSONSize))
			}
			numMessageBytes += len(documentBytes)
			documents = append(documents, bson.Raw{3, documentBytes})
		case <-imp.Dying():
			return nil
		}
	}

	// ingest any documents left in slice
	if len(documents) != 0 {
		return imp.insert(documents, collection)
	}
	return nil
}

// insert  performs the actual insertion/updates. If no upsert fields are
// present in the document to be inserted, it simply inserts the documents
// into the given collection
func (imp *MongoDocGen) insert(documents []bson.Raw, collection *mgo.Collection) (err error) {
	numInserted := 0
	stopOnError := imp.IngestOptions.StopOnError

	defer func() {
		imp.insertionLock.Lock()
		imp.insertionCount += uint64(numInserted)
		imp.insertionLock.Unlock()
	}()

	if len(documents) == 0 {
		return
	}
	bulk := collection.Bulk()
	for _, document := range documents {
		bulk.Insert(document)
	}
	// mgo.Bulk doesn't currently implement write commands so mgo.BulkResult
	// isn't informative
	_, err = bulk.Run()

	// TOOLS-349: Note that this count may not be entirely accurate if some
	// ingester workers insert when another errors out.
	//
	// Without write commands, we can't say for sure how many documents
	// were inserted when we use bulk inserts so we assume the entire batch
	// insert failed if an error is returned. The result is that we may
	// report that less documents - than were actually inserted - were
	// inserted into the database. This will change as soon as BulkResults
	// are supported by the driver
	if err == nil {
		numInserted = len(documents)
	}
	return filterIngestError(stopOnError, err)
}

