package mongodocgen

import (
	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
)

// channelQuorumError takes a channel and a quorum - which specifies how many
// messages to receive on that channel before returning. It either returns the
// first non-nil error received on the channel or nil if up to `quorum` nil
// errors are received
func channelQuorumError(ch <-chan error, quorum int) (err error) {
	for i := 0; i < quorum; i++ {
		if err = <-ch; err != nil {
			return
		}
	}
	return
}

// filterIngestError accepts a boolean indicating if a non-nil error should be,
// returned as an actual error.
//
// If the error indicates an unreachable server, it returns that immediately.
//
// If the error indicates an invalid write concern was passed, it returns nil
//
// If the error is not nil, it logs the error. If the error is an io.EOF error -
// indicating a lost connection to the server, it sets the error as such.
//
func filterIngestError(stopOnError bool, err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == db.ErrNoReachableServers.Error() {
		return err
	}
	if err.Error() == io.EOF.Error() {
		err = db.ErrLostConnection
	}
	log.Logf(log.Always, "error inserting documents: %v", err)
	if stopOnError || err == db.ErrLostConnection {
		return err
	}
	return nil
}

// setNestedValue takes a nested field - in the form "a.b.c" -
// its associated value, and a document. It then assigns that
// value to the appropriate nested field within the document
func setNestedValue(key string, value interface{}, document *bson.D) {
	index := strings.Index(key, ".")
	if index == -1 {
		*document = append(*document, bson.DocElem{key, value})
		return
	}
	keyName := key[0:index]
	subDocument := &bson.D{}
	elem, err := bsonutil.FindValueByKey(keyName, document)
	if err != nil { // no such key in the document
		elem = nil
	}
	var existingKey bool
	if elem != nil {
		subDocument = elem.(*bson.D)
		existingKey = true
	}
	setNestedValue(key[index+1:], value, subDocument)
	if !existingKey {
		*document = append(*document, bson.DocElem{keyName, subDocument})
	}
}

