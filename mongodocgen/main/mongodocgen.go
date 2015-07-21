// Main package for the mongodocgen tool.
package main

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongodocgen"
	"os"
)

func main() {
	go signals.Handle()
	// initialize command-line opts
	opts := options.New("mongodocgen", mongodocgen.Usage,
		options.EnabledOptions{Auth: true, Connection: true, Namespace: true})

	generationOpts := &mongodocgen.GenerationOptions{}
	opts.AddOptions(generationOpts)
	ingestOpts := &mongodocgen.IngestOptions{}
	opts.AddOptions(ingestOpts)

	args, err := opts.Parse()
	if err != nil {
		log.Logf(log.Always, "error parsing command line options: %v", err)
		log.Logf(log.Always, "try 'mongodocgen --help' for more information")
		os.Exit(util.ExitBadOptions)
	}

	log.SetVerbosity(opts.Verbosity)

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}

	// connect directly, unless a replica set name is explicitly specified
	_, setName := util.ParseConnectionString(opts.Host)
	opts.Direct = (setName == "")
	opts.ReplicaSetName = setName

	// create a session provider to connect to the db
	sessionProvider, err := db.NewSessionProvider(*opts)
	if err != nil {
		log.Logf(log.Always, "error connecting to host: %v", err)
		os.Exit(util.ExitError)
	}

	m := mongodocgen.MongoDocGen {
		ToolOptions:       opts,
		GenerationOptions: generationOpts,
		IngestOptions:     ingestOpts,
		SessionProvider:   sessionProvider,
	}

	if err = m.ValidateOptions(args); err != nil {
		log.Logf(log.Always, "error validating options: %v", err)
		log.Logf(log.Always, "try 'mongodocgen --help' for more information")
		os.Exit(util.ExitError)
	}

	numDocs, err := m.GenerateDocuments()
	if !opts.Quiet {
		if err != nil {
			log.Logf(log.Always, "Failed: %v", err)
		}
		message := fmt.Sprintf("inserted 1 document")
		if numDocs != 1 {
			message = fmt.Sprintf("inserted %v documents", numDocs)
		}
		log.Logf(log.Always, message)
	}
	if err != nil {
		os.Exit(util.ExitError)
	}
}
