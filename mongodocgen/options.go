package mongodocgen

var Usage = `<options> -n NUM <template>

Generate and insert documents based on the template document written in JSON. If the template is not given as a command-line argument it is read from stdin.

See http://docs.mongodb.org/manual/reference/program/mongodocgen/ for more information.`

// GenerationOptions defines the set of options for reading input data.
type GenerationOptions struct {
	// Num is the number of documents that should be inserted
	Num uint64 `long:"num" short:"n" description:"Number of documents to insert"`

	// Template string. Hidden as '--option' because it's a positional arg.
	Template string `no-flag:"true" long:"template"`
}

// Name returns a description of the GenerationOptions struct.
func (_ *GenerationOptions) Name() string {
	return "generation"
}

// IngestOptions defines the set of options for storing data.
type IngestOptions struct {
	// Drops target collection before importing.
	Drop bool `long:"drop" description:"drop collection before inserting documents"`

	// Sets the number of insertion routines to use
	NumInsertionWorkers int `short:"j" long:"numInsertionWorkers" description:"number of insert operations to run concurrently (defaults to 1)" default:"1" default-mask:"-"`

	// Forces mongodocgen to halt the import operation at the first insert or upsert error.
	StopOnError bool `long:"stopOnError" description:"stop importing at first insert/upsert error"`

	// Sets write concern level for write operations.
	WriteConcern string `long:"writeConcern" default:"majority" default-mask:"-" description:"write concern options e.g. --writeConcern majority, --writeConcern '{w: 3, wtimeout: 500, fsync: true, j: true}' (defaults to 'majority')"`
}

// Name returns a description of the IngestOptions struct.
func (_ *IngestOptions) Name() string {
	return "ingest"
}

