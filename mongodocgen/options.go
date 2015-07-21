package mongodocgen

var Usage = `<options> <file>

Generate and insert documents based on the template document written in JSON. If the template is not given as a command-line argument it is read from stdin.

See http://docs.mongodb.org/manual/reference/program/mongodocgen/ for more information.`

// GenerationOptions defines the set of options for reading input data.
type GenerationOptions struct {
	// Fields is an option to directly specify comma-separated fields to import to CSV.
	Num uint `long:"num" short:"n" description:"Number of documents to insert"`

	// Specifies the location and name of a file containing the data to import.
	File string `long:"file" description:"file to import from; if not specified, stdin is used"`

	//Dev note: The common ToolOptions has a NumDecodingWorkers option. Checked in mongoimport.ValidateSettings
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

