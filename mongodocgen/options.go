package mongodocgen

var Usage = `<options> <file>

Generate and insert documents based on the template document written in JSON. If the template is not given as a command-line argument it is read from stdin.

See http://docs.mongodb.org/manual/reference/program/mongodocgen/ for more information.`

// GenerationOptions defines the set of options for reading input data.
type GenerationOptions struct {
	// Fields is an option to directly specify comma-separated fields to import to CSV.
	Num uint `long:"num" short:"n" description:"Number of documents to insert"`
}

// Name returns a description of the GenerationOptions struct.
func (_ *GenerationOptions) Name() string {
	return "generation"
}

