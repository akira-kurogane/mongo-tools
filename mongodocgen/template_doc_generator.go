package mongodocgen

import (
	"gopkg.in/mgo.v2/bson"
)

type TemplateDocumentGenerator struct {
	Template string //A dummy member as a placeholder for the template generator type to come later
}

func (imp *TemplateDocumentGenerator) StreamDocument(read chan bson.D) error {
	//DEV in progress. Generates only dummy BSON docs
	go func() {
		nextBatch := make([]bson.D, 1000)
		for {
			for i := range nextBatch {
				nextBatch[i] = bson.D{{"x", 1}, {"b", true}}
			}
			for i := range nextBatch {
				read <- nextBatch[i]
			}
		}
	}()
	return nil
}
