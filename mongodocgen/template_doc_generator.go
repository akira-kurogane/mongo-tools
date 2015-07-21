package mongodocgen

import (
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"fmt"
)

type TemplateDocumentGenerator struct {
	Plug bson.M
}

func NewTemplateDocumentGenerator(templateString string) (TemplateDocumentGenerator, error) {
	tdg := TemplateDocumentGenerator{}
	var plug map[string]interface{}
	err := json.Unmarshal([]byte(templateString), &plug)
	if err != nil {
		return tdg, fmt.Errorf("Template string was invalid JSON. %s", err)
	}
	tdg.Plug = bson.M(plug)
	return tdg, nil
}

func (tdg *TemplateDocumentGenerator) StreamDocument(read chan bson.D) error {
	//DEV in progress. Generates only dummy BSON docs
	go func() {
		nextBatch := make([]bson.D, 1000)
		for {
			for i := range nextBatch {
				nextBatch[i] = bson.D{} //tdg.Plug
			}
			for i := range nextBatch {
				read <- nextBatch[i]
			}
		}
	}()
	return nil
}
