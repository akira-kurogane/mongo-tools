package mongodocgen

import (
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"fmt"
)

type TemplateDocumentGenerator struct {
	Plug bson.D
}

func appendMapAsBsonD (doc bson.D, m map[string]interface{}) bson.D {
	for name, value := range m {
		switch value.(type) {
		case map[string]interface{}:
			//fmt.Printf("%s has type %T\n", name, t)
			newNestedDocElem := appendMapAsBsonD(bson.D{}, value.(map[string]interface{}))
			doc = append(doc, bson.DocElem{name, newNestedDocElem})
		default:
			//fmt.Printf("%s has type %T\n", name, t)
			doc = append(doc, bson.DocElem{name, value})
		}
	}
	return doc
}


func NewTemplateDocumentGenerator(templateString string) (TemplateDocumentGenerator, error) {
	tdg := TemplateDocumentGenerator{}
	var templateAsMap map[string]interface{}
	err := json.Unmarshal([]byte(templateString), &templateAsMap)
	if err != nil {
		return tdg, fmt.Errorf("Template string was invalid JSON. %s", err)
	}
	tdg.Plug = appendMapAsBsonD(tdg.Plug, templateAsMap)
	return tdg, nil
}

func (tdg *TemplateDocumentGenerator) StreamDocument(read chan bson.D) error {
	go func() {
		nextBatch := make([]bson.D, 1000)
		for {
			for i := range nextBatch {
				nextBatch[i] = tdg.Plug
			}
			for i := range nextBatch {
				read <- nextBatch[i]
			}
		}
	}()
	return nil
}
