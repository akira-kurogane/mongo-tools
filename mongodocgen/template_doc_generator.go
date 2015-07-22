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
			newNestedDocElem := appendMapAsBsonD(bson.D{}, value.(map[string]interface{}))
			doc = append(doc, bson.DocElem{name, newNestedDocElem})
		case []interface{}:
			newNestedArray := appendArrayAsBsonD(bson.D{}, value.([]interface{}))
			doc = append(doc, bson.DocElem{name, newNestedArray})
		case string:
			if value.(string)[:1] == "$" {
				fmt.Printf("Found %s\n", value)
				value = "func goes here"
			}
			doc = append(doc, bson.DocElem{name, value})
		default:
			//fmt.Printf("%s has type %T\n", name, t)
			doc = append(doc, bson.DocElem{name, value})
		}
	}
	return doc
}

func appendArrayAsBsonD (doc bson.D, a []interface{}) []interface{} {
	docElemArray := make([]interface{}, len(a))
	for ord, arrayVal := range a {
		fmt.Printf("ord: %d, val: %#v\n", ord, arrayVal)
		switch arrayVal.(type) {
		case map[string]interface{}:
			docElemArray[ord] = appendMapAsBsonD(bson.D{}, arrayVal.(map[string]interface{}))
		case []interface{}:
			docElemArray[ord] = appendArrayAsBsonD(bson.D{}, arrayVal.([]interface{}))
		default:
			docElemArray[ord] = arrayVal
		}
	}
	return docElemArray
}

func NewTemplateDocumentGenerator(templateString string) (TemplateDocumentGenerator, error) {
	tdg := TemplateDocumentGenerator{}
	var templateAsMap map[string]interface{}
	err := json.Unmarshal([]byte(templateString), &templateAsMap)
	fmt.Printf("%#v\n", templateAsMap)
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
