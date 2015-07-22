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
				value = "FUNC"
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
		switch arrayVal.(type) {
		case map[string]interface{}:
			docElemArray[ord] = appendMapAsBsonD(bson.D{}, arrayVal.(map[string]interface{}))
		case []interface{}:
			docElemArray[ord] = appendArrayAsBsonD(bson.D{}, arrayVal.([]interface{}))
		case string:
			if arrayVal.(string)[:1] == "$" {
				arrayVal = "FUNC"
			}
			docElemArray[ord] = arrayVal
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

func stampOutDocElem(plugDocElem bson.DocElem) (bson.DocElem, error) {
	//fmt.Printf("Value Type %T elem: %v\n", plugDocElem.Value, plugDocElem)
	switch plugDocElem.Value.(type) {
	case bson.D:
		y, _ := stampOut(plugDocElem.Value.(bson.D))
		return bson.DocElem{plugDocElem.Name, y}, nil
	case []interface{}:
		a := make([]interface{}, len(plugDocElem.Value.([]interface{})))
		for ord, arrayVal := range plugDocElem.Value.([]interface{}) {
			x, _ := stampOutDocElem(bson.DocElem{"dummy_name", arrayVal})
			a[ord] = x.Value
		}
		return bson.DocElem{plugDocElem.Name, a}, nil
	case string:
		if plugDocElem.Value.(string) == "FUNC" {
			plugDocElem.Value = "FUNC -- 0000000"
		}
		return plugDocElem, nil
	default:
		return plugDocElem, nil
	}
}

func stampOut(plugDoc bson.D) (bson.D, error) {
	doc := make([]bson.DocElem, len(plugDoc))
	for ord, plugDocElem := range plugDoc {
		doc[ord], _ = stampOutDocElem(plugDocElem)
	}
	return doc, nil
}

func (tdg *TemplateDocumentGenerator) Next() (bson.D, error) {
	return stampOut(tdg.Plug)
}

func (tdg *TemplateDocumentGenerator) StreamDocument(read chan bson.D) error {
	go func() {
		nextBatch := make([]bson.D, 1000)
		for {
			for i := range nextBatch {
				d, _ := stampOut(tdg.Plug)
				nextBatch[i] = d
			}
			for i := range nextBatch {
				read <- nextBatch[i]
			}
		}
	}()
	return nil
}
