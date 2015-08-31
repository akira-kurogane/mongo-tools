package mongodocgen

import (
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"fmt"
	"crypto/rand"
	"encoding/base64"
	"math/big"
)


func randomInt(min int64, max int64) int64 {
	argMax := *big.NewInt(max - min)
	n, _ := rand.Int(rand.Reader, &argMax)
	return n.Int64() + min
}

func randomString(l uint) string {
	b := make([]byte, l)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Printf("failure to rand.Read(): %s\n", err)
	}
	return base64.URLEncoding.EncodeToString(b)[:l]
}

func newObjectId() bson.ObjectId {
	return bson.NewObjectId()
}

func randomBinary(l uint) (b []byte) {
	b = make([]byte, l)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Printf("failure to rand.Read(): %s\n", err)
	}
	return
}

type BoundTemplateFunc func() interface{}

type TemplateDocumentGenerator struct {
	Plug bson.D
}

func makeBoundGeneratorFunc(desc string) BoundTemplateFunc {
	if desc == "$string" {
		return func() interface{} {
			return randomString(12)
		}
	} else if desc == "$number" {
		return func() interface{} {
			return randomInt(0, 100)
		}
	} else if desc == "$objectid" {
		return func() interface{} {
			return newObjectId()
		}
	} else if desc == "$bindata" {
		return func() interface{} {
			return bson.Binary{0x0, randomBinary(16)}
		}
	}
	return nil
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
			f := makeBoundGeneratorFunc(value.(string))
			if f != nil {
				doc = append(doc, bson.DocElem{name, f})
			} else {
				doc = append(doc, bson.DocElem{name, value})
			}
		default:
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
		default:
			switch arrayVal.(type) {
			case string:
				f := makeBoundGeneratorFunc(arrayVal.(string))
				if f != nil {
					docElemArray[ord] = f
				} else {
					docElemArray[ord] = arrayVal
				}
			default:
				docElemArray[ord] = arrayVal
			}
		}
	}
	return docElemArray
}

func NewTemplateDocumentGenerator(templateString string) (TemplateDocumentGenerator, error) {
	tdg := TemplateDocumentGenerator{}
	var templateAsMap map[string]interface{}
	err := json.Unmarshal([]byte(templateString), &templateAsMap)
	//fmt.Printf("%#v\n", templateAsMap)
	if err != nil {
		return tdg, fmt.Errorf("Template string was invalid JSON. %s", err)
	}
	tdg.Plug = appendMapAsBsonD(tdg.Plug, templateAsMap)
	return tdg, nil
}

func stampOutDocElem(plugDocElem bson.DocElem) (bson.DocElem, error) {
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
	case BoundTemplateFunc:
		plugDocElem.Value = plugDocElem.Value.(BoundTemplateFunc)()
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
