package mongodocgen

import (
	"github.com/mongodb/mongo-tools/common/log"
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

func makeBoundGeneratorFunc(m map[string]interface{}) BoundTemplateFunc {
	gfn := m["generator_func"]
	if gfn == "RandomString" {
		var l uint = 12
		_l, ok := m["len"]
		if ok {
			switch _l.(type) {
			case float64:
				l = uint(_l.(float64))
			case json.Number:
				_jni, err := _l.(json.Number).Int64()
				if err == nil {
					l = uint(_jni)
				}
			default:
				log.Logf(log.Always, "The \"len\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, _l)
			}
		}
		return func() interface{} {
			return randomString(l)
		}
	} else if gfn == "RandomInt" {
		var min, max int64 = 0, 100
		_min, ok := m["min"]
		if ok {
			switch _min.(type) {
			case float64:
				min = int64(_min.(float64))
			case json.Number:
				_jni, err := _min.(json.Number).Int64()
				if err == nil {
					min = _jni
				}
			default:
				log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, _min)
			}
		}
		_max, ok := m["max"]
		if ok {
			switch _max.(type) {
			case float64:
				max = int64(_max.(float64))
			case json.Number:
				_jni, err := _max.(json.Number).Int64()
				if err == nil {
					max = _jni
				}
			default:
				log.Logf(log.Always, "The \"max\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, _max)
			}
		}
		return func() interface{} {
			return randomInt(min, max)
		}
	} else if gfn == "ObjectId" {
		return func() interface{} {
			return newObjectId()
		}
	} else if gfn == "RandomBinary" {
		var l uint = 12
		_l, ok := m["len"]
		if ok {
			switch _l.(type) {
			case float64:
				l = uint(_l.(float64))
			case json.Number:
				_jni, err := _l.(json.Number).Int64()
				if err == nil {
					l = uint(_jni)
				}
			default:
				log.Logf(log.Always, "The \"len\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, _l)
			}
		}
		return func() interface{} {
			return bson.Binary{0x0, randomBinary(l)}
		}
	} else {
		log.Logf(log.Always, "A generator_func value %v was encountered. As it did not (case-sensitively) match any of the expected generator function names it is being ignored", gfn)
	}
	return nil
}

func appendMapAsBsonD (doc bson.D, m map[string]interface{}) bson.D {
	for name, value := range m {
		switch value.(type) {
		case map[string]interface{}:
			_, elem_found := value.(map[string]interface{})["generator_func"]
			var f BoundTemplateFunc
			if elem_found {
				f = makeBoundGeneratorFunc(value.(map[string]interface{}))
			}
			if f != nil {
				doc = append(doc, bson.DocElem{name, f})
			} else {
				newNestedDocElem := appendMapAsBsonD(bson.D{}, value.(map[string]interface{}))
				doc = append(doc, bson.DocElem{name, newNestedDocElem})
			}
		case []interface{}:
			newNestedArray := appendArrayAsBsonD(bson.D{}, value.([]interface{}))
			doc = append(doc, bson.DocElem{name, newNestedArray})
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
			_, elem_found := arrayVal.(map[string]interface{})["generator_func"]
			var f BoundTemplateFunc
			if elem_found {
				f = makeBoundGeneratorFunc(arrayVal.(map[string]interface{}))
			}
			if f != nil {
				docElemArray[ord] = f
			} else {
				docElemArray[ord] = appendMapAsBsonD(bson.D{}, arrayVal.(map[string]interface{}))
			}
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
