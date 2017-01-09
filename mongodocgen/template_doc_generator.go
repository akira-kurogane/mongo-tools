package mongodocgen

import (
	"github.com/mongodb/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"fmt"
)

type BoundTemplateFunc func() interface{}

type TemplateDocumentGenerator struct {
	Plug bson.D
}

func makeBoundGeneratorFunc(m map[string]interface{}) (BoundTemplateFunc, error) {
	gfn := m["generator_func"]
	if gfn == "RandomString" {
		opts, err := MapToRandomStringOpts(m)
		if err != nil {
			return nil, err
		}
		return func() interface{} {
			return RandomString(opts.Length)
		}, nil
	} else if gfn == "RandomInt" {
		opts, err := MapToRandomIntOpts(m)
		if err != nil {
			return nil, err
		}
		return func() interface{} {
			return RandomInt(opts.Min, opts.Max)
		}, nil
	//TODO: } else if gfn == "RandomBoolean" {
		//opts, err := MapToRandomBooleanOpts(m) //to have a TrueLikelihood 0.0 ~ 1.0 argument
		//if err != nil {
		//	return nil, err
		//}
		//return func() interface{} {
		//	return RandomBoolean(opts.TrueLikelihood)
		//}, nil
	} else if gfn == "ObjectId" {
		return func() interface{} {
			return NewObjectId()
		}, nil
	} else if gfn == "RandomBinary" {
		opts, err := MapToRandomBinaryOpts(m)
		if err != nil {
			return nil, err
		}
		return func() interface{} {
			return bson.Binary{0x0, RandomBinary(opts.Length)}
		}, nil
	} else if gfn == "CurrentTimestamp" {
		return func() interface{} {
			return CurrentTimestamp()
		}, nil
	} else if gfn == "RandomTimestamp" {
		opts, err := MapToTimestampOpts(m)
		if err != nil {
			return nil, err
		}
		return func() interface{} {
			t, err := RandomTimestamp(opts.StartTs, opts.EndTs)
			if err != nil {
				return err
			}
			return t
		}, nil
	} else if gfn == "Sequence" {
		opts, err := MapToSequenceOpts(m)
		if err != nil {
			return nil, err
		}
		seqFunc := CreateNewSequenceFunc(opts.Start, opts.Step)
		log.Logvf(log.DebugLow, "A sequence for (Start = %f, Step = %f) is created.", gfn)
		return seqFunc, nil
	} else {
		log.Logvf(log.Always, "A generator_func value %v was encountered. As it did not (case-sensitively) match any of the expected generator function names it is being ignored", gfn)
	}
	return nil, nil
}

func appendMapAsBsonD (doc bson.D, m map[string]interface{}) bson.D {
	for name, value := range m {
		switch value.(type) {
		case map[string]interface{}:
			_, elem_found := value.(map[string]interface{})["generator_func"]
			var f BoundTemplateFunc
			var err error
			if elem_found {
				f, err = makeBoundGeneratorFunc(value.(map[string]interface{}))
				if err != nil {
					log.Logvf(log.Always, "Could not parse \"%s\" (%#v) into a generator function due to following error: %s", name, value.(map[string]interface{}), err.Error())
				}
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
			var err error
			if elem_found {
				f, err = makeBoundGeneratorFunc(arrayVal.(map[string]interface{}))
				if err != nil {
					log.Logvf(log.Always, "Could not parse %#v into a generator function due to following error: %s", arrayVal.(map[string]interface{}), err.Error())
				}
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
