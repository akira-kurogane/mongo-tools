package mongodocgen

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
	"math"
	"math/big"
	"time"
)

// Convenience function for converting a numeric type returned by the standard 
// json decoder lib to int64, which is the integer type the mgo lib expects in
// its bson structs.
func corralToInt64 (jdmv interface{}, x *int64) {
	switch jdmv.(type) {
	case float64:
		*x = int64(jdmv.(float64))
	case json.Number:
		_jni, err := jdmv.(json.Number).Int64()
		if err == nil {
			*x = _jni
		}
	default:
		log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", jdmv)
	}
}

// Convenience function for converting a numeric type returned by the standard 
// json decoder lib to uint. Used to assert length values are >= 0, etc.
func corralToUInt (jdmv interface{}, x *uint) {
	switch jdmv.(type) {
	case float64:
		*x = uint(jdmv.(float64))
	case json.Number:
		_jni, err := jdmv.(json.Number).Int64()
		if err == nil {
			*x = uint(_jni)
		}
	default:
		log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", jdmv)
	}
}

// Convenience function for converting a numeric type returned by the standard 
// json decoder lib to float64, which is the type the mgo lib expects in it's
// bson structs.
func corralToFloat64 (jdmv interface{}, x *float64) {
	switch jdmv.(type) {
	case float64:
		*x = jdmv.(float64)
	case json.Number:
		_jni, err := jdmv.(json.Number).Float64()
		if err == nil {
			*x = _jni
		}
	default:
		log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", jdmv)
	}
}

// Convenience function for converting a ISO 8601 datetime string to a standard 
// time.Time struct.
func corralToTimestamp (jdmv interface{}, x *time.Time) {
	switch jdmv.(type) {
	// time.Parse seems to strictly require all the fields in the string, e.g. 
	// it won't assume 00:00:00 as the time when only the date is specified. 
	// To make it more convenient we parse all the forms below, continuing until
	// a non-erroring case is found.
	case string:
		_x, err := time.Parse("2006-01-02", jdmv.(string))
		if err != nil {
			_x, err = time.Parse("2006-01-02T15:04", jdmv.(string))
		}
		if err != nil {
			_x, err = time.Parse("2006-01-02T15:04:05", jdmv.(string))
		}
		if err != nil {
			_x, err = time.Parse(time.RFC3339, jdmv.(string))
		}
		if err != nil {
			log.Logf(log.Always, "Parse Error: %v\n", err)
		}
		*x = _x
	default:
		log.Logf(log.Always, "corralToTimestamp() cannot convert the value %#v to a timestamp because it was not a string.", jdmv)
	}
}

type RandomIntOpts struct {
	Min int64 `map_key:"min"`
	Max int64 `map_key:"max"`
}

// Makes a RandomIntOpts struct out of a variable map[string] representation of the same options.
func MapToRandomIntOpts(m map[string]interface{}) (o RandomIntOpts) {
	_min, ok := m["min"]
	if ok {
		corralToInt64(_min, &o.Min)
	}
	_max, ok := m["max"]
	if ok {
		corralToInt64(_max, &o.Max)
	}
	//min and max both being 0 is assumed to be just zero-value defaults.
	//Assign to whole int64 range instead.
	if o.Min == 0 && o.Max == 0 {
		o.Min = math.MinInt64
		o.Max = math.MaxInt64
	}
	return
}

func RandomInt(min int64, max int64) int64 {
	argMax := *big.NewInt(max - min)
	n, _ := rand.Int(rand.Reader, &argMax)
	return n.Int64() + min
}

type RandomStringOpts struct {
	Length uint
	DummyLanguage string
}

// Makes a RandomStringOpts struct out of a variable map[string] representation of the same options.
func MapToRandomStringOpts(m map[string]interface{}) (o RandomStringOpts) {
	_l, ok := m["len"]
	if ok {
		corralToUInt(_l, &o.Length)
	}
	//Assume zero is zero-by-default. Set 12 as default instead
	if o.Length == 0 {
		o.Length = 12
	}
	return
}

func RandomString(l uint) string {
	b := make([]byte, l)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Printf("failure to rand.Read(): %s\n", err)
	}
	return base64.URLEncoding.EncodeToString(b)[:l]
}

func NewObjectId() bson.ObjectId {
	return bson.NewObjectId()
}

type RandomBinaryOpts struct {
	Length uint
	DummyLanguage string
}

// Makes a RandomBinaryOpts struct out of a variable map[string] representation of the same options.
func MapToRandomBinaryOpts(m map[string]interface{}) (o RandomBinaryOpts) {
	_l, ok := m["len"]
	if ok {
		corralToUInt(_l, &o.Length)
	}
	//Assume zero is zero-by-default. Set 12 as default instead
	if o.Length == 0 {
		o.Length = 12
	}
	return
}

func RandomBinary(l uint) (b []byte) {
	b = make([]byte, l)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Printf("failure to rand.Read(): %s\n", err)
	}
	return
}

type TimestampOpts struct {
	StartTs time.Time
	EndTs time.Time
}

// Makes a TimestampOpts struct out of a variable map[string] representation of the same options.
func MapToTimestampOpts(m map[string]interface{}) (o TimestampOpts) {
	_s, ok := m["start_ts"]
	if ok {
		corralToTimestamp(_s, &o.StartTs)
	}
	_e, ok := m["end_ts"]
	if ok {
		corralToTimestamp(_e, &o.EndTs)
	}
	//start and end both being 0 is assumed to be just zero-value defaults.
	//Assign to whole date range instead.
	if o.StartTs.IsZero() || o.EndTs.IsZero() {
		fmt.Println("Failed to parse valid Start and End timestamps for the RandomTimestamp function from the following map values: %v\n", m)
	}
	return
}

func CurrentTimestamp() (t time.Time) {
	return time.Now()
}

// Produces a datetime value chosen at random somewhere between the start and end
// datetime arguments.
func RandomTimestamp(s time.Time, e time.Time) (t time.Time, err error) {
	if e.IsZero() || e.Before(s) {
		return time.Time{}, fmt.Errorf("the end time was before the start time in RandomTimestamp")
	}
	argMax := *big.NewInt(int64(e.Sub(s)/time.Nanosecond))
	n, _ := rand.Int(rand.Reader, &argMax)
	return s.Add(time.Duration(n.Int64()) * time.Nanosecond), nil
}

type SequenceOpts struct {
	Start float64
	Step float64
}

// Makes a SequenceOpts struct out of a variable map[string] representation of the same options.
func MapToSequenceOpts(m map[string]interface{}) (o SequenceOpts) {
	_s, ok := m["start"]
	if ok {
		corralToFloat64(_s, &o.Start)
	} else {
		o.Start = 0
	}
	_p, ok := m["step"]
	if ok {
		corralToFloat64(_p, &o.Step)
	} else {
		o.Step = 1
	}
	return
}

// Creates a closure which return the Start argument on the first execution
// and increment by Step on each subsequent call.
func CreateNewSequenceFunc(Start float64, Step float64) BoundTemplateFunc {
	x := Start
	step := Step
	return func() interface{} {
		_x := x
		x = x + step
		return _x
	}
}
