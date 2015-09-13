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

func corralToInt64 (gfn string, jdmv interface{}, x *int64) {
	switch jdmv.(type) {
	case float64:
		*x = int64(jdmv.(float64))
	case json.Number:
		_jni, err := jdmv.(json.Number).Int64()
		if err == nil {
			*x = _jni
		}
	default:
		log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, jdmv)
	}
}

func corralToUInt (gfn string, jdmv interface{}, x *uint) {
	switch jdmv.(type) {
	case float64:
		*x = uint(jdmv.(float64))
	case json.Number:
		_jni, err := jdmv.(json.Number).Int64()
		if err == nil {
			*x = uint(_jni)
		}
	default:
		log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, jdmv)
	}
}

func corralToTimestamp (gfn string, jdmv interface{}, x *time.Time) {
	switch jdmv.(type) {
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
		log.Logf(log.Always, "The \"min\" value in the \"%s\" generator_func object was ignored because it was not a float64 or json.Number type. It was = %#v.", gfn, jdmv)
	}
}

type RandomIntOpts struct {
	Min int64 `map_key:"min"`
	Max int64 `map_key:"max"`
}

func MapToRandomIntOpts(m map[string]interface{}) (o RandomIntOpts) {
	gfn := "RandomInt"
	_min, ok := m["min"]
	if ok {
		corralToInt64(gfn, _min, &o.Min)
	}
	_max, ok := m["max"]
	if ok {
		corralToInt64(gfn, _max, &o.Max)
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
func MapToRandomStringOpts(m map[string]interface{}) (o RandomStringOpts) {
	gfn := "RandomString"
	_l, ok := m["len"]
	if ok {
		corralToUInt(gfn, _l, &o.Length)
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
func MapToRandomBinaryOpts(m map[string]interface{}) (o RandomBinaryOpts) {
	gfn := "RandomBinary"
	_l, ok := m["len"]
	if ok {
		corralToUInt(gfn, _l, &o.Length)
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
func MapToTimestampOpts(m map[string]interface{}) (o TimestampOpts) {
	gfn := "RandomTimestamp"
	_s, ok := m["start_ts"]
	if ok {
		corralToTimestamp(gfn, _s, &o.StartTs)
	}
	_e, ok := m["end_ts"]
	if ok {
		corralToTimestamp(gfn, _e, &o.EndTs)
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

func RandomTimestamp(s time.Time, e time.Time) (t time.Time, err error) {
	if e.IsZero() || e.Before(s) {
		return time.Time{}, fmt.Errorf("the end time was before the start time in RandomTimestamp")
	}
	argMax := *big.NewInt(int64(e.Sub(s)/time.Nanosecond))
	n, _ := rand.Int(rand.Reader, &argMax)
	return s.Add(time.Duration(n.Int64()) * time.Nanosecond), nil
}
