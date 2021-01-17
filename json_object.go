package psql

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type JSONObject map[string]interface{}

// Scan and Value for db read / write

func (o *JSONObject) Scan(src interface{}) error {
	if src == nil {
		*o = nil
		return nil
	}

	d, ok := src.([]byte)
	if !ok {
		return errors.New("source must be bytes")
	}

	return json.Unmarshal(d, o)
}

func (o JSONObject) Value() (driver.Value, error) {
	return json.Marshal(o)
}

// Getters for json valid json types

var ErrNilValue = errors.New("value is nil")

func (o JSONObject) Object(key string) (JSONObject, error) {
	var val = o[key]

	if val == nil {
		return nil, ErrNilValue
	}

	obj, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("incompatible type %T", val)
	}

	return obj, nil
}

func (o JSONObject) Slice(key string) ([]interface{}, error) {
	var val = o[key]

	if val == nil {
		return nil, ErrNilValue
	}

	arr, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("incompatible type %T", val)
	}

	return arr, nil
}

func (o JSONObject) Float64(key string) (float64, error) {
	var val = o[key]
	switch casted := val.(type) {
	case nil:
		return 0, ErrNilValue
	case float64:
		return casted, nil
	case int:
		return float64(casted), nil
	case int64:
		return float64(casted), nil
	case string:
		return strconv.ParseFloat(casted, 64)
	default:
		return 0, fmt.Errorf("incompatible type %T", val)
	}
}

func (o JSONObject) Bool(key string) (bool, error) {
	var val = o[key]
	switch casted := val.(type) {
	case nil:
		return false, ErrNilValue
	case bool:
		return casted, nil
	case string:
		return strconv.ParseBool(casted)
	default:
		return false, fmt.Errorf("incompatible type %T", val)
	}
}

func (o JSONObject) String(key string) (string, error) {
	var val = o[key]
	switch casted := val.(type) {
	case nil:
		return "", ErrNilValue
	case float64:
		return fmt.Sprintf("%f", casted), nil
	case int, int64:
		return fmt.Sprintf("%d", casted), nil
	case string:
		return casted, nil
	case bool:
		return fmt.Sprintf("%t", casted), nil
	default:
		return "", fmt.Errorf("incompatible type %T", val)
	}
}

// Convenience getters

func (o JSONObject) Int64(key string) (int64, error) {
	var val = o[key]
	switch casted := val.(type) {
	case nil:
		return 0, ErrNilValue
	case float64:
		return int64(casted), nil
	case int:
		return int64(casted), nil
	case int64:
		return casted, nil
	case string:
		return strconv.ParseInt(casted, 10, 64)
	default:
		return 0, fmt.Errorf("incompatible type %T", val)
	}
}

func (o JSONObject) Int(key string) (int, error) {
	var val = o[key]
	switch casted := val.(type) {
	case nil:
		return 0, ErrNilValue
	case float64:
		return int(casted), nil
	case int:
		return casted, nil
	case int64:
		return int(casted), nil
	case string:
		return strconv.Atoi(casted)
	default:
		return 0, fmt.Errorf("incompatible type %T", val)
	}
}
