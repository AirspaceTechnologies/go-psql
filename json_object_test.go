// +build albatross

package psql

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJsonObject_Test(t *testing.T) {
	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.Exec(modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		c.Exec("drop table mock_models")
		c.Close()
	}()

	ctx := context.Background()

	var val float64 = 1 // numbers get unmarshaled as float64

	m := &MockModel{JSONObject: JSONObject{"key": val}}

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	var results []*MockModel

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	result := results[0]

	require.Equal(t, val, result.JSONObject["key"])

	// nil out

	m.JSONObject = nil

	if err := c.Update(ctx, m, "json_object"); err != nil {
		t.Fatalf("Error updating %v", err)
	}

	results = nil

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	result = results[0]

	require.Equal(t, nil, result.JSONObject["key"])

}

func TestJsonObject_Slice(t *testing.T) {
	str := `{ "key": [ { "inner_key": 1 } ] }`

	var obj JSONObject
	if err := json.Unmarshal([]byte(str), &obj); err != nil {
		t.Fatalf("failed to unmarshal %v", err)
	}

	arr, err := obj.Slice("key")
	if err != nil {
		t.Fatalf("failed to get object %v", err)
	}

	var inner JSONObject = arr[0].(map[string]interface{})

	i, err := inner.Int("inner_key")
	if err != nil {
		t.Fatalf("failed to get inner int %v", err)
	}

	require.Equal(t, 1, i)
}

func TestJsonObject_Object(t *testing.T) {
	str := `{ "key": { "inner_key": 1 } }`

	var obj JSONObject
	if err := json.Unmarshal([]byte(str), &obj); err != nil {
		t.Fatalf("failed to unmarshal %v", err)
	}

	inner, err := obj.Object("key")
	if err != nil {
		t.Fatalf("failed to get object %v", err)
	}

	i, err := inner.Int("inner_key")
	if err != nil {
		t.Fatalf("failed to get inner int %v", err)
	}

	require.Equal(t, 1, i)

	// nil

	str = `{ "key": null }`

	if err = json.Unmarshal([]byte(str), &obj); err != nil {
		t.Fatalf("failed to unmarshal %v", err)
	}

	_, err = obj.Object("key")
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestJsonObject_Float64(t *testing.T) {
	var nilTimePtr *time.Time

	tcs := []struct {
		Val   interface{}
		Error bool
	}{
		{
			Val:   float64(1000),
			Error: false,
		},
		{
			Val:   int(1000),
			Error: false,
		},
		{
			Val:   int64(1000),
			Error: false,
		},
		{
			Val:   "1000",
			Error: false,
		},
		{
			Val:   "2019-01-31T14:27:03.000-08:00",
			Error: true,
		},
		{
			Val:   "2019-01-31T14:27:03.000-0800",
			Error: true,
		},
		{
			Val:   "a",
			Error: true,
		},
		{
			Val:   nil,
			Error: true,
		},
		{
			Val:   nilTimePtr,
			Error: true,
		},
		{
			Val:   time.Time{},
			Error: true,
		},
		{
			Val:   &time.Time{},
			Error: true,
		},
		{
			Val:   true,
			Error: true,
		},
	}

	for i, tc := range tcs {
		obj := make(JSONObject)
		obj["key"] = tc.Val

		_, err := obj.Float64("key")

		if tc.Val == nil {
			if err != ErrNilValue {
				t.Fatalf("expected ErrNilValue")
			}
			continue
		}

		if err == nil && tc.Error {
			t.Fatalf("expected an error for tc %v", i)
		} else if err != nil && !tc.Error {
			t.Fatalf("unexpected an error for tc %v %v", i, err)
		}
	}
}

func TestJsonObject_Int64(t *testing.T) {
	var nilTimePtr *time.Time

	tcs := []struct {
		Val   interface{}
		Error bool
	}{
		{
			Val:   float64(1000),
			Error: false,
		},
		{
			Val:   int(1000),
			Error: false,
		},
		{
			Val:   int64(1000),
			Error: false,
		},
		{
			Val:   "1000",
			Error: false,
		},
		{
			Val:   "2019-01-31T14:27:03.000-08:00",
			Error: true,
		},
		{
			Val:   "2019-01-31T14:27:03.000-0800",
			Error: true,
		},
		{
			Val:   "a",
			Error: true,
		},
		{
			Val:   nil,
			Error: true,
		},
		{
			Val:   nilTimePtr,
			Error: true,
		},
		{
			Val:   time.Time{},
			Error: true,
		},
		{
			Val:   &time.Time{},
			Error: true,
		},
		{
			Val:   true,
			Error: true,
		},
	}

	for i, tc := range tcs {
		obj := make(JSONObject)
		obj["key"] = tc.Val

		_, err := obj.Int64("key")
		if err == nil && tc.Error {
			t.Fatalf("expected an error for tc %v", i)
		} else if err != nil && !tc.Error {
			t.Fatalf("unexpected an error for tc %v %v", i, err)
		}
	}
}

func TestJsonObject_Int(t *testing.T) {
	var nilTimePtr *time.Time

	tcs := []struct {
		Val   interface{}
		Error bool
	}{
		{
			Val:   float64(1000),
			Error: false,
		},
		{
			Val:   int(1000),
			Error: false,
		},
		{
			Val:   int64(1000),
			Error: false,
		},
		{
			Val:   "1000",
			Error: false,
		},
		{
			Val:   "2019-01-31T14:27:03.000-08:00",
			Error: true,
		},
		{
			Val:   "2019-01-31T14:27:03.000-0800",
			Error: true,
		},
		{
			Val:   "a",
			Error: true,
		},
		{
			Val:   nil,
			Error: true,
		},
		{
			Val:   nilTimePtr,
			Error: true,
		},
		{
			Val:   time.Time{},
			Error: true,
		},
		{
			Val:   &time.Time{},
			Error: true,
		},
		{
			Val:   true,
			Error: true,
		},
	}

	for i, tc := range tcs {
		obj := make(JSONObject)
		obj["key"] = tc.Val

		_, err := obj.Int("key")

		if tc.Val == nil {
			if err != ErrNilValue {
				t.Fatalf("expected ErrNilValue")
			}
			continue
		}

		if err == nil && tc.Error {
			t.Fatalf("expected an error for tc %v", i)
		} else if err != nil && !tc.Error {
			t.Fatalf("unexpected an error for tc %v %v", i, err)
		}
	}
}

func TestJsonObject_Bool(t *testing.T) {
	tcs := []struct {
		Val   interface{}
		Error bool
	}{
		{
			Val:   float64(1000),
			Error: true,
		},
		{
			Val:   int(1000),
			Error: true,
		},
		{
			Val:   int64(1000),
			Error: true,
		},
		{
			Val:   "1000",
			Error: true,
		},
		{
			Val:   "2019-01-31T14:27:03.000-08:00",
			Error: true,
		},
		{
			Val:   "a",
			Error: true,
		},
		{
			Val:   "true",
			Error: false,
		},
		{
			Val:   "0",
			Error: false,
		},
		{
			Val:   nil,
			Error: true,
		},
		{
			Val:   time.Time{},
			Error: true,
		},
		{
			Val:   &time.Time{},
			Error: true,
		},
		{
			Val:   true,
			Error: false,
		},
	}

	for i, tc := range tcs {
		obj := make(JSONObject)
		obj["key"] = tc.Val

		_, err := obj.Bool("key")

		if tc.Val == nil {
			if err != ErrNilValue {
				t.Fatalf("expected ErrNilValue")
			}
			continue
		}

		if err == nil && tc.Error {
			t.Fatalf("expected an error for tc %v", i)
		} else if err != nil && !tc.Error {
			t.Fatalf("unexpected an error for tc %v %v", i, err)
		}
	}
}

func TestJsonObject_String(t *testing.T) {
	var nilTimePtr *time.Time

	tcs := []struct {
		Val   interface{}
		Error bool
	}{
		{
			Val:   float64(1000),
			Error: false,
		},
		{
			Val:   int(1000),
			Error: false,
		},
		{
			Val:   int64(1000),
			Error: false,
		},
		{
			Val:   "1000",
			Error: false,
		},
		{
			Val:   "2019-01-31T14:27:03.000-08:00",
			Error: false,
		},
		{
			Val:   "2019-01-31T14:27:03.000-0800",
			Error: false,
		},
		{
			Val:   "a",
			Error: false,
		},
		{
			Val:   nil,
			Error: true,
		},
		{
			Val:   nilTimePtr,
			Error: true,
		},
		{
			Val:   time.Time{},
			Error: true,
		},
		{
			Val:   &time.Time{},
			Error: true,
		},
		{
			Val:   true,
			Error: false,
		},
	}

	for i, tc := range tcs {
		obj := make(JSONObject)
		obj["key"] = tc.Val

		_, err := obj.String("key")

		if tc.Val == nil {
			if err != ErrNilValue {
				t.Fatalf("expected ErrNilValue")
			}
			continue
		}

		if err == nil && tc.Error {
			t.Fatalf("expected an error for tc %v", i)
		} else if err != nil && !tc.Error {
			t.Fatalf("unexpected an error for tc %v %v", i, err)
		}
	}
}
