package psql

import (
	"fmt"
	"reflect"
)

type Model interface {
	TableName() string
}

type ModelHelper struct {
	Model Model
}

// Returns a map of the attributes of the struct excluding id
func (h ModelHelper) Attributes(only ...string) map[string]interface{} {
	t := reflect.TypeOf(h.Model)
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}

	v := reflect.ValueOf(h.Model)
	if isPtr {
		v = v.Elem()
	}

	idxs := indexes(t)
	n := len(idxs) - 1
	if n < 0 {
		n = 0
	}

	l := len(only)
	onlyMap := make(map[string]bool, l)
	for _, s := range only {
		onlyMap[s] = true
	}

	attrs := make(map[string]interface{}, n)

	for col, idx := range idxs {
		if col == "id" {
			continue
		}

		if l > 0 && !onlyMap[col] {
			continue
		}

		attrs[col] = fieldAt(v, idx).Interface()
	}

	return attrs
}

func (h ModelHelper) ID() (int64, error) {
	t := reflect.TypeOf(h.Model)
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}

	idIndex, err := idIndex(t)
	if err != nil {
		return 0, err
	}

	v := reflect.ValueOf(h.Model)
	if isPtr {
		v = v.Elem()
	}

	f := v.Field(idIndex)

	switch v := f.Interface().(type) {
	case int, int64:
		return f.Int(), nil
	case Inter:
		return v.Int()
	default:
		return 0, fmt.Errorf("unsupported id type of %v", t)
	}
}

func (h ModelHelper) SetID(id int64) error {
	t := reflect.TypeOf(h.Model)
	if err := verifyPtr(t); err != nil {
		return err
	}

	t = t.Elem()

	index, err := idIndex(t)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(h.Model).Elem()
	f := v.Field(index)

	switch f.Interface().(type) {
	case int, int64:
		f.SetInt(id)
	case NullInt64:
		n := reflect.ValueOf(NewNullInt64(id))
		f.Set(n)
	default:
		return fmt.Errorf("unsupported id of type %v", t)
	}

	return nil
}
