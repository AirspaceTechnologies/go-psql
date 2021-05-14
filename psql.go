package psql

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

var (
	ErrNoRows = sql.ErrNoRows

	scanInterfaceType  = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	modelInterfaceType = reflect.TypeOf((*Model)(nil)).Elem()
	timeType           = reflect.TypeOf(time.Time{})
)

type Attrs = map[string]interface{}

// Range

type Range struct {
	Start interface{}
	End   interface{}
}

// Queries

func selectQuery(table string, columns []string, where string, orderBys []string, limit int) string {
	var b StringsBuilder
	var cols string
	if len(columns) == 0 {
		cols = "*"
	} else {
		cols = strings.Join(quoteStrings(columns...), ", ")
	}
	b.WriteStrings("SELECT ", cols, " FROM ", Quote(table))

	if where != "" {
		b.WriteStrings(" WHERE ", where)
	}

	if len(orderBys) > 0 {
		b.WriteStrings(" ORDER BY ", strings.Join(orderBys, ", "))
	}

	if limit > 0 {
		b.WriteStrings(" LIMIT ", strconv.Itoa(limit))
	}

	return b.String()
}

func insertQuery(table string, cols []string, returning []string) string {
	var b StringsBuilder
	placeHolders := placeHolders(1, len(cols))

	colsStr := strings.Join(quoteStrings(cols...), ", ")
	valsStr := strings.Join(placeHolders, ", ")

	if len(returning) == 0 {
		returning = []string{"id"}
	}

	returnCols := strings.Join(quoteStrings(returning...), ", ")

	b.WriteStrings("INSERT INTO ", Quote(table), " (", colsStr, ") VALUES (", valsStr, ") RETURNING ", returnCols)
	return b.String()
}

func updateQuery(table string, cols []string, where string, returning []string) string {
	var b StringsBuilder
	placeHolders := placeHolders(1, len(cols))

	colsStr := strings.Join(quoteStrings(cols...), ", ")
	valsStr := strings.Join(placeHolders, ", ")

	b.WriteStrings("UPDATE ", Quote(table), " SET (", colsStr, ") = ", "ROW(", valsStr, ")")

	if where != "" {
		b.WriteStrings(" WHERE ", where)
	}

	if len(returning) > 0 {
		b.WriteStrings(" RETURNING ", strings.Join(quoteStrings(returning...), ", "))
	}

	return b.String()
}

func deleteQuery(table string, where string, returning []string) string {
	var b StringsBuilder
	b.WriteStrings("DELETE FROM ", Quote(table))

	if where != "" {
		b.WriteStrings(" WHERE ", where)
	}

	if len(returning) > 0 {
		b.WriteStrings(" RETURNING ", strings.Join(quoteStrings(returning...), ", "))
	}

	return b.String()
}

// Helpers

// start should be at 1 so that the first placeholder is $1
func placeHolders(start, size int) []string {
	placeHolders := make([]string, size)
	var b StringsBuilder

	for i := 0; i < size; i++ {
		b.WriteStrings("$", strconv.Itoa(start+i))
		placeHolders[i] = b.String()
		b.Reset()
	}
	return placeHolders
}

func keysValues(m map[string]interface{}) ([]string, []interface{}) {
	keys := make([]string, len(m))
	vals := make([]interface{}, len(m))

	var i int
	for col, v := range m {
		keys[i] = col
		vals[i] = v
		i++
	}

	return keys, vals
}

func idIndex(t reflect.Type) (int, error) {
	if err := verifyStruct(t); err != nil {
		return 0, err
	}

	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		t := f.Tag.Get("sql")
		if t == "id" {
			return i, nil
		}
	}

	return 0, errors.New("no id tag found")
}

func scanStructs(rows *sql.Rows, baseType reflect.Type, sliceElemType reflect.Type, outSliceVal reflect.Value) error {
	fieldIdxs := indexes(baseType)

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	isModel := sliceElemType.Implements(modelInterfaceType)
	isPtr := sliceElemType.Kind() == reflect.Ptr

	for rows.Next() {
		v := reflect.New(baseType).Elem()

		var vals []interface{}
		if isModel {
			vals = modelVals(v, fieldIdxs, cols)
		} else {
			vals = structVals(v, cols)
		}

		if err := rows.Scan(vals...); err != nil {
			return err
		}

		if isPtr {
			outSliceVal.Set(reflect.Append(outSliceVal, v.Addr()))
		} else {
			outSliceVal.Set(reflect.Append(outSliceVal, v))
		}
	}

	return rows.Err()
}

func modelVals(v reflect.Value, fieldIdxs map[string][]int, cols []string) []interface{} {
	var vals []interface{}
	for _, col := range cols {
		idxs, ok := fieldIdxs[col]
		if !ok {
			// add blank val so that scan doesn't fail if the struct does not define a column returned
			var val interface{}
			vals = append(vals, &val)
			continue
		}

		val := fieldAt(v, idxs)
		//if val.IsZero() {
		//	// add blank val so that scan doesn't fail if the struct does not define a column returned
		//	var blankV interface{}
		//	vals = append(vals, &blankV)
		//	continue
		//}

		if val.Kind() != reflect.Ptr {
			val = val.Addr()
		}

		vals = append(vals, val.Interface())
	}
	return vals
}

func structVals(v reflect.Value, cols []string) []interface{} {
	var vals []interface{}
	for idx := range cols {
		if idx >= v.NumField() {
			break
		}

		val := v.Field(idx)
		if val.Kind() != reflect.Ptr {
			val = val.Addr()
		}

		vals = append(vals, val.Interface())
	}
	return vals
}

func scanNatives(rows *sql.Rows, baseType reflect.Type, sliceElemType reflect.Type, outSliceVal reflect.Value) error {
	isPtr := sliceElemType.Kind() == reflect.Ptr

	for rows.Next() {
		v := reflect.New(baseType)

		if err := rows.Scan(v.Interface()); err != nil {
			return err
		}

		if isPtr {
			outSliceVal.Set(reflect.Append(outSliceVal, v))
		} else {
			outSliceVal.Set(reflect.Append(outSliceVal, v.Elem()))
		}
	}

	return rows.Err()
}

func verifyPtr(t reflect.Type) error {
	if t.Kind() != reflect.Ptr {
		return errors.New("requires pointer parameter")
	}

	return nil
}

func verifySlice(t reflect.Type) error {
	if t.Kind() != reflect.Slice {
		return errors.New("requires slice parameter")
	}

	return nil
}

func verifyArray(t reflect.Type) error {
	if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		return errors.New("requires slice or array parameter")
	}

	return nil
}

func verifyStruct(t reflect.Type) error {
	if t.Kind() != reflect.Struct {
		return errors.New("requires struct parameter")
	}

	return nil
}

func scanAsStruct(t reflect.Type) bool {
	baseType := t
	if err := verifyPtr(t); err == nil {
		baseType = t.Elem()
	}

	if baseType.Kind() != reflect.Struct || baseType == timeType {
		return false
	}

	return t.Implements(modelInterfaceType) || !t.Implements(scanInterfaceType)
}

func fieldAt(v reflect.Value, idxs []int) reflect.Value {
	f := v
	for _, idx := range idxs {
		f = f.Field(idx)
	}

	return f
}

func indexes(t reflect.Type) map[string][]int {
	fields := make(map[string][]int)

	if err := verifyStruct(t); err != nil {
		return fields
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag
		col := tag.Get("sql")

		if col == "" {
			if f.Anonymous && f.Type.Kind() == reflect.Struct {
				for jCol, js := range indexes(f.Type) {
					fields[jCol] = append([]int{i}, js...)
				}
			}

			continue
		}

		fields[col] = []int{i}
	}

	return fields
}

// Strings helper

type StringsBuilder struct {
	strings.Builder
}

func (b *StringsBuilder) WriteStrings(strs ...string) {
	for _, str := range strs {
		b.WriteString(str)
	}
}

func Quote(str string) string {
	return pq.QuoteIdentifier(str)
}

// will not quote *
func quoteStrings(strs ...string) []string {
	for i, str := range strs {
		if str == "*" {
			strs[i] = str
			continue
		}

		strs[i] = Quote(str)
	}
	return strs
}
