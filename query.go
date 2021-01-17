package psql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"errors"
)

type Query struct {
	tableName  string
	action     string                 // can be select, update, insert, or delete
	conditions []*condition           // stores conditions for where clause
	values     map[string]interface{} // stores values for insert or update
	columns    []string               // stores columns for select
	client     *Client
	ors        []*Query
	ands       []*Query
	orderBys   []string
	limit      int
	returning  []string // holds columns to return for insert, update, and delete
}

func SubQuery() *Query {
	var q Query
	return &q
}

func (q *Query) Where(attrs map[string]interface{}) *Query {
	for col, val := range attrs {
		q.conditions = append(q.conditions, &condition{col: col, val: val, negative: false})
	}
	return q
}

func (q *Query) WhereNot(attrs map[string]interface{}) *Query {
	for col, val := range attrs {
		q.conditions = append(q.conditions, &condition{col: col, val: val, negative: true})
	}
	return q
}

// Instead of the where clause using field = $1 use field = %v
func (q *Query) WhereRaw(raw string, vals ...interface{}) *Query {
	q.conditions = append(q.conditions, &condition{raw: raw, rawVals: vals})
	return q
}

func (q *Query) Or(or *Query) *Query {
	q.ors = append(q.ors, or)
	return q
}

// wraps on top of Ors
func (q *Query) And(and *Query) *Query {
	q.ands = append(q.ands, and)
	return q
}

// pass strings like "field_name ASC"
func (q *Query) OrderBy(bys ...string) *Query {
	q.orderBys = append(q.orderBys, bys...)
	return q
}

// limit for select query, must be > 0
func (q *Query) Limit(i int) *Query {
	q.limit = i
	return q
}

// default returns
func (q *Query) Returning(cols ...string) *Query {
	q.returning = append(q.returning, cols...)
	return q
}

/*
Exec() executes the query either with db.Exec() (RowsAffected) or db.Query() (Rows) depending on the query

Select returns Rows

Insert returns the Rows which represents the id of the inserted record by default

Delete and Update return RowsAffected unless the Returning() function is called on the query, then it returns Rows
*/
func (q *Query) Exec(ctx context.Context) (*QueryResult, error) {
	var r QueryResult

	if q.client == nil || q.client.DB == nil {
		return &r, fmt.Errorf("client or db is nil")
	}

	switch q.action {
	case "select":
		rows, err := q.execSelect(ctx)
		r.Rows = rows
		return &r, err
	case "insert":
		rows, err := q.execInsert(ctx)
		r.Rows = rows
		return &r, err
	case "update":
		var err error
		if len(q.returning) == 0 {
			r.RowsAffected, err = q.execUpdate(ctx)
		} else {
			r.Rows, err = q.execUpdateR(ctx)
		}
		return &r, err
	case "delete":
		var err error
		if len(q.returning) == 0 {
			r.RowsAffected, err = q.execDelete(ctx)
		} else {
			r.Rows, err = q.execDeleteR(ctx)
		}
		return &r, err
	default:
		return &r, fmt.Errorf("unsupported action %v", q.action)
	}
}

func (q *Query) Slice(ctx context.Context, outSlicePtr interface{}) error {
	r, err := q.Exec(ctx)
	if err != nil {
		return err
	}

	return r.Slice(ctx, outSlicePtr)
}

func (q *Query) Scan(ctx context.Context, ptr interface{}) error {
	r, err := q.Exec(ctx)
	if err != nil {
		return err
	}

	return r.Scan(ctx, ptr)
}

// SELECT

func SelectQuery(c *Client, tableName string, cols ...string) *Query {
	return &Query{
		client:    c,
		action:    "select",
		tableName: tableName,
		columns:   cols,
	}
}

func (q *Query) execSelect(ctx context.Context) (*sql.Rows, error) {
	where, vals := q.whereClause(1)
	qs := selectQuery(q.tableName, q.columns, where, q.orderBys, q.limit)

	return q.client.QueryContext(ctx, qs, vals...)
}

// INSERT

func InsertQuery(c *Client, tableName string, attrs map[string]interface{}) *Query {
	return &Query{
		client:    c,
		action:    "insert",
		tableName: tableName,
		values:    attrs,
	}
}

func (q *Query) execInsert(ctx context.Context) (*sql.Rows, error) {
	if len(q.values) == 0 {
		return nil, errors.New("no values to insert")
	}

	cols, vals := keysValues(q.values)
	qs := insertQuery(q.tableName, cols, q.returning)

	return q.client.QueryContext(ctx, qs, vals...)
}

// UPDATE

func UpdateQuery(c *Client, tableName string, attrs map[string]interface{}) *Query {
	return &Query{
		client:    c,
		action:    "update",
		tableName: tableName,
		values:    attrs,
	}
}

func (q *Query) execUpdate(ctx context.Context) (int64, error) {
	cols, vals := keysValues(q.values)
	where, whereVals := q.whereClause(1)

	qs := updateQuery(q.tableName, cols, where, q.returning)

	vals = append(vals, whereVals...)
	result, err := q.client.ExecContext(ctx, qs, vals...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (q *Query) execUpdateR(ctx context.Context) (*sql.Rows, error) {
	cols, vals := keysValues(q.values)
	where, whereVals := q.whereClause(1)

	qs := updateQuery(q.tableName, cols, where, q.returning)

	vals = append(vals, whereVals...)

	return q.client.QueryContext(ctx, qs, vals...)
}

// DELETE

func DeleteQuery(c *Client, tableName string) *Query {
	return &Query{
		client:    c,
		action:    "delete",
		tableName: tableName,
	}
}

func (q *Query) execDelete(ctx context.Context) (int64, error) {
	where, vals := q.whereClause(1)

	qs := deleteQuery(q.tableName, where, q.returning)

	result, err := q.client.ExecContext(ctx, qs, vals...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (q *Query) execDeleteR(ctx context.Context) (*sql.Rows, error) {
	where, vals := q.whereClause(1)

	qs := deleteQuery(q.tableName, where, q.returning)

	return q.client.QueryContext(ctx, qs, vals...)
}

// Helpers

// i is the first $ number, startPos is the length of attributes in an update or insert query excluding where clause
func (q *Query) whereClause(i int) (string, []interface{}) {
	if i < 1 {
		// the numbering starts at $1 not $0
		i = 1
	}

	vals := make([]interface{}, 0, len(q.conditions))

	b := strings.Builder{}

	// get length of values since the numbering of the where clause starts after values
	startPos := len(q.values) + i

	clauses := make([]string, 0, len(q.conditions))
	for _, cond := range q.conditions {
		if cond.raw != "" {
			n := len(cond.rawVals)
			if n == 0 {
				clauses = append(clauses, cond.raw)
				continue
			}

			replacements := make([]interface{}, n)
			for j, str := range placeHolders(startPos, n) {
				replacements[j] = str
			}

			c := fmt.Sprintf(cond.raw, replacements...)
			clauses = append(clauses, c)

			vals = append(vals, cond.rawVals...)

			startPos += n
			continue
		}

		// Clause returns the clause string and the new position (start position + number of values)
		c, nextPos := cond.Clause(startPos)
		clauses = append(clauses, c)

		if startPos < nextPos {
			// if the next position is greater than the start then values need to be added
			if r, ok := cond.val.(Range); ok {
				vals = append(vals, r.Start, r.End)
			} else if err := verifyArray(reflect.TypeOf(cond.val)); err == nil {
				s := reflect.ValueOf(cond.val)
				n := s.Len()
				for j := 0; j < n; j++ {
					v := s.Index(j).Interface()
					vals = append(vals, v)
				}
			} else {
				vals = append(vals, cond.val)
			}
			startPos = nextPos
		}
	}

	b.WriteString(strings.Join(clauses, " AND "))

	where := b.String()

	// Ors
	if len(q.ors) > 0 {
		newWhere, newVals := addQueries(where, "OR", startPos, q.ors)
		vals = append(vals, newVals...)

		startPos += len(newVals)
		where = newWhere
	}

	// Ands
	if len(q.ands) > 0 {
		newWhere, newVals := addQueries(where, "AND", startPos, q.ands)
		vals = append(vals, newVals...)

		startPos += len(newVals) //nolint:ineffassign
		where = newWhere
	}

	return where, vals
}

func addQueries(where, separator string, startPos int, queries []*Query) (string, []interface{}) {
	var vals []interface{}
	n := len(queries)

	var clauses []string
	if where == "" {
		clauses = make([]string, 0, n)
	} else {
		clauses = make([]string, 1, n+1)
		clauses[0] = strings.Join([]string{"(", where, ")"}, "")
	}

	for _, q := range queries {
		addWhere, addVals := q.whereClause(startPos)
		if addWhere == "" {
			continue
		}

		addWhere = strings.Join([]string{"(", addWhere, ")"}, "")
		clauses = append(clauses, addWhere)
		vals = append(vals, addVals...)

		startPos += len(addVals)
	}

	padSep := strings.Join([]string{" ", separator, " "}, "")

	return strings.Join(clauses, padSep), vals
}

// Condition

type condition struct {
	col      string
	val      interface{}
	negative bool
	raw      string
	rawVals  []interface{}
}

// returns the clause and the input int after incrementing by the amount of placeholders ($1) created
func (c *condition) Clause(i int) (string, int) {
	var condClause string
	if nullable, ok := c.val.(Nullable); c.val == nil || (ok && nullable.IsNull()) {
		condClause = condClauseNull(c.negative)
	} else if _, ok := c.val.(Range); ok {
		condClause, i = condClauseRange(i, c.negative)
	} else if err := verifyArray(reflect.TypeOf(c.val)); err == nil {
		condClause, i = condClauseSlice(i, reflect.ValueOf(c.val).Len(), c.negative)
	} else {
		condClause, i = condClauseVal(i, c.negative)
	}

	var b StringsBuilder
	b.WriteStrings(Quote(c.col), " ", condClause)

	return b.String(), i
}

// Condition clause helpers

func condClauseNull(negative bool) string {
	if negative {
		return "IS NOT NULL"
	}

	return "IS NULL"
}

func condClauseRange(start int, negative bool) (string, int) {
	var op string
	if negative {
		op = "NOT BETWEEN"
	} else {
		op = "BETWEEN"
	}
	var b StringsBuilder
	b.WriteStrings(op, " $", strconv.Itoa(start), " AND $", strconv.Itoa(start+1))
	return b.String(), start + 2
}

func condClauseSlice(start int, size int, negative bool) (string, int) {
	var op string
	if negative {
		op = "NOT IN"
	} else {
		op = "IN"
	}
	valsStr := strings.Join(placeHolders(start, size), ", ")
	var b StringsBuilder
	b.WriteStrings(op, " (", valsStr, ")")
	return b.String(), start + size
}

func condClauseVal(start int, negative bool) (string, int) {
	var op string
	if negative {
		op = "!="
	} else {
		op = "="
	}
	var b StringsBuilder
	b.WriteStrings(op, " $", strconv.Itoa(start))
	return b.String(), start + 1
}

// QueryResult

type QueryResult struct {
	*sql.Rows
	RowsAffected int64
}

// Pass in a pointer to a slice to convert the rows into
func (r *QueryResult) Slice(ctx context.Context, slicePtr interface{}) error {
	if r.Rows == nil {
		return errors.New("result rows is nil")
	}

	defer r.Close()

	slicePtrType := reflect.TypeOf(slicePtr)

	// make sure this is a pointer
	if err := verifyPtr(slicePtrType); err != nil {
		return err
	}

	sliceType := slicePtrType.Elem()

	// make sure this is a slice
	if err := verifySlice(sliceType); err != nil {
		return err
	}

	sliceElemType := sliceType.Elem()
	outSliceVal := reflect.ValueOf(slicePtr).Elem()

	// determine what type of elements the slice holds
	var baseType reflect.Type
	if sliceElemType.Kind() == reflect.Ptr {
		baseType = sliceElemType.Elem()
	} else {
		baseType = sliceElemType
	}

	if scanAsStruct(sliceElemType) {
		return scanStructs(r.Rows, baseType, sliceElemType, outSliceVal)
	}

	return scanNatives(r.Rows, baseType, sliceElemType, outSliceVal)
}

// send in the pointer to scan a single value from a single row
func (r *QueryResult) Scan(ctx context.Context, ptr interface{}) error {
	if r.Rows == nil {
		return errors.New("result rows is nil")
	}

	defer r.Close()

	if !r.Rows.Next() {
		if r.Rows.Err() != nil {
			return r.Rows.Err()
		}

		return ErrNoRows
	}

	ptrType := reflect.TypeOf(ptr)

	// make sure this is a pointer
	if err := verifyPtr(ptrType); err != nil {
		return err
	}

	if !scanAsStruct(ptrType) {
		return r.Rows.Scan(ptr)
	}

	cols, err := r.Rows.Columns()
	if err != nil {
		return err
	}

	baseType := ptrType.Elem()
	fieldIdxs := indexes(baseType)

	v := reflect.ValueOf(ptr).Elem()

	var vals []interface{}
	if ptrType.Implements(modelInterfaceType) {
		vals = modelVals(v, fieldIdxs, cols)
	} else {
		vals = structVals(v, cols)
	}

	return r.Rows.Scan(vals...)
}
