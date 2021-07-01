package psql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
)

type QueryClient interface {
	Started() bool
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func Select(c QueryClient, tableName string, cols ...string) *Query {
	return SelectQuery(c, tableName, cols...)
}

// returns id into the model
func Insert(ctx context.Context, c QueryClient, v Model, cols ...string) error {
	t := reflect.TypeOf(v)
	if err := verifyPtr(t); err != nil {
		return err
	}

	mh := &ModelHelper{v}

	result, err := InsertQuery(c, v.TableName(), mh.Attributes(cols...)).Exec(ctx)
	if err != nil {
		return err
	}

	return result.Scan(ctx, v)
}

func Update(ctx context.Context, c QueryClient, v Model, cols ...string) error {
	mh := &ModelHelper{v}
	id, err := mh.ID()
	if err != nil {
		return err
	}
	if id == 0 {
		return errors.New("cannot update with id of 0")
	}

	_, err = UpdateQuery(c, v.TableName(), mh.Attributes(cols...)).Where(Attrs{"id": id}).Exec(ctx)

	return err
}

func Delete(ctx context.Context, c QueryClient, v Model) (int64, error) {
	mh := &ModelHelper{v}

	id, err := mh.ID()
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, errors.New("cannot delete with id of 0")
	}

	result, err := DeleteQuery(c, v.TableName()).Where(Attrs{"id": id}).Exec(ctx)

	return result.RowsAffected, err
}

func Save(ctx context.Context, c QueryClient, v Model, cols ...string) error {
	mh := &ModelHelper{v}
	id, err := mh.ID()
	if err != nil {
		return err
	}

	if id == 0 {
		return Insert(ctx, c, v, cols...)
	}

	return Update(ctx, c, v, cols...)
}

func UpdateAll(c QueryClient, table string, attrs Attrs) *Query {
	return UpdateQuery(c, table, attrs)
}

func DeleteAll(c QueryClient, table string) *Query {
	return DeleteQuery(c, table)
}

func RawSelect(ctx context.Context, c QueryClient, outSlicePtr interface{}, q string, args ...interface{}) error {
	r, err := RawQuery(ctx, c, q, args...)
	if err != nil {
		return err
	}
	return r.Slice(ctx, outSlicePtr)
}

func RawQuery(ctx context.Context, c QueryClient, q string, args ...interface{}) (*QueryResult, error) {
	var r QueryResult
	rows, err := c.QueryContext(ctx, q, args...)
	r.Rows = rows
	return &r, err
}

// Returning Queries
// The following scan all values back into the struct from the row

func InsertReturning(ctx context.Context, c QueryClient, v Model, cols ...string) error {
	t := reflect.TypeOf(v)
	if err := verifyPtr(t); err != nil {
		return err
	}

	mh := &ModelHelper{v}

	return InsertQuery(c, v.TableName(), mh.Attributes(cols...)).Returning("*").Scan(ctx, v)
}

func UpdateReturning(ctx context.Context, c QueryClient, v Model, cols ...string) error {
	t := reflect.TypeOf(v)
	if err := verifyPtr(t); err != nil {
		return err
	}

	mh := &ModelHelper{v}

	id, err := mh.ID()
	if err != nil {
		return err
	}

	if id == 0 {
		return errors.New("cannot update with id of 0")
	}

	q := UpdateQuery(c, v.TableName(), mh.Attributes(cols...)).Where(Attrs{"id": id})
	return q.Returning("*").Scan(ctx, v)
}

func DeleteReturning(ctx context.Context, c QueryClient, v Model) error {
	t := reflect.TypeOf(v)
	if err := verifyPtr(t); err != nil {
		return err
	}

	id, err := ModelHelper{v}.ID()
	if err != nil {
		return err
	}

	if id == 0 {
		return errors.New("cannot delete with id of 0")
	}

	return DeleteQuery(c, v.TableName()).Where(Attrs{"id": id}).Returning("*").Scan(ctx, v)
}
