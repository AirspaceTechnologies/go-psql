package psql

import (
	"context"
	"database/sql"
)

type Tx struct {
	*sql.Tx
}

func (tx *Tx) Started() bool {
	return tx.Tx != nil
}

func (tx *Tx) Select(tableName string, cols ...string) *Query {
	return Select(tx, tableName, cols...)
}

func (tx *Tx) Insert(ctx context.Context, v Model, cols ...string) error {
	return Insert(ctx, tx, v, cols...)
}

func (tx *Tx) Update(ctx context.Context, v Model, cols ...string) error {
	return Update(ctx, tx, v, cols...)
}

func (tx *Tx) Delete(ctx context.Context, v Model) (int64, error) {
	return Delete(ctx, tx, v)
}

func (tx *Tx) Save(ctx context.Context, v Model, cols ...string) error {
	return Save(ctx, tx, v, cols...)
}

func (tx *Tx) UpdateAll(table string, attrs Attrs) *Query {
	return UpdateAll(tx, table, attrs)
}

func (tx *Tx) DeleteAll(table string) *Query {
	return DeleteAll(tx, table)
}

func (tx *Tx) RawSelect(ctx context.Context, outSlicePtr interface{}, q string, args ...interface{}) error {
	return RawSelect(ctx, tx, outSlicePtr, q, args...)
}

func (tx *Tx) RawQuery(ctx context.Context, q string, args ...interface{}) (*QueryResult, error) {
	return RawQuery(ctx, tx, q, args...)
}

// Returning Queries
// The following scan all values back into the struct from the row

func (tx *Tx) InsertReturning(ctx context.Context, v Model, cols ...string) error {
	return InsertReturning(ctx, tx, v, cols...)
}

func (tx *Tx) UpdateReturning(ctx context.Context, v Model, cols ...string) error {
	return UpdateReturning(ctx, tx, v, cols...)
}

func (tx *Tx) DeleteReturning(ctx context.Context, v Model) error {
	return DeleteReturning(ctx, tx, v)
}
