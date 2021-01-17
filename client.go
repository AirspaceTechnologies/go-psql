package psql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
)

// Client is a helper type to easily connect to PostgreSQL database instances.
//
// It satisfies the `health.Metric` interface.
type Client struct {
	*sql.DB
	connStr string
}

func NewClient(cfg *Config) *Client {
	connStr := os.Getenv("DATABASE_URL")

	if connStr == "" && cfg != nil {
		connStr = cfg.connString()
	}

	return &Client{connStr: connStr}
}

func (c *Client) Start(driverName string) error {
	if driverName == "" {
		driverName = "postgres"
	}

	db, err := sql.Open(driverName, c.connStr)
	if err != nil {
		return fmt.Errorf("unable to open connection to postgres db: %w", err)
	}

	c.DB = db

	return nil
}

func (c *Client) Stop() error {
	return c.Close()
}

func (c *Client) Select(tableName string, cols ...string) *Query {
	return SelectQuery(c, tableName, cols...)
}

// returns id into the model
func (c *Client) Insert(ctx context.Context, v Model, cols ...string) error {
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

func (c *Client) Update(ctx context.Context, v Model, cols ...string) error {
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

func (c *Client) Delete(ctx context.Context, v Model) (int64, error) {
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

func (c *Client) Save(ctx context.Context, v Model) error {
	mh := &ModelHelper{v}
	id, err := mh.ID()
	if err != nil {
		return err
	}

	if id == 0 {
		return c.Insert(ctx, v)
	}

	return c.Update(ctx, v)
}

func (c *Client) UpdateAll(table string, attrs Attrs) *Query {
	return UpdateQuery(c, table, attrs)
}

func (c *Client) DeleteAll(table string) *Query {
	return DeleteQuery(c, table)
}

func (c *Client) RawSelect(ctx context.Context, outSlicePtr interface{}, q string, args ...interface{}) error {
	r, err := c.RawQuery(ctx, q, args...)
	if err != nil {
		return err
	}
	return r.Slice(ctx, outSlicePtr)
}

func (c *Client) RawQuery(ctx context.Context, q string, args ...interface{}) (*QueryResult, error) {
	var r QueryResult
	rows, err := c.QueryContext(ctx, q, args...)
	r.Rows = rows
	return &r, err
}

// Returning Queries
// The following scan all values back into the struct from the row

func (c *Client) InsertReturning(ctx context.Context, v Model, cols ...string) error {
	t := reflect.TypeOf(v)
	if err := verifyPtr(t); err != nil {
		return err
	}

	mh := &ModelHelper{v}

	return InsertQuery(c, v.TableName(), mh.Attributes(cols...)).Returning("*").Scan(ctx, v)
}

func (c *Client) UpdateReturning(ctx context.Context, v Model, cols ...string) error {
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

func (c *Client) DeleteReturning(ctx context.Context, v Model) error {
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

////////////////////////////////////////////////////////////////////////////////////////////////////

// health.Metric

// Name returns `"psql"`.
//
// To better identify the `Client` may be wrapped in a prefixed metric:
//
//     health.PrefixedMetric{Prefix: "MyDatabaseName", BaseMetric: c}
//
func (c *Client) Name() string {
	return "psql"
}

// Status calls the client's `Ping` method to determine connectivity.
func (c *Client) Status(ctx context.Context) error {
	return c.PingContext(ctx)
}
