package psql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

type Client interface {
	QueryClient

	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
	RunInTransaction(ctx context.Context, f func(context.Context, *Tx) error, opts *sql.TxOptions) error

	Select(tableName string, cols ...string) *Query
	Insert(ctx context.Context, v Model, cols ...string) error
	Update(ctx context.Context, v Model, cols ...string) error
	Delete(ctx context.Context, v Model) (int64, error)
	Save(ctx context.Context, v Model, cols ...string) error

	UpdateAll(table string, attrs Attrs) *Query
	DeleteAll(table string) *Query

	RawSelect(ctx context.Context, outSlicePtr interface{}, q string, args ...interface{}) error
	RawQuery(ctx context.Context, q string, args ...interface{}) (*QueryResult, error)

	InsertReturning(ctx context.Context, v Model, cols ...string) error
	UpdateReturning(ctx context.Context, v Model, cols ...string) error
	DeleteReturning(ctx context.Context, v Model) error

	Start(driverName string) error
	Stop() error
	Close() error

	BulkInserter() BulkInserter
	BulkInsert(ctx context.Context, p BulkProvider) error
	MonitorBulkInsertChannel(ctx context.Context, ch chan Model, errFunc ModelErrorFunc) error

	Name() string
	Status(ctx context.Context) error
}

// client is a helper type to easily connect to PostgreSQL database instances.
//
// It satisfies the `health.Metric` interface.
type client struct {
	*sql.DB
	connStr string
}

func NewClient(cfg *Config) Client {
	connStr := os.Getenv("DATABASE_URL")

	if connStr == "" && cfg != nil {
		connStr = cfg.connString()
	}

	return &client{connStr: connStr}
}

func (c *client) Start(driverName string) error {
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

func (c *client) Stop() error {
	return c.Close()
}

func (c *client) Started() bool {
	return c.DB != nil
}

func (c *client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if c.DB == nil {
		return nil, errors.New("db is nil")
	}

	tx, err := c.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx}, nil
}

func (c *client) RunInTransaction(ctx context.Context, f func(context.Context, *Tx) error, opts *sql.TxOptions) error {
	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	var ranFunc bool
	defer func() {
		if !ranFunc {
			// panicked in f still need to rollback transaction
			tx.Rollback()
		}
	}()

	err = f(ctx, tx)
	ranFunc = true
	if err != nil {
		return tx.Rollback()
	}

	return tx.Commit()
}

func (c *client) Select(tableName string, cols ...string) *Query {
	return Select(c, tableName, cols...)
}

func (c *client) Insert(ctx context.Context, v Model, cols ...string) error {
	return Insert(ctx, c, v, cols...)
}

func (c *client) Update(ctx context.Context, v Model, cols ...string) error {
	return Update(ctx, c, v, cols...)
}

func (c *client) Delete(ctx context.Context, v Model) (int64, error) {
	return Delete(ctx, c, v)
}

func (c *client) Save(ctx context.Context, v Model, cols ...string) error {
	return Save(ctx, c, v, cols...)
}

func (c *client) UpdateAll(table string, attrs Attrs) *Query {
	return UpdateAll(c, table, attrs)
}

func (c *client) DeleteAll(table string) *Query {
	return DeleteAll(c, table)
}

func (c *client) RawSelect(ctx context.Context, outSlicePtr interface{}, q string, args ...interface{}) error {
	return RawSelect(ctx, c, outSlicePtr, q, args...)
}

func (c *client) RawQuery(ctx context.Context, q string, args ...interface{}) (*QueryResult, error) {
	return RawQuery(ctx, c, q, args...)
}

// Returning Queries
// The following scan all values back into the struct from the row

func (c *client) InsertReturning(ctx context.Context, v Model, cols ...string) error {
	return InsertReturning(ctx, c, v, cols...)
}

func (c *client) UpdateReturning(ctx context.Context, v Model, cols ...string) error {
	return UpdateReturning(ctx, c, v, cols...)
}

func (c *client) DeleteReturning(ctx context.Context, v Model) error {
	return DeleteReturning(ctx, c, v)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// health.Metric

// Name returns `"psql"`.
//
// To better identify the `client` may be wrapped in a prefixed metric:
//
//     health.PrefixedMetric{Prefix: "MyDatabaseName", BaseMetric: c}
//
func (c *client) Name() string {
	return "psql"
}

// Status calls the client's `Ping` method to determine connectivity.
func (c *client) Status(ctx context.Context) error {
	return c.PingContext(ctx)
}
