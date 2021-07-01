package psql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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

func (c *Client) Started() bool {
	return c.DB != nil
}

func (c *Client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if c.DB == nil {
		return nil, errors.New("db is nil")
	}

	tx, err := c.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx}, nil
}

func (c *Client) RunInTransaction(ctx context.Context, f func(context.Context, *Tx) error, opts *sql.TxOptions) error {
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

func (c *Client) Select(tableName string, cols ...string) *Query {
	return Select(c, tableName, cols...)
}

func (c *Client) Insert(ctx context.Context, v Model, cols ...string) error {
	return Insert(ctx, c, v, cols...)
}

func (c *Client) Update(ctx context.Context, v Model, cols ...string) error {
	return Update(ctx, c, v, cols...)
}

func (c *Client) Delete(ctx context.Context, v Model) (int64, error) {
	return Delete(ctx, c, v)
}

func (c *Client) Save(ctx context.Context, v Model, cols ...string) error {
	return Save(ctx, c, v, cols...)
}

func (c *Client) UpdateAll(table string, attrs Attrs) *Query {
	return UpdateAll(c, table, attrs)
}

func (c *Client) DeleteAll(table string) *Query {
	return DeleteAll(c, table)
}

func (c *Client) RawSelect(ctx context.Context, outSlicePtr interface{}, q string, args ...interface{}) error {
	return RawSelect(ctx, c, outSlicePtr, q, args...)
}

func (c *Client) RawQuery(ctx context.Context, q string, args ...interface{}) (*QueryResult, error) {
	return RawQuery(ctx, c, q, args...)
}

// Returning Queries
// The following scan all values back into the struct from the row

func (c *Client) InsertReturning(ctx context.Context, v Model, cols ...string) error {
	return InsertReturning(ctx, c, v, cols...)
}

func (c *Client) UpdateReturning(ctx context.Context, v Model, cols ...string) error {
	return UpdateReturning(ctx, c, v, cols...)
}

func (c *Client) DeleteReturning(ctx context.Context, v Model) error {
	return DeleteReturning(ctx, c, v)
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
