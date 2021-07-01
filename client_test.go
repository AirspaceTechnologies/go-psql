package psql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClient_Test(t *testing.T) {
	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.Exec(modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.Exec("drop table mock_models")
		_ = c.Close()
	}()

	m := &MockModel{
		StringField:     "test",
		NullStringField: NewNullString("test"),
		IntField:        5,
		FloatField:      4.5,
		BoolField:       true,
		TimeField:       time.Now(),
	}

	ctx := context.Background()

	// Insert
	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	if m.ID < 1 {
		t.Fatalf("insert did not set id")
	}

	var results []*MockModel

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result := results[0]

	if result.ID != m.ID {
		t.Fatalf("ids do not match (%v and %v)", result.ID, m.ID)
	}

	if result.StringField != m.StringField ||
		result.IntField != m.IntField ||
		result.FloatField != m.FloatField ||
		result.BoolField != m.BoolField ||
		!result.NullStringField.Valid {
		t.Fatalf("result does not match expect (%v and %v)", result, m)
	}

	// Update

	m.BoolField = false
	m.IntField = 10
	m.NullStringField = InvalidNullString()

	if err := c.Update(ctx, m); err != nil {
		t.Fatalf("Error updating %v", err)
	}

	results = nil

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result = results[0]

	if result.BoolField {
		t.Fatalf("incorrect value for bool_field after update, expected false %v", result)
	}

	if result.IntField != 10 {
		t.Fatalf("incorrect value for int_field after update, expected 10 %v", result)
	}

	// Select with nullable
	results = nil

	if err := c.Select("mock_models").Where(Attrs{"null_string_field": NullString{}}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	// with nil

	results = nil

	if err := c.Select("mock_models").Where(Attrs{"null_string_field": nil}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	m.IntField = 15
	m.FloatField = 18.2

	// Update single column
	if err := c.Update(ctx, m, "float_field"); err != nil {
		t.Fatalf("Error updating %v", err)
	}

	results = nil

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result = results[0]

	if result.FloatField != 18.2 {
		t.Fatalf("incorrect value for float_field after update, expected 18.2 %v", result)
	}

	if result.IntField != 10 {
		t.Fatalf("incorrect value for int_field after update, expected 10 %v", result)
	}

	// Delete
	if i, err := c.Delete(ctx, m); err != nil {
		t.Fatalf("Error deleting (rows affected %v) %v", i, err)
	}

	results = nil

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("results returned incorrect amount, expected 0 and got %v", len(results))
	}

	// Multi insert
	m.BoolField = false

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("Error inserting %v", err)
	}

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("Error inserting %v", err)
	}

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("Error inserting %v", err)
	}

	results = nil

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("results returned incorrect amount, expected 3 and got %v", len(results))
	}

	// Select with clause
	results = nil

	if err := c.Select("mock_models").Where(Attrs{"id": m.ID}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	// RawSelect

	results = nil

	if err := c.RawSelect(ctx, &results, "SELECT * FROM mock_models WHERE id = $1", m.ID); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	// RawQuery

	if r, err := c.RawQuery(ctx, "SELECT count(*) FROM mock_models"); err != nil {
		t.Fatalf("Select failed %v", err)
	} else {
		var count int
		err := r.Scan(ctx, &count)
		if err != nil {
			t.Fatalf("failed to scan %v", err)
		}

		require.Equal(t, 3, count)
	}

	// UpdateAll with no constraint

	if r, err := c.UpdateAll(m.TableName(), Attrs{"bool_field": true}).Exec(ctx); err != nil || r.RowsAffected != 3 {
		t.Fatalf("update all failed with rows returned %v and error %v", r.RowsAffected, err)
	}

	// UpdateAll with constraint

	m.BoolField = false

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("Error inserting %v", err)
	}

	if r, err := c.UpdateAll(m.TableName(), Attrs{"bool_field": false}).Where(Attrs{"bool_field": true}).Exec(ctx); err != nil || r.RowsAffected != 3 {
		t.Fatalf("update all failed with rows returned %v and error %v", r.RowsAffected, err)
	}

	// DeleteAll with constraint
	if result, err := c.DeleteAll(m.TableName()).Where(Attrs{"id": m.ID}).Exec(ctx); result.RowsAffected != 1 || err != nil {
		t.Fatalf("Error deleting with consrtaint, rows affected %v, error %v", result.RowsAffected, err)
	}

	// DeleteAll with no constraint
	if result, err := c.DeleteAll(m.TableName()).Exec(ctx); result.RowsAffected != 3 || err != nil {
		t.Fatalf("Error deleting with consrtaint, rows affected %v, error %v", result.RowsAffected, err)
	}

	// Nullable ID

	mn := &MockModelNullableID{
		StringField:     "test",
		NullStringField: NewNullString("test"),
		IntField:        5,
		FloatField:      4.5,
		BoolField:       true,
		TimeField:       time.Now(),
	}

	if mn.ID.Valid {
		t.Fatalf("id valid prior to insert")
	}

	// Insert
	if err := c.Insert(ctx, mn); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	if !mn.ID.Valid {
		t.Fatalf("insert did not set id")
	}

	// Returning queries

	strField := "test string"

	m = &MockModel{StringField: strField}

	// InsertReturning

	insertCols := []string{"bool_field", "string_field", "int_field", "float_field", "time_field", "table"}

	if err := c.InsertReturning(ctx, m, insertCols...); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	id := m.ID
	if id < 1 {
		t.Fatalf("insert did not set id")
	}

	if m.CreatedAt.IsZero() {
		t.Fatalf("created at not returned")
	}

	// UpdateReturning

	m = &MockModel{ID: id, BoolField: true}

	if err := c.UpdateReturning(ctx, m, "bool_field"); err != nil {
		t.Fatalf("error updating %v", err)
	}

	require.Equal(t, true, m.BoolField)
	require.Equal(t, strField, m.StringField)

	// DeleteReturning

	m = &MockModel{ID: id}

	if err := c.DeleteReturning(ctx, m); err != nil {
		t.Fatalf("error deleting %v", err)
	}

	require.Equal(t, true, m.BoolField)
	require.Equal(t, strField, m.StringField)
}

type QuoteNeededModel struct {
	ID               int    `sql:"id"`
	QuoteNeededField string `sql:"table"` // use reserved word
}

func (*QuoteNeededModel) TableName() string {
	return "table" // use reserved word
}

func TestClient_Quoting(t *testing.T) {
	var createTable = `create table "table" (id bigserial primary key, "table" text)`

	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.Exec(createTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.Exec(`drop table "table"`)
		_ = c.Close()
	}()

	ctx := context.Background()

	m := &QuoteNeededModel{QuoteNeededField: "text"}

	// Insert
	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	if m.ID < 1 {
		t.Fatalf("insert did not set id")
	}

	var results []*QuoteNeededModel

	if err := c.Select(m.TableName()).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result := results[0]

	if result.ID != m.ID {
		t.Fatalf("ids do not match (%v and %v)", result.ID, m.ID)
	}

	require.Equal(t, m.QuoteNeededField, result.QuoteNeededField)

	// Update
	m.QuoteNeededField = "table"

	results = nil

	if err := c.Update(ctx, m); err != nil {
		t.Fatalf("Error updating %v", err)
	}

	if err := c.Select(m.TableName()).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result = results[0]

	require.Equal(t, m.QuoteNeededField, result.QuoteNeededField)

	// Delete
	if i, err := c.Delete(ctx, m); err != nil {
		t.Fatalf("Error deleting (rows affected %v) %v", i, err)
	}

	results = nil

	if err := c.Select(m.TableName()).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("results returned incorrect amount, expected 0 and got %v", len(results))
	}
}

func TestSliceModelProvider_NextModel(t *testing.T) {
	arr := make([]Model, 1)
	arr[0] = &MockModel{}
	p := NewSliceModelProvider(arr)

	if m := p.NextModel(); m == nil {
		t.Fatal("expected next model to not be nil")
	}

	if m := p.NextModel(); m != nil {
		t.Fatal("expected next model to be nil")
	}

	if m := p.NextModel(); m != nil {
		t.Fatal("expected next model to be nil")
	}
}

func TestChannelModelProvider_NextModel(t *testing.T) {
	ch := make(chan Model, 1)
	ch <- &MockModel{}
	m := &MockModel{}
	p := NewChannelModelProvider(m, ch)

	if m := p.NextModel(); m == nil {
		t.Fatal("expected next model to not be nil")
	}

	if m := p.NextModel(); m == nil {
		t.Fatal("expected next model to not be nil")
	}

	if m := p.NextModel(); m != nil {
		t.Fatal("expected next model to be nil")
	}

	if m := p.NextModel(); m != nil {
		t.Fatal("expected next model to be nil")
	}

	close(ch)

	if m := p.NextModel(); m != nil {
		t.Fatal("expected next model to be nil")
	}
}

func TestClient_BulkInsert(t *testing.T) {
	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.Exec(modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.Exec("drop table mock_models")
		_ = c.Close()
	}()

	models := []Model{
		&MockModel{
			StringField:     "test1",
			NullStringField: NewNullString("test"),
			IntField:        1,
			FloatField:      4.5,
			BoolField:       true,
			TimeField:       time.Now(),
			Table:           "table",
		},
		&MockModel{
			StringField:     "test2",
			NullStringField: NewNullString("test"),
			IntField:        2,
			FloatField:      5.6,
			BoolField:       false,
			TimeField:       time.Now(),
		},
		&MockModel{
			StringField:     "test3",
			NullStringField: NewNullString("test"),
			IntField:        3,
			FloatField:      6.7,
			BoolField:       true,
			TimeField:       time.Now(),
		},
	}

	ctx := context.Background()

	// Slice Model Provider
	if err := c.BulkInsert(NewSliceModelProvider(models)); err != nil {
		t.Fatalf("bulk insert failed, %v", err)
	}

	results := make([]*MockModel, 0)

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != len(models) {
		t.Fatalf("results returned incorrect amount, expected %v and got %v", len(models), len(results))
	}

	// Clear data for next test
	if r, err := c.DeleteAll("mock_models").Exec(ctx); err != nil || r.RowsAffected != int64(len(models)) {
		t.Fatalf("error deleting %v rows affected, error: %v", r.RowsAffected, err)
	}

	// Channel Model Provider
	n := len(models) - 1
	ch := make(chan Model, n)
	m := models[n]
	for i := 0; i < n; i++ {
		ch <- models[i]
	}

	if err := c.BulkInsert(NewChannelModelProvider(m, ch)); err != nil {
		t.Fatalf("bulk insert failed, %v", err)
	}

	results = nil

	if err := c.Select("mock_models").OrderBy("string_field asc").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != len(models) {
		t.Fatalf("results returned incorrect amount, expected %v and got %v", len(models), len(results))
	}

	for i, result := range results {
		require.Equal(t, fmt.Sprintf("test%v", i+1), result.StringField)
		require.Equal(t, NewNullString("test"), result.NullStringField)
	}

	// Clear data for next test
	if r, err := c.DeleteAll("mock_models").Exec(ctx); err != nil || r.RowsAffected != int64(len(models)) {
		t.Fatalf("error deleting %v rows affected, error: %v", r.RowsAffected, err)
	}

	// Channel Model Provider Cap()
	n = len(models) - 1
	ch = make(chan Model, n-1)
	m = models[n]
	go func() {
		for i := 0; i < n; i++ {
			ch <- models[i]
		}
	}()

	if err := c.BulkInsert(NewChannelModelProvider(m, ch)); err != nil {
		t.Fatalf("bulk insert failed, %v", err)
	}

	results = nil

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != len(models)-1 {
		t.Fatalf("results returned incorrect amount, expected %v and got %v", len(models)-1, len(results))
	}
}

func TestClient_EmbeddedModelSlice(t *testing.T) {
	type EmbeddedMockModel struct {
		StringField string `sql:"string_field"` // will not override embedded
		MockModel
		FloatField float64 `sql:"float_field"` // will override embedded
	}

	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.Exec(modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.Exec("drop table mock_models")
		_ = c.Close()
	}()

	m := &MockModel{
		StringField:     "test",
		NullStringField: NewNullString("test"),
		IntField:        5,
		FloatField:      4.5,
		BoolField:       true,
		TimeField:       time.Now(),
	}

	ctx := context.Background()

	// Insert
	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	if m.ID < 1 {
		t.Fatalf("insert did not set id")
	}

	var results []*EmbeddedMockModel

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result := results[0]

	if result.ID != m.ID {
		t.Fatalf("ids do not match (%v and %v)", result.ID, m.ID)
	}

	if result.MockModel.StringField != m.StringField ||
		result.StringField != "" || // overridden in embedded struct
		result.IntField != m.IntField ||
		result.MockModel.IntField != m.IntField ||
		result.FloatField != m.FloatField ||
		result.MockModel.FloatField != 0 || // overridden in main struct
		result.BoolField != m.BoolField ||
		!result.NullStringField.Valid {
		t.Fatalf("result does not match expect (%v and %v)", result, m)
	}

	result = &EmbeddedMockModel{}
	if err := c.Select("mock_models").Scan(ctx, result); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if result.ID != m.ID {
		t.Fatalf("ids do not match (%v and %v)", result.ID, m.ID)
	}

	if result.MockModel.StringField != m.StringField ||
		result.StringField != "" || // overridden in embedded struct
		result.IntField != m.IntField ||
		result.MockModel.IntField != m.IntField ||
		result.FloatField != m.FloatField ||
		result.MockModel.FloatField != 0 || // overridden in main struct
		result.BoolField != m.BoolField ||
		!result.NullStringField.Valid {
		t.Fatalf("result does not match expect (%v and %v)", result, m)
	}
}

func TestClient_EmbeddedModelInsert(t *testing.T) {
	type EmbeddedMockModel struct {
		StringField string `sql:"string_field"` // will not override embedded
		MockModel
		FloatField float64 `sql:"float_field"` // will override embedded
	}

	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.Exec(modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.Exec("drop table mock_models")
		_ = c.Close()
	}()

	m := &EmbeddedMockModel{
		StringField: "will be overridden",
		MockModel: MockModel{
			StringField:     "test",
			NullStringField: NewNullString("test"),
			IntField:        5,
			FloatField:      4.5,
			BoolField:       true,
			TimeField:       time.Now(),
		},
		FloatField: 5.5,
	}

	ctx := context.Background()

	// Insert
	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	if m.ID < 1 {
		t.Fatalf("insert did not set id")
	}

	var results []*MockModel

	if err := c.Select("mock_models").Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results returned incorrect amount, expected 1 and got %v", len(results))
	}

	result := results[0]

	if result.ID != m.ID {
		t.Fatalf("ids do not match (%v and %v)", result.ID, m.ID)
	}

	if result.StringField != m.MockModel.StringField ||
		result.IntField != m.IntField ||
		result.FloatField != m.FloatField ||
		result.BoolField != m.BoolField ||
		!result.NullStringField.Valid {
		t.Fatalf("result does not match expect (%v and %v)", result, m)
	}
}

func TestClient_RunInTransaction(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"no error returned": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					err := tx.Update(ctx, MockModel{ID: 1, IntField: 20}, "int_field")
					require.Nil(t, err)

					err = tx.Update(ctx, MockModel{ID: 1, IntField: 6}, "int_field")
					require.Nil(t, err)

					return nil
				}, nil)
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{6, 10})
			},
		},
		"panic": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()
				defer func() {
					require.NotNil(t, recover())

					var results []int
					q := c.Select(MockModel{}.TableName(), "int_field")
					err := q.OrderBy("int_field asc").Slice(ctx, &results)
					require.Nil(t, err)
					require.ElementsMatch(t, results, []int{4, 10})
				}()

				c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					err := tx.Update(ctx, MockModel{ID: 1, IntField: 20}, "int_field")
					require.Nil(t, err)

					err = tx.Update(ctx, MockModel{ID: 1, IntField: 6}, "int_field")
					require.Nil(t, err)

					panic(errors.New("error for use in panic"))
				}, nil)
			},
		},
		"error returned": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					err := tx.Update(ctx, MockModel{ID: 1, IntField: 20}, "int_field")
					require.Nil(t, err)

					err = tx.Update(ctx, MockModel{ID: 1, IntField: 6}, "int_field")
					require.Nil(t, err)

					return errors.New("some error returned")
				}, nil)
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

// test individual methods

func TestClient_Select(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"select values": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err := q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
		"select models": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				var results []MockModel
				q := c.Select(MockModel{}.TableName())
				err := q.OrderBy("id asc").Slice(ctx, &results)
				require.Nil(t, err)

				var ids []int
				var ints []int
				for _, r := range results {
					ids = append(ids, r.ID)
					ints = append(ints, r.IntField)
				}
				require.ElementsMatch(t, ids, []int{1, 2})
				require.ElementsMatch(t, ints, []int{4, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_Insert(t *testing.T) {
	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"insert 2 models": {
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				models := []*MockModel{
					{IntField: 4},
					{IntField: 10},
				}
				for _, m := range models {
					err := c.Insert(ctx, m)
					require.Nil(t, err)
				}

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err := q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_Update(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"update a model": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				err := c.Update(ctx, MockModel{ID: 1, IntField: 20}, "int_field")
				require.Nil(t, err)

				err = c.Update(ctx, MockModel{ID: 1, IntField: 6}, "int_field")
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{6, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_Delete(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"delete a model": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				i, err := c.Delete(ctx, MockModel{ID: 2})
				require.Nil(t, err)
				require.Equal(t, int64(1), i)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_Save(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"update a model": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				err := c.Save(ctx, MockModel{ID: 1, IntField: 20}, "int_field")
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{10, 20})
			},
		},
		"insert a model": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				err := c.Save(ctx, &MockModel{IntField: 20}, "int_field")
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10, 20})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_UpdateAll(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 5},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"update models": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				q := c.UpdateAll(MockModel{}.TableName(), Attrs{"int_field": -1})
				r, err := q.WhereRaw("int_field < %v", 10).Exec(ctx)
				require.Nil(t, err)
				require.Equal(t, int64(2), r.RowsAffected)

				var results []int
				q = c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{-1, -1, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_DeleteAll(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 5},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"update models": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				q := c.DeleteAll(MockModel{}.TableName())
				r, err := q.WhereRaw("int_field < %v", 10).Exec(ctx)
				require.Nil(t, err)
				require.Equal(t, int64(2), r.RowsAffected)

				var results []int
				q = c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_RawSelect(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"select values": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				var results []int
				err := c.RawSelect(ctx, &results, "select int_field from mock_models where int_field < $1", 10)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4})

				results = nil
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
		"select models": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					var results []MockModel
					err := tx.RawSelect(ctx, &results, "select * from mock_models where int_field < $1", 10)
					require.Nil(t, err)

					var ids []int
					var ints []int
					for _, r := range results {
						ids = append(ids, r.ID)
						ints = append(ints, r.IntField)
					}
					require.ElementsMatch(t, ids, []int{1})
					require.ElementsMatch(t, ints, []int{4})
					return nil
				}, nil)
				require.Nil(t, err)

				var results []MockModel
				q := c.Select(MockModel{}.TableName())
				err = q.OrderBy("id asc").Slice(ctx, &results)
				require.Nil(t, err)

				var ids []int
				var ints []int
				for _, r := range results {
					ids = append(ids, r.ID)
					ints = append(ints, r.IntField)
				}
				require.ElementsMatch(t, ids, []int{1, 2})
				require.ElementsMatch(t, ints, []int{4, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_RawQuery(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"update models": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				r, err := c.RawQuery(ctx, "update mock_models set int_field = $1 where int_field < $1 returning id", 10)
				require.Nil(t, err)

				var results []int
				err = r.Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, []int{1}, results)

				results = nil
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{10, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_InsertReturning(t *testing.T) {
	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"insert 2 models": {
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				models := []*MockModel{
					{IntField: 4},
					{IntField: 10},
				}
				for _, m := range models {
					err := c.InsertReturning(ctx, m)
					require.Nil(t, err)
					require.Greater(t, m.ID, 0)
					require.Greater(t, m.IntField, 0)
				}

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err := q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_UpdateReturning(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"update a model": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				m := &MockModel{ID: 1, IntField: 20}
				err := c.UpdateReturning(ctx, m, "int_field")
				require.Nil(t, err)

				r, err := c.UpdateAll(m.TableName(), Attrs{"float_field": 4.6}).Exec(ctx)
				require.Nil(t, err)
				require.Equal(t, int64(2), r.RowsAffected)

				m.IntField = 6
				err = c.UpdateReturning(ctx, m, "int_field")
				require.Nil(t, err)
				require.Equal(t, 6, m.IntField)
				require.Equal(t, 4.6, m.FloatField)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{6, 10})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestClient_DeleteReturning(t *testing.T) {
	before := func(t *testing.T, c *Client) {
		ctx := context.Background()
		models := []*MockModel{
			{IntField: 4},
			{IntField: 10},
		}
		for _, m := range models {
			err := c.Insert(ctx, m)
			require.Nil(t, err)
		}
	}

	tcs := map[string]struct {
		Before func(*testing.T, *Client)
		Run    func(*testing.T, *Client)
	}{
		"delete a model": {
			Before: before,
			Run: func(t *testing.T, c *Client) {
				ctx := context.Background()

				m := &MockModel{ID: 2}
				err := c.DeleteReturning(ctx, m)
				require.Nil(t, err)
				require.Equal(t, 10, m.IntField)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.Exec(modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.Exec("drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}
