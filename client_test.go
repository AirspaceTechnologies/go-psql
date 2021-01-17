// +build albatross

package psql

import (
	"context"
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
		c.Exec("drop table mock_models")
		c.Close()
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
		c.Exec(`drop table "table"`)
		c.Close()
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

	close(p.Channel)

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
		c.Exec("drop table mock_models")
		c.Close()
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
