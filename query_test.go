package psql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQuery_Test(t *testing.T) {
	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.ExecContext(context.Background(), modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.ExecContext(context.Background(), "drop table mock_models")
		_ = c.Close()
	}()

	tableName := "mock_models"

	ctx := context.Background()

	//// Insert

	var models []*MockModel

	if err := InsertQuery(c, tableName, Attrs{"float_field": 1}).Slice(ctx, &models); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(models) != 1 || models[0].ID == 0 {
		t.Fatalf("insert failed to scan")
	}

	if _, err := InsertQuery(c, tableName, Attrs{"int_field": 3}).Exec(ctx); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if _, err := InsertQuery(c, tableName, Attrs{"float_field": 10.2}).Exec(ctx); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	//// Select

	models = nil

	if err := SelectQuery(c, tableName, "id").Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 3 {
		t.Fatalf("wrong number selected, expected 3 and got %v", len(models))
	}

	//// limit

	models = nil

	if err := SelectQuery(c, tableName, "id").Limit(1).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	//// or

	models = nil

	if err := SelectQuery(c, tableName, "id").Where(Attrs{"float_field": 10.2}).Or(SubQuery().Where(Attrs{"float_field": nil})).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 2 {
		t.Fatalf("wrong number selected, expected 2 and got %v", len(models))
	}

	// without main query

	models = nil

	if err := SelectQuery(c, tableName, "id").Or(SubQuery().Where(Attrs{"float_field": 10.2})).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	require.Equal(t, 1, len(models))

	//// not

	models = nil

	if err := SelectQuery(c, tableName, "id", "float_field").WhereNot(Attrs{"float_field": nil}).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 2 {
		t.Fatalf("wrong number selected, expected 2 and got %v", len(models))
	}

	//// raw where

	models = nil

	if err := SelectQuery(c, tableName, "id").WhereRaw("int_field IS DISTINCT FROM %v", 0).WhereRaw("null_string_field IS NULL").WhereNot(Attrs{"id": nil}).WhereRaw("float_field < %v", 10).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	//// range

	models = nil

	if err := SelectQuery(c, tableName, "id", "float_field").WhereNot(Attrs{"float_field": nil}).OrderBy("float_field DESC").Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 2 {
		t.Fatalf("wrong number selected, expected 2 and got %v", len(models))
	}

	if models[0].FloatField != 10.2 {
		t.Fatalf("float field value incorrect %v", models[0].FloatField)
	}

	//// order

	models = nil

	if err := SelectQuery(c, tableName, "id").Where(Attrs{"float_field": Range{1, 10}}).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	//// Update

	if r, err := UpdateQuery(c, tableName, Attrs{"string_field": "test"}).Exec(ctx); err != nil || r.RowsAffected != 3 {
		t.Fatalf("error updating with rows affected %v and error %v", r.RowsAffected, err)
	}

	models = nil

	if err := SelectQuery(c, tableName, "id").Where(Attrs{"string_field": "test"}).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 3 {
		t.Fatalf("wrong number selected, expected 3 and got %v", len(models))
	}

	if r, err := UpdateQuery(c, tableName, Attrs{"string_field": nil}).Where(Attrs{"float_field": 10.2}).Exec(ctx); err != nil || r.RowsAffected != 1 {
		t.Fatalf("error updating with rows affected %v and error %v", r.RowsAffected, err)
	}

	// with where

	models = nil

	if err := SelectQuery(c, tableName, "id").Where(Attrs{"string_field": "test"}).Slice(ctx, &models); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	if len(models) != 2 {
		t.Fatalf("wrong number selected, expected 2 and got %v", len(models))
	}

	//// Delete

	if r, err := DeleteQuery(c, tableName).Exec(ctx); err != nil || r.RowsAffected != 3 {
		t.Fatalf("delete failed with rows affected %v and error %v", r.RowsAffected, err)
	}

	if _, err := InsertQuery(c, tableName, Attrs{"int_field": 3}).Exec(ctx); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if _, err := InsertQuery(c, tableName, Attrs{"float_field": 10.2}).Exec(ctx); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	// with where

	if r, err := DeleteQuery(c, tableName).Where(Attrs{"float_field": 10.2}).Exec(ctx); err != nil || r.RowsAffected != 1 {
		t.Fatalf("delete failed with rows affected %v and error %v", r.RowsAffected, err)
	}

	if r, err := DeleteQuery(c, tableName).Exec(ctx); err != nil || r.RowsAffected != 1 {
		t.Fatalf("delete failed with rows affected %v and error %v", r.RowsAffected, err)
	}

	//// Insert Returning

	// primitive slice
	var ints []int

	// ids returned by default
	if err := InsertQuery(c, tableName, Attrs{"int_field": 2}).Slice(ctx, &ints); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(ints) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	if ints[0] == 0 {
		t.Fatalf("id not set")
	}

	// when primitive column specified specified
	ints = nil

	if err := InsertQuery(c, tableName, Attrs{"int_field": 2}).Returning("int_field").Slice(ctx, &ints); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(ints) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	if ints[0] != 2 {
		t.Fatalf("did not return int_field")
	}

	// non primitive slice

	// anonymous struct
	var intHs []struct {
		Int    int
		String string
	}

	attrs := Attrs{"int_field": 2, "string_field": "test_string"}
	if err := InsertQuery(c, tableName, attrs).Returning("int_field", "string_field").Slice(ctx, &intHs); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(intHs) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	if intHs[0].Int != attrs["int_field"] {
		t.Fatalf("did not return int_field")
	}

	if intHs[0].String != attrs["string_field"] {
		t.Fatalf("did not return string_field")
	}

	// jsonObj
	var jsonObjs []JSONObject

	if err := InsertQuery(c, tableName, Attrs{"json_object": JSONObject{"t": 1}}).Returning("json_object").Slice(ctx, &jsonObjs); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(jsonObjs) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	if i, _ := jsonObjs[0].Int("t"); i != 1 {
		t.Fatalf("did not return correct json object")
	}

	// struct slice
	models = nil

	if err := InsertQuery(c, tableName, Attrs{"float_field": 1}).Returning("id", "created_at").Slice(ctx, &models); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(models) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	if models[0].ID == 0 {
		t.Fatalf("id not set")
	}

	if models[0].CreatedAt.IsZero() {
		t.Fatalf("created at not set")
	}

	require.Equal(t, float64(0), models[0].FloatField)

	// returning *

	models = nil

	m := &MockModel{
		FloatField:      2,
		CreatedAt:       time.Now().UTC(),
		NullStringField: NewNullString("test"),
	}

	if err := InsertQuery(c, tableName, ModelHelper{Model: m}.Attributes()).Returning("*").Slice(ctx, &models); err != nil {
		t.Fatalf("insert failed with error %v", err)
	}

	if len(models) != 1 {
		t.Fatalf("wrong number selected, expected 1 and got %v", len(models))
	}

	if models[0].ID == 0 {
		t.Fatalf("id not set")
	}

	if models[0].CreatedAt.IsZero() {
		t.Fatalf("created at not set")
	}

	require.Equal(t, float64(2), models[0].FloatField)

	// update

	models = nil

	attrs = Attrs{"null_string_field": "field", "time_field": nil}
	if err := UpdateQuery(c, tableName, attrs).WhereNot(Attrs{"null_string_field": nil}).Returning("id").Slice(ctx, &models); err != nil {
		t.Fatalf("update failed with error %v", err)
	}

	require.Equal(t, 1, len(models))

	if models[0].ID == 0 {
		t.Fatalf("id not set")
	}

	// updating using slice in where
	m = models[0]
	models = nil

	attrs = Attrs{"time_field": time.Now().UTC()}
	if err := UpdateQuery(c, tableName, attrs).Where(Attrs{"id": []int{m.ID}}).Returning("*").Slice(ctx, &models); err != nil {
		t.Fatalf("update failed with error %v", err)
	}

	// updating using array in where
	models = nil

	attrs = Attrs{"time_field": time.Now().UTC()}
	if err := UpdateQuery(c, tableName, attrs).Where(Attrs{"id": [1]int{m.ID}}).Returning("*").Slice(ctx, &models); err != nil {
		t.Fatalf("update failed with error %v", err)
	}

	//// Scan

	// model
	m = &MockModel{}

	if err := SelectQuery(c, tableName).Where(Attrs{"id": models[0].ID}).Scan(ctx, m); err != nil {
		t.Fatalf("select failed with error %v", err)
	}

	require.EqualValues(t, models[0], m)

	// anonymous struct
	var s struct {
		Int    int
		String string
		Unused bool
	}

	attrs = Attrs{"int_field": 2, "string_field": "test_string"}
	if err := InsertQuery(c, tableName, attrs).Returning("int_field", "string_field").Scan(ctx, &s); err != nil {
		t.Fatalf("insert and failed with error %v", err)
	}

	if s.Int != attrs["int_field"] {
		t.Fatalf("did not return int_field")
	}

	if s.String != attrs["string_field"] {
		t.Fatalf("did not return string_field")
	}

	// struct (time)
	var createdAt time.Time
	if err := SelectQuery(c, tableName, "created_at").Where(Attrs{"id": m.ID}).Scan(ctx, &createdAt); err != nil {
		t.Fatalf("select and scan failed with error %v", err)
	}

	require.Equal(t, false, createdAt.IsZero())
	require.Equal(t, m.CreatedAt, createdAt)

	// float
	var f float64
	if err := SelectQuery(c, tableName, "float_field").Where(Attrs{"id": m.ID}).Scan(ctx, &f); err != nil {
		t.Fatalf("select scan failed with error %v", err)
	}

	require.Equal(t, m.FloatField, f)

	// delete
	var str string
	if err := DeleteQuery(c, tableName).Where(Attrs{"id": m.ID}).WhereNot(Attrs{"null_string_field": nil}).Returning("null_string_field").Scan(ctx, &str); err != nil {
		t.Fatalf("delete scan failed with error %v", err)
	}

	require.Equal(t, "field", str)
}

func TestQuery_And_Or(t *testing.T) {
	c := NewClient(nil)

	if err := c.Start(""); err != nil {
		t.Fatalf("Failed to start %v", err)
	}

	if _, err := c.ExecContext(context.Background(), modelsTable); err != nil {
		t.Fatalf("failed to create table %v", err)
	}

	defer func() {
		_, _ = c.ExecContext(context.Background(), "drop table mock_models")
		_ = c.Close()
	}()

	seeds := []*MockModel{
		{
			FloatField:  1,
			StringField: "1",
		},
		{
			FloatField:      2,
			NullStringField: NewNullString("2"),
		},
		{
			FloatField: 2,
		},
		{
			IntField: 3,
		},
	}

	ctx := context.Background()

	for i, m := range seeds {
		err := c.Insert(ctx, m)
		if err != nil {
			t.Fatalf("failed to insert %v %v", i, err)
		}
	}

	tableName := "mock_models"

	// select

	var models []*MockModel

	q := c.Select(tableName, "id").Where(Attrs{"float_field": 1})
	q.Or(SubQuery().Where(Attrs{"float_field": 2}))
	q.Or(SubQuery().WhereRaw("int_field > %v", 0))
	q.And(SubQuery().Where(Attrs{"null_string_field": nil}))
	q.OrderBy("id ASC")

	if err := q.Slice(ctx, &models); err != nil {
		t.Fatalf("failed to select %v", err)
	}

	require.Equal(t, 3, len(models))
	require.Equal(t, seeds[0].ID, models[0].ID)
	require.Equal(t, seeds[2].ID, models[1].ID)
	require.Equal(t, seeds[3].ID, models[2].ID)

	// and without query
	models = nil

	q = c.Select(tableName, "id")
	q.And(SubQuery().Where(Attrs{"string_field": "1"}))

	if err := q.Slice(ctx, &models); err != nil {
		t.Fatalf("failed to select %v", err)
	}

	require.Equal(t, 1, len(models))
	require.Equal(t, seeds[0].ID, models[0].ID)

	// update

	q = c.UpdateAll(tableName, Attrs{"int_field": 3})
	q.Where(Attrs{"float_field": 1})
	q.Or(SubQuery().Where(Attrs{"float_field": 2}))
	q.And(SubQuery().WhereNot(Attrs{"null_string_field": nil}))

	if result, err := q.Exec(ctx); err != nil {
		t.Fatalf("failed to update %v", err)
	} else {
		require.Equal(t, int64(1), result.RowsAffected)
	}

	// delete

	q = c.DeleteAll(tableName)
	q.Where(Attrs{"float_field": 1, "string_field": "1"})
	q.Or(SubQuery().WhereNot(Attrs{"null_string_field": nil}))
	q.And(SubQuery().WhereRaw("int_field > %v", 2))

	var id int
	if err := q.Returning("id").Scan(ctx, &id); err != nil {
		t.Fatalf("failed to delete %v", err)
	}

	require.Equal(t, seeds[1].ID, id)
}
