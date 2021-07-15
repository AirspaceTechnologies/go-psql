package psql

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTx_CommitAndRollback(t *testing.T) {
	tcs := map[string]struct {
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"insert 2 and delete 1 committed": {
			Run: func(t *testing.T, c Client) {
				m1 := &MockModel{IntField: 1}
				err := c.Insert(context.Background(), m1)
				require.Nil(t, err)

				tx, err := c.BeginTx(context.Background(), nil)
				require.Nil(t, err)

				m2 := &MockModel{IntField: 2}
				err = tx.Insert(context.Background(), m2)
				require.Nil(t, err)

				var results []int
				err = tx.Select(m1.TableName(), "int_field").OrderBy("int_field ASC").Slice(context.Background(), &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{1, 2})

				results = nil
				_, err = tx.Delete(context.Background(), m1)
				require.Nil(t, err)

				// should only have record outside commit
				results = nil
				err = c.Select(m1.TableName(), "int_field").OrderBy("int_field ASC").Slice(context.Background(), &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{1})

				err = tx.Commit()
				require.Nil(t, err)

				results = nil
				err = c.Select(m1.TableName(), "int_field").OrderBy("int_field ASC").Slice(context.Background(), &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{2})
			},
		},
		"insert update then rollback": {
			Run: func(t *testing.T, c Client) {
				m1 := &MockModel{IntField: 1}
				err := c.Insert(context.Background(), m1)
				require.Nil(t, err)

				tx, err := c.BeginTx(context.Background(), nil)
				require.Nil(t, err)

				m1.IntField = 2
				err = tx.Update(context.Background(), m1, "int_field")
				require.Nil(t, err)

				var results []int
				err = tx.Select(m1.TableName(), "int_field").OrderBy("int_field ASC").Slice(context.Background(), &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{2})

				results = nil
				_, err = tx.Delete(context.Background(), m1)
				require.Nil(t, err)

				// should be original outside tx
				results = nil
				err = c.Select(m1.TableName(), "int_field").OrderBy("int_field ASC").Slice(context.Background(), &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{1})

				err = tx.Rollback()
				require.Nil(t, err)

				results = nil
				err = c.Select(m1.TableName(), "int_field").OrderBy("int_field ASC").Slice(context.Background(), &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{1})
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_Select(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"select values": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					var results []int
					q := tx.Select(MockModel{}.TableName(), "int_field")
					err := q.OrderBy("int_field asc").Slice(ctx, &results)
					require.Nil(t, err)
					require.ElementsMatch(t, results, []int{4, 10})
					return nil
				}, nil)
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
		"select models": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					var results []MockModel
					q := tx.Select(MockModel{}.TableName())
					err := q.OrderBy("int_field asc").Slice(ctx, &results)
					require.Nil(t, err)

					var ids []int
					var ints []int
					for _, r := range results {
						ids = append(ids, r.ID)
						ints = append(ints, r.IntField)
					}
					require.ElementsMatch(t, ids, []int{1, 2})
					require.ElementsMatch(t, ints, []int{4, 10})
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_Insert(t *testing.T) {
	tcs := map[string]struct {
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"insert 2 models": {
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					models := []*MockModel{
						{IntField: 4},
						{IntField: 10},
					}
					for _, m := range models {
						err := tx.Insert(ctx, m)
						require.Nil(t, err)
					}

					return nil
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_Update(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"update a model": {
			Before: before,
			Run: func(t *testing.T, c Client) {
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
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_Delete(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"delete a model": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					i, err := tx.Delete(ctx, MockModel{ID: 2})
					require.Nil(t, err)
					require.Equal(t, int64(1), i)

					return nil
				}, nil)
				require.Nil(t, err)

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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_Save(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"update a model": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					err := tx.Save(ctx, MockModel{ID: 1, IntField: 20}, "int_field")
					require.Nil(t, err)

					return nil
				}, nil)
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
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					err := tx.Save(ctx, &MockModel{IntField: 20}, "int_field")
					require.Nil(t, err)

					return nil
				}, nil)
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_UpdateAll(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"update models": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					q := tx.UpdateAll(MockModel{}.TableName(), Attrs{"int_field": -1})
					r, err := q.WhereRaw("int_field < %v", 10).Exec(ctx)
					require.Nil(t, err)
					require.Equal(t, int64(2), r.RowsAffected)

					return nil
				}, nil)
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_DeleteAll(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"update models": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					q := tx.DeleteAll(MockModel{}.TableName())
					r, err := q.WhereRaw("int_field < %v", 10).Exec(ctx)
					require.Nil(t, err)
					require.Equal(t, int64(2), r.RowsAffected)

					return nil
				}, nil)
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
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

			if _, err := c.ExecContext(context.Background(), modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_RawSelect(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"select values": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					var results []int
					err := tx.RawSelect(ctx, &results, "select int_field from mock_models where int_field < $1", 10)
					require.Nil(t, err)
					require.ElementsMatch(t, results, []int{4})
					return nil
				}, nil)
				require.Nil(t, err)

				var results []int
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4, 10})
			},
		},
		"select models": {
			Before: before,
			Run: func(t *testing.T, c Client) {
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_RawQuery(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"update models": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					r, err := tx.RawQuery(ctx, "update mock_models set int_field = $1 where int_field < $1 returning id", 10)
					require.Nil(t, err)

					var results []int
					err = r.Slice(ctx, &results)
					require.Nil(t, err)
					require.ElementsMatch(t, []int{1}, results)
					return nil
				}, nil)
				require.Nil(t, err)

				var results []int
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_InsertReturning(t *testing.T) {
	tcs := map[string]struct {
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"insert 2 models": {
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					models := []*MockModel{
						{IntField: 4},
						{IntField: 10},
					}
					for _, m := range models {
						err := tx.InsertReturning(ctx, m)
						require.Nil(t, err)
						require.Greater(t, m.ID, 0)
						require.Greater(t, m.IntField, 0)
					}

					return nil
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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_UpdateReturning(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"update a model": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					m := &MockModel{ID: 1, IntField: 20}
					err := tx.UpdateReturning(ctx, m, "int_field")
					require.Nil(t, err)

					r, err := tx.UpdateAll(m.TableName(), Attrs{"float_field": 4.6}).Exec(ctx)
					require.Nil(t, err)
					require.Equal(t, int64(2), r.RowsAffected)

					m.IntField = 6
					err = tx.UpdateReturning(ctx, m, "int_field")
					require.Nil(t, err)
					require.Equal(t, 6, m.IntField)
					require.Equal(t, 4.6, m.FloatField)

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
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			c := NewClient(nil)

			if err := c.Start(""); err != nil {
				t.Fatalf("Failed to start %v", err)
			}

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}

func TestTx_DeleteReturning(t *testing.T) {
	before := func(t *testing.T, c Client) {
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
		Before func(*testing.T, Client)
		Run    func(*testing.T, Client)
	}{
		"delete a model": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				err := c.RunInTransaction(ctx, func(ctx context.Context, tx *Tx) error {
					m := &MockModel{ID: 2}
					err := tx.DeleteReturning(ctx, m)
					require.Nil(t, err)
					require.Equal(t, 10, m.IntField)

					return nil
				}, nil)
				require.Nil(t, err)

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

			if _, err := c.ExecContext(context.Background(),modelsTable); err != nil {
				t.Fatalf("failed to create table %v", err)
			}

			defer func() {
				_, _ = c.ExecContext(context.Background(),"drop table mock_models")
				_ = c.Close()
			}()

			if tc.Before != nil {
				tc.Before(t, c)
			}
			tc.Run(t, c)
		})
	}
}
