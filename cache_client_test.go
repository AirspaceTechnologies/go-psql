package psql

import (
	"context"
	"github.com/airspacetechnologies/go-psql/cache"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCacheClient_RawSelect(t *testing.T) {
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

		func(){
			var results []int
			err := c.RawSelect(ctx, &results, "select int_field from mock_models where int_field < $1", 10)
			require.Nil(t, err)
			require.ElementsMatch(t, results, []int{4})
		}()

		func(){
			var results []MockModel
			err := c.RawSelect(ctx, &results, "select * from mock_models where int_field < $1", 10)
			require.Nil(t, err)

			var ids []int
			var ints []int
			for _, r := range results {
				ids = append(ids, r.ID)
				ints = append(ints, r.IntField)
			}
			require.ElementsMatch(t, ids, []int{1})
			require.ElementsMatch(t, ints, []int{4})
		}()

		for _, m := range models {
			m.IntField -= 2
			err := c.Update(ctx, m)
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

				var results []int
				err := c.RawSelect(ctx, &results, "select int_field from mock_models where int_field < $1", 10)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{4})

				results = nil
				q := c.Select(MockModel{}.TableName(), "int_field")
				err = q.OrderBy("int_field asc").Slice(ctx, &results)
				require.Nil(t, err)
				require.ElementsMatch(t, results, []int{2, 8})
			},
		},
		"select models": {
			Before: before,
			Run: func(t *testing.T, c Client) {
				ctx := context.Background()

				var results []MockModel
				err := c.RawSelect(ctx, &results, "select * from mock_models where int_field < $1", 10)
				require.Nil(t, err)

				var ids []int
				var ints []int
				for _, r := range results {
					ids = append(ids, r.ID)
					ints = append(ints, r.IntField)
				}
				require.ElementsMatch(t, ids, []int{1})
				require.ElementsMatch(t, ints, []int{4})

				results = nil
				q := c.Select(MockModel{}.TableName())
				err = q.OrderBy("id asc").Slice(ctx, &results)
				require.Nil(t, err)

				ids = nil
				ints = nil
				for _, r := range results {
					ids = append(ids, r.ID)
					ints = append(ints, r.IntField)
				}
				require.ElementsMatch(t, ids, []int{1, 2})
				require.ElementsMatch(t, ints, []int{2, 8})
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
				_, _ = c.ExecContext(context.Background(), "drop table mock_models")
				_ = c.Close()
			}()

			cc := NewCacheClient(cache.NewRW(), c, time.Second)
			if tc.Before != nil {
				tc.Before(t, cc)
			}
			tc.Run(t, cc)
		})
	}
}
