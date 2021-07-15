package psql

import (
	"context"
	"fmt"
	"github.com/airspacetechnologies/go-psql/cache"
	"reflect"
	"strings"
	"time"
)

type cacheKey string

type cacheClient struct {
	Client
	cache  cache.Cache
	exp    time.Duration
}

func NewCacheClient(cache cache.Cache, client Client, exp time.Duration) Client {
	return &cacheClient{
		cache:  cache,
		Client: client,
		exp:    exp,
	}
}

func (c *cacheClient) RawSelect(ctx context.Context, outSlicePtr interface{}, q string, args ...interface{}) error {
	outSliceType := reflect.TypeOf(outSlicePtr)
	k := c.key(q, append(args, ";slice_ptr_type::", outSliceType)...)

	if cached, ok := c.cache.Get(k); ok {
		out := reflect.ValueOf(outSlicePtr).Elem()
		out.Set(reflect.ValueOf(cached))

		return nil
	}

	if err := c.Client.RawSelect(ctx, outSlicePtr, q, args...); err != nil {
		return err
	}

	var exp time.Time
	if c.exp > 0 {
		exp = time.Now().Add(c.exp)
	}

	c.cache.Set(k, reflect.ValueOf(outSlicePtr).Elem().Interface(), exp)

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *cacheClient) key(sql string, args ...interface{}) cacheKey {
	var b strings.Builder
	b.WriteString(sql)
	for _, v := range args {
		fmt.Fprint(&b, v)
	}
	return cacheKey(b.String())
}
