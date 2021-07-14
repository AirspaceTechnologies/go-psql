package cache

import "time"

type item struct {
	Value   interface{}
	Expires int64
}

func (i item) Expired() bool {
	if i.Expires <= 0 {
		return false
	}

	return time.Now().UnixNano() > i.Expires
}
