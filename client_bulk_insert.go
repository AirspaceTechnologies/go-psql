package psql

import (
	"errors"
	"reflect"

	"github.com/lib/pq"
)

// Unlike the other Client funcs this executes immediately and does not return a Query that you Exec() on
func (c *Client) BulkInsert(p BulkProvider, errFunc ModelErrorFunc) error {
	m := p.NextModel()
	if m == nil {
		return errors.New("no models to insert")
	}

	tx, err := c.DB.Begin()
	if err != nil {
		return err
	}

	t := reflect.TypeOf(m)
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}

	colMap := indexes(t)

	fieldIdxs := make([]int, 0, len(colMap))
	cols := make([]string, 0, len(colMap))

	for col, i := range colMap {
		if col == "id" {
			continue
		}
		cols = append(cols, col)
		fieldIdxs = append(fieldIdxs, i)
	}

	baseStmt := pq.CopyIn(m.TableName(), cols...)

	stmt, err := tx.Prepare(baseStmt)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(m)
	if isPtr {
		v = v.Elem()
	}

	attrs := make([]interface{}, 0, len(fieldIdxs))
	for _, f := range fieldIdxs {
		attrs = append(attrs, v.Field(f).Interface())
	}

	_, err = stmt.Exec(attrs...)
	if err != nil {
		return err
	}

	limit := p.Cap()
	for i := 0; i < limit; i++ {
		m := p.NextModel()
		if m == nil {
			break
		}

		v := reflect.ValueOf(m)
		if isPtr {
			v = v.Elem()
		}

		attrs := make([]interface{}, 0, len(fieldIdxs))
		for _, f := range fieldIdxs {
			attrs = append(attrs, v.Field(f).Interface())
		}

		_, err = stmt.Exec(attrs...)
		if err != nil {
			if errFunc != nil {
				errFunc(m, err)
			}

			continue
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) MonitorBulkInsertChannel(ch chan Model, errFunc ModelErrorFunc) error {
	for {
		m, ok := <-ch
		if !ok {
			return nil
		}

		err := c.BulkInsert(NewChannelModelProvider(m, ch), errFunc)
		if err != nil {
			return err
		}
	}
}

// Make sure all Models are the same type
type BulkProvider interface {
	NextModel() Model
	Cap() int
}

// Bulk provider for channels, see '*Client MonitorBulkInsertChannel()' for usage example
type ChannelModelProvider struct {
	first   Model
	Channel <-chan Model
}

func NewChannelModelProvider(m Model, ch <-chan Model) *ChannelModelProvider {
	return &ChannelModelProvider{first: m, Channel: ch}
}

func (p *ChannelModelProvider) NextModel() Model {
	if p.first != nil {
		m := p.first
		p.first = nil
		return m
	}

	if p.Channel == nil {
		return nil
	}

	select {
	case m, ok := <-p.Channel:
		if !ok {
			return nil
		}
		return m
	default:
		return nil
	}
}

func (p *ChannelModelProvider) Cap() int {
	return cap(p.Channel)
}

// Bulk provider for slices of Models
type SliceModelProvider struct {
	i   int
	arr []Model
}

func NewSliceModelProvider(arr []Model) *SliceModelProvider {
	return &SliceModelProvider{arr: arr}
}

func (p *SliceModelProvider) NextModel() Model {
	if p.i >= len(p.arr) {
		return nil
	}
	defer func() { p.i++ }()
	return p.arr[p.i]
}

func (p *SliceModelProvider) Cap() int {
	return len(p.arr)
}

type ModelErrorFunc = func(Model, error)