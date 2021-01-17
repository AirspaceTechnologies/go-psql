package psql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"

	"errors"
)

var (
	nullJSON = []byte("null")
	ErrNil   = errors.New("value is nil")
)

type Nullable interface {
	IsNull() bool
}

// NullInt64 is an alias for sql.NullInt64 data type
type NullInt64 struct {
	sql.NullInt64
}

// NullInt64 Nullable conformance
func (n NullInt64) IsNull() bool {
	return !n.Valid
}

// NullInt64 convenience initializer
func NewNullInt64(i int64) NullInt64 {
	return NullInt64{
		sql.NullInt64{
			Int64: i, Valid: true,
		},
	}
}

// NullInt64 convenience initializer for invalid (nil)
func InvalidNullInt64() NullInt64 {
	return NullInt64{
		sql.NullInt64{
			Valid: false,
		},
	}
}

// Inter
type Inter interface {
	Int() (int64, error)
}

func (n NullInt64) Int() (int64, error) {
	if !n.Valid {
		return 0, ErrNil
	}
	return n.Int64, nil
}

// MarshalJSON for NullInt64
func (n NullInt64) MarshalJSON() ([]byte, error) {
	if n.IsNull() {
		return nullJSON, nil
	}
	return json.Marshal(n.Int64)
}

// UnmarshalJSON for NullInt64
func (n *NullInt64) UnmarshalJSON(b []byte) error {
	if bytes.Equal(nullJSON, b) {
		n.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &n.Int64)
	n.Valid = err == nil
	return err
}

// NullBool is an alias for sql.NullBool data type
type NullBool struct {
	sql.NullBool
}

// NullBool Nullable conformance
func (n NullBool) IsNull() bool {
	return !n.Valid
}

// NullBool convenience initializer
func NewNullBool(b bool) NullBool {
	return NullBool{
		sql.NullBool{
			Bool: b, Valid: true,
		},
	}
}

// NullBool convenience initializer for invalid (nil)
func InvalidNullBool() NullBool {
	return NullBool{
		sql.NullBool{
			Valid: false,
		},
	}
}

// MarshalJSON for NullBool
func (n NullBool) MarshalJSON() ([]byte, error) {
	if n.IsNull() {
		return nullJSON, nil
	}
	return json.Marshal(n.Bool)
}

// UnmarshalJSON for NullBool
func (n *NullBool) UnmarshalJSON(b []byte) error {
	if bytes.Equal(nullJSON, b) {
		n.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &n.Bool)
	n.Valid = err == nil
	return err
}

// NullFloat64 is an alias for sql.NullFloat64 data type
type NullFloat64 struct {
	sql.NullFloat64
}

// NullFloat64 Nullable conformance
func (n NullFloat64) IsNull() bool {
	return !n.Valid
}

// NullFloat64 convenience initializer
func NewNullFloat64(f float64) NullFloat64 {
	return NullFloat64{
		sql.NullFloat64{
			Float64: f, Valid: true,
		},
	}
}

// NullFloat64 convenience initializer for invalid (nil)
func InvalidNullFloat64() NullFloat64 {
	return NullFloat64{
		sql.NullFloat64{
			Valid: false,
		},
	}
}

// MarshalJSON for NullFloat64
func (n NullFloat64) MarshalJSON() ([]byte, error) {
	if n.IsNull() {
		return nullJSON, nil
	}
	return json.Marshal(n.Float64)
}

// UnmarshalJSON for NullFloat64
func (n *NullFloat64) UnmarshalJSON(b []byte) error {
	if bytes.Equal(nullJSON, b) {
		n.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &n.Float64)
	n.Valid = err == nil
	return err
}

// NullString is an alias for sql.NullString data type
type NullString struct {
	sql.NullString
}

// NullString Nullable conformance
func (n NullString) IsNull() bool {
	return !n.Valid
}

// NullString convenience initializer
func NewNullString(s string) NullString {
	return NullString{
		sql.NullString{
			String: s, Valid: true,
		},
	}
}

// NullString convenience initializer for invalid (nil)
func InvalidNullString() NullString {
	return NullString{
		sql.NullString{
			Valid: false,
		},
	}
}

// MarshalJSON for NullString
func (n NullString) MarshalJSON() ([]byte, error) {
	if n.IsNull() {
		return nullJSON, nil
	}
	return json.Marshal(n.String)
}

// UnmarshalJSON for NullString
func (n *NullString) UnmarshalJSON(b []byte) error {
	if bytes.Equal(nullJSON, b) {
		n.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &n.String)
	n.Valid = err == nil
	return err
}

// NullTime is an alias for mysql.NullTime data type
type NullTime struct {
	pq.NullTime
}

// NullTime Nullable conformance
func (n NullTime) IsNull() bool {
	return !n.Valid
}

// NullTime convenience initializer
func NewNullTime(t time.Time) NullTime {
	return NullTime{
		pq.NullTime{
			Time: t, Valid: true,
		},
	}
}

// NullTime convenience initializer for invalid (nil)
func InvalidNullTime() NullTime {
	return NullTime{
		pq.NullTime{
			Valid: false,
		},
	}
}

// MarshalJSON for NullTime
func (n NullTime) MarshalJSON() ([]byte, error) {
	if n.IsNull() {
		return nullJSON, nil
	}
	val := fmt.Sprintf("\"%s\"", n.Time.Format(time.RFC3339))
	return []byte(val), nil
}

// UnmarshalJSON for NullTime
func (n *NullTime) UnmarshalJSON(b []byte) error {
	if bytes.Equal(nullJSON, b) {
		n.Valid = false
		return nil
	}

	err := json.Unmarshal(b, &n.Time)
	n.Valid = err == nil
	return err
}
