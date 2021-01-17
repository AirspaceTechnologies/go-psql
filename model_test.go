// +build albatross

package psql

import (
	"testing"
	"time"
)

const modelsTable = `
create table mock_models (
id bigserial primary key,
string_field text,
null_string_field text,
int_field integer,
float_field numeric,
bool_field boolean,
time_field timestamp,
json_object jsonb,
"table" text,
encryptable text,
created_at timestamp default CURRENT_TIMESTAMP not null
)
`

type MockModel struct {
	ID              int `sql:"id"`
	NonSQLField     string
	StringField     string            `sql:"string_field"`
	NullStringField NullString        `sql:"null_string_field"`
	IntField        int               `sql:"int_field"`
	FloatField      float64           `sql:"float_field"`
	BoolField       bool              `sql:"bool_field"`
	JSONObject      JSONObject        `sql:"json_object"`
	TimeField       time.Time         `sql:"time_field"`
	Table           string            `sql:"table"` // making sure reserved words work
	Encryptable     EncryptableString `sql:"encryptable"`
	CreatedAt       time.Time         `sql:"created_at"`
}

func (m MockModel) TableName() string {
	return "mock_models"
}

type MockModelNullableID struct {
	ID              NullInt64 `sql:"id"`
	NonSQLField     string
	StringField     string     `sql:"string_field"`
	NullStringField NullString `sql:"null_string_field"`
	IntField        int        `sql:"int_field"`
	FloatField      float64    `sql:"float_field"`
	BoolField       bool       `sql:"bool_field"`
	TimeField       time.Time  `sql:"time_field"`
	Table           string     `sql:"table"`
	JSONObject      JSONObject `sql:"json_object"`
}

func (m MockModelNullableID) TableName() string {
	return "mock_models"
}

type ExtendedNullInt64 struct {
	NullInt64
}

type MockModelExtendedID struct {
	ID              ExtendedNullInt64 `sql:"id"`
	NonSQLField     string
	StringField     string     `sql:"string_field"`
	NullStringField NullString `sql:"null_string_field"`
	IntField        int        `sql:"int_field"`
	FloatField      float64    `sql:"float_field"`
	BoolField       bool       `sql:"bool_field"`
	TimeField       time.Time  `sql:"time_field"`
	Table           string     `sql:"table"`
	JSONObject      JSONObject `sql:"json_object"`
}

func (m MockModelExtendedID) TableName() string {
	return "mock_models"
}

func TestModelHelper_SetId(t *testing.T) {
	// int id
	m := &MockModel{}
	mh := &ModelHelper{Model: m}

	if err := mh.SetID(5); err != nil || m.ID != 5 {
		t.Fatalf("error setting id %v", err)
	}

	// NullableInt64 id
	mn := &MockModelNullableID{}
	mh = &ModelHelper{Model: mn}

	if err := mh.SetID(5); err != nil || !mn.ID.Valid || mn.ID.Int64 != 5 {
		t.Fatalf("error setting id %v", err)
	}

	// Non pointer
	mh = &ModelHelper{Model: *mn}

	if err := mh.SetID(5); err == nil {
		t.Fatalf("expected error")
	}
}

func TestModelHelper_Id(t *testing.T) {
	tcs := []struct {
		Model      Model
		ExpectedID int64
	}{
		{
			Model: MockModel{
				ID: 10,
			},
			ExpectedID: 10,
		},
		{
			Model: &MockModel{
				ID: 10,
			},
			ExpectedID: 10,
		},
		{
			Model: MockModelNullableID{
				ID: NewNullInt64(10),
			},
			ExpectedID: 10,
		},
		{
			Model: MockModelExtendedID{
				ID: ExtendedNullInt64{NewNullInt64(10)},
			},
			ExpectedID: 10,
		},
	}

	for i, tc := range tcs {
		mh := &ModelHelper{Model: tc.Model}

		if id, err := mh.ID(); err != nil || id != tc.ExpectedID {
			t.Fatalf("tc %d error getting id %v", i, err)
		}
	}
}
