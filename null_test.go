package psql

import (
	"encoding/json"
	"testing"
)

func TestNullBool_UnmarshalJSON(t *testing.T) {
	type TestStruct struct {
		Key NullBool
	}

	tcs := []struct {
		JSON  string
		Valid bool
		Error bool
	}{
		{
			JSON:  "{\"key\": null}",
			Valid: false,
			Error: false,
		},
		{
			JSON:  "{\"key\": false}",
			Valid: true,
			Error: false,
		},
		{
			JSON:  "{\"key\": \"false\"}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": 0}",
			Valid: false,
			Error: true,
		},
	}

	for i, tc := range tcs {
		var testStruct TestStruct

		err := json.Unmarshal([]byte(tc.JSON), &testStruct)

		if err != nil && !tc.Error {
			t.Fatalf("test case %d error unmarshalling %v", i, err)
		} else if err == nil && tc.Error {
			t.Fatalf("test case %d expected an error unmarshalling", i)
		}

		if testStruct.Key.Valid != tc.Valid {
			t.Fatalf("test case %d test struct key valid was unexpected", i)
		}
	}
}

func TestNullInt64_UnmarshalJSON(t *testing.T) {
	type TestStruct struct {
		Key NullInt64
	}

	tcs := []struct {
		JSON  string
		Valid bool
		Error bool
	}{
		{
			JSON:  "{\"key\": null}",
			Valid: false,
			Error: false,
		},
		{
			JSON:  "{\"key\": 0}",
			Valid: true,
			Error: false,
		},
		{
			JSON:  "{\"key\": \"0\"}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": false}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": 0.5}",
			Valid: false,
			Error: true,
		},
	}

	for i, tc := range tcs {
		var testStruct TestStruct

		err := json.Unmarshal([]byte(tc.JSON), &testStruct)

		if err != nil && !tc.Error {
			t.Fatalf("test case %d error unmarshalling %v", i, err)
		} else if err == nil && tc.Error {
			t.Fatalf("test case %d expected an error unmarshalling", i)
		}

		if testStruct.Key.Valid != tc.Valid {
			t.Fatalf("test case %d test struct key valid was unexpected", i)
		}
	}
}

func TestNullFloat64_UnmarshalJSON(t *testing.T) {
	type TestStruct struct {
		Key NullFloat64
	}

	tcs := []struct {
		JSON  string
		Valid bool
		Error bool
	}{
		{
			JSON:  "{\"key\": null}",
			Valid: false,
			Error: false,
		},
		{
			JSON:  "{\"key\": 0}",
			Valid: true,
			Error: false,
		},
		{
			JSON:  "{\"key\": \"0\"}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": false}",
			Valid: false,
			Error: true,
		},
	}

	for i, tc := range tcs {
		var testStruct TestStruct

		err := json.Unmarshal([]byte(tc.JSON), &testStruct)

		if err != nil && !tc.Error {
			t.Fatalf("test case %d error unmarshalling %v", i, err)
		} else if err == nil && tc.Error {
			t.Fatalf("test case %d expected an error unmarshalling", i)
		}

		if testStruct.Key.Valid != tc.Valid {
			t.Fatalf("test case %d test struct key valid was unexpected", i)
		}
	}
}

func TestNullString_UnmarshalJSON(t *testing.T) {
	type TestStruct struct {
		Key NullString
	}

	tcs := []struct {
		JSON  string
		Valid bool
		Error bool
	}{
		{
			JSON:  "{\"key\": null}",
			Valid: false,
			Error: false,
		},
		{
			JSON:  "{\"key\": \"\"}",
			Valid: true,
			Error: false,
		},
		{
			JSON:  "{\"key\": false}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": 0.5}",
			Valid: false,
			Error: true,
		},
	}

	for i, tc := range tcs {
		var testStruct TestStruct

		err := json.Unmarshal([]byte(tc.JSON), &testStruct)

		if err != nil && !tc.Error {
			t.Fatalf("test case %d error unmarshalling %v", i, err)
		} else if err == nil && tc.Error {
			t.Fatalf("test case %d expected an error unmarshalling", i)
		}

		if testStruct.Key.Valid != tc.Valid {
			t.Fatalf("test case %d test struct key valid was unexpected", i)
		}
	}
}

func TestNullTime_UnmarshalJSON(t *testing.T) {
	type TestStruct struct {
		Key NullTime
	}

	tcs := []struct {
		JSON  string
		Valid bool
		Error bool
	}{
		{
			JSON:  "{\"key\": null}",
			Valid: false,
			Error: false,
		},
		{
			JSON:  "{\"key\": \"2014-01-01T23:28:56.782Z\"}",
			Valid: true,
			Error: false,
		},
		{
			JSON:  "{\"key\": false}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": 0.5}",
			Valid: false,
			Error: true,
		},
		{
			JSON:  "{\"key\": \"0\"}",
			Valid: false,
			Error: true,
		},
	}

	for i, tc := range tcs {
		var testStruct TestStruct

		err := json.Unmarshal([]byte(tc.JSON), &testStruct)

		if err != nil && !tc.Error {
			t.Fatalf("test case %d error unmarshalling %v", i, err)
		} else if err == nil && tc.Error {
			t.Fatalf("test case %d expected an error unmarshalling", i)
		}

		if testStruct.Key.Valid != tc.Valid {
			t.Fatalf("test case %d test struct key valid was unexpected", i)
		}
	}
}
