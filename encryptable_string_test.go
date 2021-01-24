package psql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptableString(t *testing.T) {
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

	defer func(old string) { EncryptionKey = old }(EncryptionKey)

	genKey := func() (string, error) {
		key := make([]byte, 32)
		if _, err := io.ReadFull(rand.Reader, key); err != nil {
			return "", err
		}
		return hex.EncodeToString(key), nil
	}

	ctx := context.Background()

	m := &MockModel{}

	//// no key
	EncryptionKey = ""

	// with null value
	m.Encryptable = EncryptableString{}

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	var results []*MockModel

	if err := c.Select(m.TableName()).Where(Attrs{"encryptable": nil}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, EncryptableString{}, results[0].Encryptable)

	// with value present
	m.Encryptable = NewEncryptableString("test string")

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	results = nil

	if err := c.Select(m.TableName()).Where(Attrs{"encryptable": m.Encryptable}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, m.Encryptable, results[0].Encryptable)

	//// clear

	if _, err := c.DeleteAll(m.TableName()).Exec(ctx); err != nil {
		t.Fatalf("delete failed %v", err)
	}

	//// with key (256 bit)

	if k, err := genKey(); err != nil {
		t.Fatalf("failed to generate key %v", err)
	} else {
		EncryptionKey = k
	}

	// with null value

	m.Encryptable = EncryptableString{}

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	results = nil

	if err := c.Select(m.TableName()).Where(Attrs{"encryptable": nil}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, EncryptableString{}, results[0].Encryptable)

	// with value present
	m.Encryptable = NewEncryptableString("test string")

	if err := c.Insert(ctx, m); err != nil {
		t.Fatalf("error inserting %v", err)
	}

	results = nil

	if err := c.Select(m.TableName()).WhereNot(Attrs{"encryptable": nil}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, m.Encryptable, results[0].Encryptable)

	// confirm the string does not exist in plain text

	results = nil

	if err := c.Select(m.TableName()).Where(Attrs{"encryptable": m.Encryptable.String}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	require.Equal(t, 0, len(results))

	// when encryption key changes

	if k, err := genKey(); err != nil {
		t.Fatalf("failed to generate key %v", err)
	} else {
		EncryptionKey = k
	}

	results = nil

	if err := c.Select(m.TableName()).WhereNot(Attrs{"encryptable": nil}).Slice(ctx, &results); err != nil {
		t.Fatalf("Select failed %v", err)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, EncryptableString{}, results[0].Encryptable)
}
