package psql

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

var (
	NonceByteSize       = 12                           // standard nonce size for GCM cipher
	EncryptionKey       string                         // must be 128, 192 or 256 bit, blank disables encryption
	DecryptionErrorFunc func(interface{}, error) error // You can log or take other actions. The error returned will be returned from Scan function
)

/*
EncryptableString is a struct containing a string that can be stored as an encrypted string in the database.
It extends NullString so the value can be null. In memory the string will be in plain text.
In the DB it is stored as a hex string where the first 8 bytes represent an integer which is the byte size of the nonce,
The next nonce size bytes are the nonce and the rest of the bytes are the encrypted data
*/
type EncryptableString struct {
	NullString
}

func NewEncryptableString(s string) EncryptableString {
	return EncryptableString{NullString: NewNullString(s)}
}

// Scan implements the Scanner interface. (converts from DB)
func (s *EncryptableString) Scan(value interface{}) error {
	if value == nil {
		s.String, s.Valid = "", false
		return nil
	}

	src, ok := value.(string)
	if !ok {
		s.String, s.Valid = "", false
		return fmt.Errorf("unsuported type %T", value)
	}

	key := EncryptionKey
	if key == "" {
		// skip decryption if key is blank
		s.String, s.Valid = src, true
		return nil
	}

	str, err := decrypt(src, key)
	if err != nil {
		s.String, s.Valid = "", false
		if DecryptionErrorFunc != nil {
			return DecryptionErrorFunc(value, fmt.Errorf("error decrypting during scan %w", err))
		}
		return nil
	}

	s.String, s.Valid = str, true

	return nil
}

// Value implements the driver Valuer interface. (converts for DB)
func (s EncryptableString) Value() (driver.Value, error) {
	if !s.Valid {
		return nil, nil
	}

	key := EncryptionKey
	if key == "" {
		// skip encryption if key is blank
		return s.String, nil
	}

	return encrypt(s.String, key)
}

// Helpers to encrypt and decrypt, key and the encrypted string should be in hex format

func encrypt(s, key string) (string, error) {
	b := []byte(s)

	k, err := hex.DecodeString(key)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(k)
	if err != nil {
		return "", err
	}

	n := NonceByteSize
	nonce := make([]byte, n)
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	buff := make([]byte, 8)

	// first 8 bits is the nonce size
	binary.PutVarint(buff, int64(n))

	// next nonce append the nonce
	buff = append(buff, nonce...)

	encrypted := aesgcm.Seal(nil, nonce, b, nil)
	// append encrypted data
	buff = append(buff, encrypted...)

	return hex.EncodeToString(buff), nil
}

func decrypt(s, key string) (string, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}

	k, err := hex.DecodeString(key)
	if err != nil {
		return "", err
	}

	if len(b) <= 8 {
		return "", fmt.Errorf("missing nonce size")
	}
	// get nonce size from first 8 bytes
	n, _ := binary.Varint(b[:8])
	// remove nonce size from bytes
	b = b[8:]

	if n > int64(len(b)) {
		return "", fmt.Errorf("missing nonce of size %v bytes", n)
	}

	// get nonce and encrypted data
	nonce, encrypted := b[:n], b[n:]

	block, err := aes.NewCipher(k)
	if err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	decrypted, err := aesgcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return "", err
	}

	return string(decrypted), nil
}
