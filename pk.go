package zanredisdb

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidKey = errors.New("invalid key format")

func ValidPrefix(k string) bool {
	fields := strings.SplitN(k, ":", 3)
	if len(fields) < 3 || len(fields[2]) <= 0 {
		return false
	}
	return true
}

func ParsePKey(k string) (*PKey, error) {
	fields := strings.SplitN(k, ":", 3)
	if len(fields) < 3 || len(fields[2]) <= 0 {
		return nil, ErrInvalidKey
	}
	return NewPKey(fields[0], fields[1], []byte(fields[2])), nil
}

type PKey struct {
	Namespace string
	Set       string
	PK        []byte
	RawKey    []byte
}

func NewPKey(ns string, set string, pk []byte) *PKey {
	var tmp bytes.Buffer
	tmp.WriteString(ns)
	tmp.WriteString(":")
	tmp.WriteString(set)
	tmp.WriteString(":")
	tmp.Write(pk)
	return &PKey{
		Namespace: ns,
		Set:       set,
		PK:        pk,
		RawKey:    tmp.Bytes(),
	}
}

func (self *PKey) ShardingKey() []byte {
	return self.RawKey[len(self.Namespace)+1:]
}

func (self *PKey) String() string {
	return fmt.Sprintf("%s", string(self.RawKey))
}
