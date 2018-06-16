package sybil

import (
	"errors"
	"fmt"
)

var (
	ErrMissingTable         = errors.New("missing table")
	ErrLockTimeout          = errors.New("lock timeout")
	ErrLockBroken           = errors.New("lock broken")
	ErrKeyTableInconsistent = errors.New("key table is inconsistent")
)

type ErrMissingColumn struct {
	column string
}

func (e ErrMissingColumn) Error() string {
	return fmt.Sprintf("column '%s' is missing", e.column)
}

type ErrColumnTypeMismatch struct {
	column   string
	expected string
}

func (e ErrColumnTypeMismatch) Error() string {
	return fmt.Sprintf("column '%s' is not of type %s", e.column, e.expected)
}

type ErrUnrecoverableLock struct {
	lockfile string
}

func (e ErrUnrecoverableLock) Error() string {
	return fmt.Sprintf("recovery failed for broken lock file: '%s'", e.lockfile)
}
