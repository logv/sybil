package sybil

import (
	"errors"
	"fmt"
)

var (
	ErrMissingTable = errors.New("missing table")
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
