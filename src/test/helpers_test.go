package sybil

import (
	"testing"
)

func TestMain(m *testing.M) {
	RunTests(m)
	DeleteTestDB()
}
