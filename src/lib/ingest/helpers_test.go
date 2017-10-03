package sybil

import (
	. "github.com/logv/sybil/src/lib/test_helpers"
	"testing"
)

func TestMain(m *testing.M) {
	RunTests(m)
	DeleteTestDB()
}
