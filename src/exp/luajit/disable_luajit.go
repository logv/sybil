//+build !luajit

package sybil

import . "github.com/logv/sybil/src/query/specs"
import . "github.com/logv/sybil/src/lib/structs"

// InitLua ...
func InitLua(enable *bool) {
	*enable = false
}

// LuaKey ...
type LuaKey interface{}

// SetLuaScript ...
func SetLuaScript(filename string) {}

func LuaInit(qs *QuerySpec)                      {}
func LuaMap(qs *QuerySpec, rl *RecordList)       {}
func LuaCombine(qs *QuerySpec, other *QuerySpec) {}
func LuaFinalize(qs *QuerySpec)                  {}
