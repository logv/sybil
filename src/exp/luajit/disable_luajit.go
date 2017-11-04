//+build !luajit

package sybil

import specs "github.com/logv/sybil/src/query/specs"
import . "github.com/logv/sybil/src/lib/structs"

// InitLua ...
func InitLua(enable *bool) {
	*enable = false
}

// LuaKey ...
type LuaKey interface{}

// SetLuaScript ...
func SetLuaScript(filename string) {}

func LuaInit(qs *specs.QuerySpec)                            {}
func LuaMap(qs *specs.QuerySpec, rl *RecordList)             {}
func LuaCombine(qs *specs.QuerySpec, other *specs.QuerySpec) {}
func LuaFinalize(qs *specs.QuerySpec)                        {}
