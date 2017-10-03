//+build !luajit

package sybil

import . "github.com/logv/sybil/src/lib/specs"

// InitLua ...
func InitLua(enable *bool) {
	*enable = false
}

// LuaKey ...
type LuaKey interface{}

// LuaTable ...
type LuaTable map[string]interface{}

// SetLuaScript ...
func SetLuaScript(filename string) {}

func LuaInit(qs *QuerySpec)                      {}
func LuaMap(qs *QuerySpec)                       {}
func LuaCombine(qs *QuerySpec, other *QuerySpec) {}
func LuaFinalize(qs *QuerySpec)                  {}
