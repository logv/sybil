//+build !luajit

package sybil

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

func (qs *QuerySpec) luaInit()                    {}
func (qs *QuerySpec) luaMap(rl *RecordList)       {}
func (qs *QuerySpec) luaCombine(other *QuerySpec) {}
func (qs *QuerySpec) luaFinalize()                {}
