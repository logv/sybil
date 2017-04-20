//+build !luajit

package sybil

func initLua() {
	ENABLE_LUA = false
}

type LuaKey interface{}
type LuaTable map[string]interface{}

func SetLuaScript(filename string) {}

func (qs *QuerySpec) luaInit() {}

func (qs *QuerySpec) luaMap(rl *RecordList) {}

func (qs *QuerySpec) luaCombine(other *QuerySpec) {}

func (qs *QuerySpec) luaFinalize() {}
