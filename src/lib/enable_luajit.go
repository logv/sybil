//+build luajit

package sybil

/*
#cgo LDFLAGS: -lluajit-5.1
#include <stdlib.h>
#include <stdio.h>
#include <luajit-2.0/lua.h>
#include <luajit-2.0/lualib.h>
#include <luajit-2.0/lauxlib.h>

// declare anything in luajit.c that we want to talk to
extern void set_numfield (lua_State *state, const char *index, double value);
extern void set_strfield (lua_State *state, const char *index, char *value);
*/
import "C"

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"unsafe"
)

const PREAMBLE = `
local ffi = require('ffi')
ffi.cdef([[
    int init();
	int go_get_int(int, int, int);

	int go_get_str_id(int, int, int);
	char* go_get_str_val(int, int, int);
]])

function get_int(id, col)
  return ffi.C.go_get_int(block_id, id, col)
end

-- This uses get_str_id and get_str_val
local col_str_cache = {}
function get_str(id, col)
  str_id = get_str_id(id, col)
  if col_str_cache[col] == nil then
	col_str_cache[col] = {}
  end

  if col_str_cache[col][str_id] == nil then
	local ret = ffi.string(get_str_val(str_id, col))
	col_str_cache[col][str_id]  = ret
  end

  return col_str_cache[col][str_id]
end

function get_str_id(id, col)
  return ffi.C.go_get_str_id(block_id, id, col)
end

function get_str_val(str_id, col)
  return ffi.C.go_get_str_val(block_id, str_id, col)
end





-- END PREAMBLE
`

var SRC = ` `

func initLua() {
	ENABLE_LUA = true
}

var LUA_BLOCK_ID = 0
var LUA_BLOCKS = make([]*QuerySpec, 0)
var LUA_LOCK = sync.Mutex{}

//export go_get_int
func go_get_int(block_id, record_id, col_id int) int {
	if LUA_BLOCKS[block_id-1].Matched[record_id-1].Populated[col_id] == INT_VAL {
		return int(LUA_BLOCKS[block_id-1].Matched[record_id-1].Ints[col_id])
	}

	return -1
}

//export go_get_str_id
func go_get_str_id(block_id, record_id int, col_id int) int {
	if LUA_BLOCKS[block_id-1].Matched[record_id-1].Populated[col_id] == STR_VAL {
		return int(LUA_BLOCKS[block_id-1].Matched[record_id-1].Strs[col_id])

	}

	return -1
}

//export go_get_str_val
// TODO: this should be cached so we don't keep adding new memory
func go_get_str_val(block_id, str_id int, col_id int) *C.char {
	col := LUA_BLOCKS[block_id-1].Matched[0].block.GetColumnInfo(int16(col_id))
	val := col.get_string_for_val(int32(str_id))
	return C.CString(val)

}

func SetLuaScript(filename string) {
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		Error("Couldn't open Lua script", filename, err)
	}

	FLAGS.LUA = &TRUE
	HOLD_MATCHES = true
	SRC = string(dat)
}

type LuaKey interface{}
type LuaTable map[string]interface{}

// creates a luatable inside state that contains the contents of T
// TODO: add ability to marshall arrays
func setLuaTable(state *C.struct_lua_State, t LuaTable) {
	C.lua_createtable(state, 0, 0)

	// iterate through all keys in our table
	for k, v := range t {
		switch v := v.(type) {
		case bool:
		case int:
			C.set_numfield(state, C.CString(k), C.double(v))
		case float64:
			C.set_numfield(state, C.CString(k), C.double(v))
		case string:
			C.set_strfield(state, C.CString(k), C.CString(v))
		case LuaTable:
			setLuaTable(state, v)
		default:
			fmt.Printf("unexpected type %T\n", v) // %T prints whatever type t has
		}
	}

}

func getLuaTable(state *C.struct_lua_State) LuaTable {
	/* table is in the stack at index 't' */
	it := C.int(C.lua_gettop(state))
	C.lua_pushnil(state) /* first key */

	ret := make(LuaTable, 0)

	for C.lua_next(state, it) != 0 {
		/* uses 'key' (at index -2) and 'value' (at index -1) */
		keytype := C.lua_type(state, -2)

		var key string
		var val LuaKey

		switch C.lua_type(state, -1) {
		case C.LUA_TNUMBER:
			val = float64(C.lua_tonumber(state, -1))
		case C.LUA_TBOOLEAN:
			val = C.lua_toboolean(state, -1)
		case C.LUA_TSTRING:
			val = C.lua_tolstring(state, -1, nil)
		case C.LUA_TTABLE:
			val = getLuaTable(state)
		default:
			fmt.Printf("unexpected type %T\n", C.lua_type(state, -1)) // %T prints whatever type t has

		}

		if keytype == C.LUA_TSTRING {
			key = C.GoString(C.lua_tolstring(state, -2, nil))
		} else {
			key = fmt.Sprintf("%v", int(C.lua_tonumber(state, -2)))
		}

		ret[key] = val

		/* removes 'value'; keeps 'key' for next iteration */
		C.lua_settop(state, (-1)-1)
	}

	return ret

}

func (qs *QuerySpec) luaInit() {
	// Initialize state.
	qs.LuaState = C.luaL_newstate()
	state := qs.LuaState
	if state == nil {
		fmt.Println("Unable to initialize Lua context.")
	}
	C.luaL_openlibs(state)

	// Compile the script.
	csrc := C.CString(fmt.Sprintf("%s\n%s", PREAMBLE, SRC))
	defer C.free(unsafe.Pointer(csrc))
	if C.luaL_loadstring(state, csrc) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua error: %v\n", errstring)
		os.Exit(1)
	}

	// Execute outer level
	if C.lua_pcall(state, 0, 0, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua execution error: %v\n", errstring)
	}

	LUA_LOCK.Lock()
	LUA_BLOCKS = append(LUA_BLOCKS, qs)
	LUA_BLOCK_ID += 1
	C.lua_pushnumber(state, C.lua_Number(LUA_BLOCK_ID))
	LUA_LOCK.Unlock()

	C.lua_setfield(state, C.LUA_GLOBALSINDEX, C.CString("block_id"))

	col_mapping := make(LuaTable, 0)
	for id, name := range qs.Table.key_string_id_lookup {
		col_mapping[name] = int(id)
	}
	setLuaTable(state, col_mapping)
	C.lua_setfield(state, C.LUA_GLOBALSINDEX, C.CString("COLS"))

}

func (qs *QuerySpec) luaMap(rl *RecordList) LuaTable {
	state := qs.LuaState
	// Execute map function
	C.lua_getfield(state, C.LUA_GLOBALSINDEX, C.CString("map"))
	C.lua_pushnumber(state, C.lua_Number(len(*rl)))
	if C.lua_pcall(state, 1, 1, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua Reduce execution error: %v\n", errstring)
	} else {
		ret := getLuaTable(state)

		qs.LuaResult = ret
		return ret
	}
	return make(LuaTable, 0)

}

func (qs *QuerySpec) luaCombine(other *QuerySpec) LuaTable {
	// call to reduce
	state := qs.LuaState
	C.lua_getfield(state, C.LUA_GLOBALSINDEX, C.CString("reduce"))
	setLuaTable(state, qs.LuaResult)
	setLuaTable(state, other.LuaResult)

	if C.lua_pcall(state, 2, 1, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua Combine execution error: %v\n", errstring)
	} else {

		combined := getLuaTable(state)

		qs.LuaResult = combined
		return combined
	}

	return make(LuaTable, 0)

}

func (qs *QuerySpec) luaFinalize() LuaTable {
	state := qs.LuaState
	// call to finalize
	C.lua_getfield(state, C.LUA_GLOBALSINDEX, C.CString("finalize"))
	setLuaTable(state, qs.LuaResult)

	if C.lua_pcall(state, 1, 1, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua Finalize execution error: %v\n", errstring)
	} else {

		Debug("FINALIZING", qs.LuaResult)
		finalized := getLuaTable(state)
		qs.LuaResult = finalized

		Print("FINALIZED", qs.LuaResult)
		return finalized
	}

	return make(LuaTable, 0)
}
