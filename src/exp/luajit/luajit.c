//+build luajit

#include <stdlib.h>
#include <stdio.h>
#include <luajit-2.0/lua.h>
#include <luajit-2.0/lualib.h>
#include <luajit-2.0/lauxlib.h>

#include "_cgo_export.h"
int init() {
printf("INIT!\n");
return 0;
}

void set_numfield (lua_State *state, const char *index, double value) {
lua_pushstring(state, index);
lua_pushnumber(state, value);
lua_settable(state, -3);
}

void set_strfield (lua_State *state, const char *index, char *value) {
lua_pushstring(state, index);
lua_pushstring(state, value);
lua_settable(state, -3);
}

