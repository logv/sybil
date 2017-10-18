# Style Guide

sybil follows go style mostly, minus a few small differences:

* ALL_CAPS variables express config and global variables
* under_cased structs fields express private variables that should be touched with care


# Example File


```
package sybil

import (
  "foo"
  "bar"
  "baz"
)

// these var tends to get used across files for
// global config and compilation flags. we can
// expect someone outside this file to be accessing
// or setting it
var MY_GLOBAL_VAR = "foo"

// The vars that can be influenced from the outside of the package
// are generally contained into sybil.OPTS or sybil.FLAGS
var OPTS.MY_CONFIG_VAR = MY_GLOBAL_VAR;
var FLAGS.MY_CONFIG_VAR = &MY_GLOBAL_VAR;

var privateFileVar // this var is generally local to this file
var ExportedModuleVar // this var is exported to other packages

type MyStruct struct {
  ExportedFields int64
  localFieldFoo  string

  private_field_foo string
  private_field_bar bool

}

// this is a private-ish func
func local_func() {
  local_primitive := 0
  if MY_GLOBAL_VAR {
    local_primitive += 1
  }

}

// this is a local func and supported across the package
func localFunc() {
  // this is showing that you are touching a variable that you are
  // not supposed to (or know what you are doing when touching it)
  myVar.a_primitive_var['foo'] = 1

  // This is a safe API
  myVar.setAPrimitive('foo', 1)

  // this is a public API
  myVar.SetAPrimivive('foo', 1)

}

// this is exported to other modules
func ExportedFunc() {
  localInstance := NewFooBar()
  localInstance.safeAPICall("blah")
  localInstance.PublicAPICall("blah")
}

```
