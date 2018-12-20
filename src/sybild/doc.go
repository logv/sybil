// +build grpc

// Package sybild holds the implementation of the sybil grpc service.
//
// A sybil server can be started with the `serve` subcommand.
//
// Sybil queries are sent to a remote by supplying a value to the `-dial` argument.
package sybild
