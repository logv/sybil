## About

This is an example API wrapper around calling out to sybil from golang. Sybil
(by nature) is a binary, but people often want to interact with it
programatically - this API allows for interacting with sybil directly from Go
code.

## Features

* API for ingesting JSON samples
* API for ingesting Struct samples
* Can query table info
* Declarative query builder for rollup, samples and time series queries

## Usage

See the demo/ directory for actual usage of the API

## TODO

* context with timeouts (to prevent long lasting operations)
* auto flush at regular intervals
* logging of stats for how long ingest, digest, etc take
* fetch table info before running query so queries are validated

