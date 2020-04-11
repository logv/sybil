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

## Scratch

* context with timeouts (to prevent long lasting operations)?
* auto flush at regular intervals?
* logging of stats for how long ingest, digest, etc take
* declarative sample fetcher
* can we codegen samples and use them that way? (or is that not a good idea?)
* fetch table info before running query so queries are validated?

TODO:

* write all tests
* figure out story around Struct samples and what the sybil results that come back look like
* maybe make a demo program that actually graphs results in the terminal using the API
* verify that sybil binary can be packaged with the API for ease of use (make separate repo and validate it works)
* add stats 
* more robustness
* why is reading JSON so annoying
* make tests
* make example program
* make it possible to load structs off disk instead of JSON if thats the API people want to use
  * this works fine for samples but it doesn't work right for query results. For a query result, the struct would be like:
    foo {
      int_col: { 
        avg: 
        percentiles:
        buckets:
        sum: 
      }
      str_col: {
        value
      }
    }
  * in such a case, then we'd have a SampleStruct that is actually augmented but I'm not sure how to automatically gen that struct type. It would be nice to be able to say: s.Foo.Avg though or s.Foo.Buckets and so on. s.Col.Value
