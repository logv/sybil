Sybil is an append only analytics datastore with no up front table schema
requirements; just log JSON records to a table and run queries. Written in Go,
sybil is designed for fast full table scans of multi-dimensional data on a
single machine.

more documentation is available [on the wiki](http://github.com/logv/sybil/wiki)
and [in the repo](http://github.com/logv/sybil/blob/master/docs).

if sybil by itself is uninteresting (who wants to run command line queries,
anyways?), sybil is a supported backend for
[snorkel](http://github.com/logv/snorkel)

advantages
----------

  * Easy to setup and get data inside sybil - just pipe JSON on stdin to sybil
  * Supports histograms (and percentiles), standard deviations and time series roll ups
  * Runs fast full table queries ([performance notes](http://github.com/logv/sybil/wiki/Performance))
  * Lower disk usage through per column compression schemes
  * Serverless design with controlled memory usage
  * Per table retention policies (specify max age and/or size of tables)
  * Per block query cache (optional) that avoids recomputation

disadvantages
-------------

  * JOINS not supported
  * No UPDATE operation on data - only writes
  * No sharding

installation
------------

    go get github.com/logv/sybil

grpc
----

Sybil can run as a standalone binary or as a GRPC service. To use the sybil
grpc service. the server is run with `sybil serve`, while the clients have an
additional `-dial localhost:7000` flag added to their invocations.


additional information
----------------------

* [command line tour](http://github.com/logv/sybil/wiki/Quick-Overview)
* [want to contribute?](http://github.com/logv/sybil/wiki/Contributing)
* [notes on performance](http://github.com/logv/sybil/wiki/Performance)
* [abadi survey of column stores](http://db.csail.mit.edu/pubs/abadi-column-stores.pdf)
