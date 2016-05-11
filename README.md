Sybil is a write once analytics datastore with no up front table schema requirements;
just log JSON records to a table and run queries. Written in Go, sybil is
designed for fast ad-hoc analysis of multi-dimensional data on a single machine.

advantages
----------

  * Easy to setup and get data inside sybil - just pipe JSON on stdin to sybil
  * Supports histograms (and percentiles), standard deviations and time series roll ups
  * Runs really fast full table queries (analyze millions of samples in under a second!)
  * Lower disk usage through per column compression schemes
  * Serverless design with controlled memory usage

disadvantages
-------------

  * Not optimized for write speed, mainly for query speed
  * Does not support JOINS
  * Doesn't have a transaction log or full ACID reliability guarantees
  * No UPDATE operation on data - only writes
  * Sybil is meant for use on a single machine, no sharding

installation
============

    go get github.com/logV/sybil

documentation
=============

[the wiki](http://github.com/logv/sybil/wiki)


usage
=====

inserting records
-----------------

    # import from a file (one record per line)
    sybil ingest -table my_table < record.json

    # import from a mongo DB
    mongoexport -collection my_collection | sybil ingest -table my_table

    # import from a CSV file
    sybil ingest -csv -table my_csv_table < some_csv.csv

    # check out the db file structure
    ls -R db/


querying records
----------------

    # list tables
    sybil query -tables

    # query that table
    sybil query -table my_table -info
    sybil query -table my_table -print

    # run a more complicated query (get status, host and histogram of pings)
    ./bin/sybil query -table uptime -group status,host -int ping -print -op hist

    # make that a time series JSON blob
    ./bin/sybil query -table uptime -group status,host -int ping -json -op hist -time

    # filter the previous query to samples that with host ~= mysite.*
    ./bin/sybil query -table uptime -group status,host -int ping -json -op hist -time -str-filter host:re:mysite.*
