Sybil is a write once analytics datastore with no up front table schema requirements;
just log JSON records to a table and run queries. Written in Go, sybil is
designed for fast full table scans of multi-dimensional data on a single machine.

more documentation is available [on the wiki](http://github.com/logv/sybil/wiki)
and [in the repo](http://github.com/logv/sybil/blob/master/docs)


advantages
----------

  * Easy to setup and get data inside sybil - just pipe JSON on stdin to sybil
  * Supports histograms (and percentiles), standard deviations and time series roll ups
  * Runs fast full table queries (analyze millions of samples in under a second!)
  * Lower disk usage through per column compression schemes
  * Serverless design with controlled memory usage

disadvantages
-------------

  * Not optimized for write speed, mostly for query speed ([see the performance notes](http://github.com/logv/sybil/wiki/Performance) for more info)
  * Does not support JOINS
  * Doesn't have a transaction log or full ACID reliability guarantees
  * No UPDATE operation on data - only writes
  * Sybil is meant for use on a single machine, no sharding

installation
------------

    go get github.com/logV/sybil


inserting records
-----------------

    # import from a file (one record per line)
    sybil ingest -table my_table < record.json

    # import from a mongo DB, making sure to exclude the _id column
    mongoexport -collection my_collection | sybil ingest -table my_table -exclude _id

    # import from a CSV file
    sybil ingest -csv -table my_csv_table < some_csv.csv

    # check out the db file structure
    ls -R db/


collating records
-----------------

    # turn the ingest log into column store
    sybil digest -table my_collection


querying records
----------------

    # list tables
    sybil query -tables

    # query that table
    sybil query -table my_table -info
    sybil query -table my_table -print

    # run a more complicated query (get status, host and histogram of pings)
    sybil query -table uptime -group status,host -int ping -print -op hist

    # make that a time series JSON blob
    sybil query -table uptime -group status,host -int ping -json -op hist -time

    # filter the previous query to samples that with host ~= mysite.*
    sybil query -table uptime -group status,host -int ping -json -op hist -time -str-filter host:re:mysite.*

additional information
----------------------

* [want to contribute?](http://github.com/logv/sybil/wiki/Contributing)
* [notes on performance](http://github.com/logv/sybil/wiki/Performance)
* [abadi survey of column stores](http://db.csail.mit.edu/pubs/abadi-column-stores.pdf)
