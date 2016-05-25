Sybil is a write once analytics datastore with no up front table schema requirements;
just log JSON records to a table and run queries. Written in Go, sybil is
designed for fast full table scans of multi-dimensional data on a single machine.

more documentation is available [on the wiki](http://github.com/logv/sybil/wiki)
and [in the repo](http://github.com/logv/sybil/blob/master/docs).

advantages
----------

  * Easy to setup and get data inside sybil - just pipe JSON on stdin to sybil
  * Supports histograms (and percentiles), standard deviations and time series roll ups
  * Runs fast full table queries ([performance notes](http://github.com/logv/sybil/wiki/Performance))
  * Lower disk usage through per column compression schemes
  * Serverless design with controlled memory usage
  * Per table retention policies (specify max age and/or size of tables)

disadvantages
-------------

  * JOINS not supported
  * No UPDATE operation on data - only writes
  * Sybil is meant for use on a single machine, no sharding

installation
------------

    go get github.com/logv/sybil


inserting records
-----------------

    # import from a file (one record per line)
    sybil ingest -table my_table < record.json

    # import from a mongo DB, making sure to exclude the _id column
    mongoexport -collection my_collection | sybil ingest -table my_table -exclude _id

    # import from a CSV file
    sybil ingest -csv -table my_csv_table < some_csv.csv

    # import samples from a subkey of an already existing JSON doc
    # the pathname here would be used for the following document:
    # {
    #    records: [ sample, sample, sample ]
    # }
    sybil ingest -table my_other_table -path "$.records" < my_json_doc.json

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


trimming old records
--------------------

    # list all blocks that won't fit in 100MB (give or take a few MB)
    sybil trim -table uptime -time-col time -mb 100

    # list all blocks that have no data newer than a week ago
    # TIP: other time durations can be used, like: day, month & year
    sybil trim -table uptime -time-col time -before `date --date "-1 week" +%s`

    # the above two trimming commands can be combined!
    # list all blocks that have no data newer than a week and fit within 100mb
    sybil trim -table uptime -time-col time -before `date --date "-1 week" +%s` -mb 100

    # delete the blocks that won't fit in memory
    # TIP: use -really flag if you don't want to be prompted (for use in scripts)
    sybil trim -table uptime -time-col time -mb 100 -delete



additional information
----------------------

* [want to contribute?](http://github.com/logv/sybil/wiki/Contributing)
* [notes on performance](http://github.com/logv/sybil/wiki/Performance)
* [abadi survey of column stores](http://db.csail.mit.edu/pubs/abadi-column-stores.pdf)
