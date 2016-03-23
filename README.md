sybil is a write once analytics datastore with no up front schema requirements;
just log JSON records to a table and run queries. written in Go, sybil is
designed for fast ad-hoc analysis of multi-dimensional data

installation
============

    go get github.com/logV/sybil

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
