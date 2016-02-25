sybil is a write once analytics datastore with no up front schema requirements;
just log JSON records to a table and run queries. written in Go, sybil is
designed for fast ad-hoc analysis of multi-dimensional data

installation
------------

    go get github.com/logV/sybil

usage
-----


    # import from a file (one record per line)
    sybil ingest -table my_table < record.json

    # query that table
    sybil query -table my_table -info
    sybil query -table my_table -print

    # import from a mongo DB
    mongoexport -collection my_collection | sybil ingest -table my_table

    # import from a CSV file
    sybil ingest -csv -table my_csv_table < some_csv.csv

