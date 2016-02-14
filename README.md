sybil is a command line write once analytics database with no up front schema;
Written in Go, sybil is designed for fast ad-hoc analysis of heterogeneous data

installation
------------

    go get github.com/logV/sybil


usage
-----


    # import from a file
    sybil ingest -table my_table < record.json

    # query that file
    sybil query -table my_table -info
    sybil query -table my_table -print

    # import from a mongo DB
    mongoexport -collection my_collection | sybil ingest -table my_table

    # import from a CSV file
    sybil ingest -csv -table my_csv_table < some_csv.csv

