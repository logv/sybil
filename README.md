Sybil is a command line write once analytics database with no up front schema;
Written in Go, it's designed for fast ad-hoc analysis of heterogeneous data

what sybil does
-------------


Sybil has two parts: the ingestion / digestion system and the query engine

During the ingestion (and digestion) phase, Sybil reads records of data
containing strings, integers and sets off stdin, then collates and writes the
records into blocks (directories) of columns (files) on disk. 

Once records are on disk, the query engine lets you compose and run queries on
the saved blocks for fast and iterative analysis. The main query operations
supported are filtering, grouping and aggregating.

Sybil can be used by itself, but it really works well as part of a real-time
analytic pipeline. In general, Sybil is a good place to store transient,
ephemeral or meta data


features sybil is lacking
--------------------------

* Sybil does not support DELETE, JOIN or UPDATEs
* Sybil is meant to be used for event data or instrumentation: giving the ability
  to quickly and iteratively analyze data, as such, it's a supplement to other DBs
* Sybil is not a server


cool parts of sybil
-------------------

* it runs on a single machine - no cluster headaches. you use the command line
  program to ingest new records and issue queries on data in a local dir
* schemas are dynamic: you ingest records into any DB with mixed schema and
  issue queries on them. No table creation, no schema design. Just throw data
  at it and query. If you were using NoSQL for analytics previously, sybil is
  probably for you
* it's fast & multi-threaded; table scans and aggregations are done in parallel
* it's a command line program, not a server - memory is returned to the OS as
  soon as each query is done. A block by block execution model releases memory
  to the OS as soon as each block is finished processing, meaning memory stays
  under control
* built in support for 3 query types: rollups (aka group by with aggregates),
  time series (everyone loves time series) and raw sample queries
* full histograms and outliers can be calculated for any rollup



running
-------

    make

    # add some data to uptime and people tables
    make fake-data

    # run our first query on the table
    ./bin/sybil query -table uptime -samples -limit 5

    # run a more complicated query (get status, host and histogram of pings)
    ./bin/sybil query -table uptime -group status,host -int ping -print -op hist

    # run another query
    ./bin/sybil query -table people -int age -group state -print -limit 10 -sort age

    # use the writer to load a single JSON record into the ingestion log
    # use -ints to cast strings (in JSON records) as int columns
    # use -exclude to exclude columns from being ingested
    ./bin/sybil ingest -table test1 < example/single_record.json

    # turn it into the column store format
    ./bin/sybil digest -table test1

    # importing a mongo collection (can take a while...)
    ./bin/sybil ingest -table my_test_db --exclude a_nested_key < mongoexport -db my_test -collection test_collection


profiling
---------

    make profile

    # ADD PROFILE FLAG
    ./bin/sybil query -profile -table test0 -group age -int age
    go tool pprof ./bin/sybil cpu.pprof

    python scripts/fakedata/host_generator.py 10000 | ./bin/sybil ingest -profile -table test0
    go tool pprof ./bin/sybil cpu.pprof

    # PROFILE MEMORY
    ./bin/sybil query -profile -table test0 -group age -int age -mem
    go tool pprof ./bin/sybil mem.pprof


more info on column stores
--------------------------

Organization by column allows for loading columns only when necessary , as well
as per column compression optimizations. If your data never needs to be updated
(it's event data or log store, for example) - a column store can save space
over traditional data stores, as well as run faster queries

more info on column stores: http://db.csail.mit.edu/pubs/abadi-column-stores.pdf

