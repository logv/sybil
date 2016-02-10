

PCS is a write once analytics backend designed for fast ad-hoc analysis of
heterogenous data. (it is multi-threaded! but not multi-machine)


DESIGN
------

PCS consists of three main parts:

* the ingestion binary (writes blocks in row form)
* the digestion binary (writes blocks in column form from row form)
* and the query binary (reads blocks in column form)


WHAT PCS DOES
-------------

PCS ingests records containing strings and integers and save large blocks of
these records (65Kish) into column based files - 1 file per column per block.
This allows for partial loading and analysis of data off disk, as well as fast
aggregation operations of many records at a time.

GOALS / DECISIONS
-----------------

* Fast to read, Slow to write
* Everyone loves Percentiles
* Max out all the CPUs
* Full table scans aren't a bad thing


SUPPORTED
---------

* GROUP BY
* GROUP BY TIME
* PERCENTILES
* AVG / MIN / MAX
* INSERT
* INT FILTERS
* STR FILTERS


TO BE IMPLEMENTED
-----------------

* Array / Set fields
* AdHoc Column definitions
* Sessionization: (using a single join key) 
  * aggregation
  * filtering using event ordering

 
UNSUPPORTED
-----------

* UPDATE
* JOIN
* DELETE
* SQL Queries
* ACID


WISHLIST / TBD
--------------

*  MapReduce execution model

RUNNING
-------

    make

    # add some data to uptime and people tables
    make fake-data

    # run our first query on the table
    ./bin/query -table uptime -samples -limit 5

    # run a more complicated query (get status, host and histogram of pings)
    ./bin/query -table uptime -group status,host -int ping -print -op hist

    # run another query
    ./bin/query -table people -int age -group state -print -limit 10 -sort age

    # use the writer to load a single JSON record into the ingestion log
    ./bin/ingest -table test1 < example/single_record.json

    # turn it into the column store format
    ./bin/digest -table test1


PROFILING
---------

    make profile

    # ADD PROFILE FLAG
    ./bin/query -profile -table test0 -group age -int age
    go tool pprof ./bin/query cpu.pprof

    python scripts/fakedata/host_generator.py 10000 | ./bin/ingest -profile -table test0
    go tool pprof ./bin/fakedata cpu.pprof

    # PROFILE MEMORY
    ./bin/query -profile -table test0 -group age -int age -mem
    go tool pprof ./bin/query mem.pprof

