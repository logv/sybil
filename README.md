PCS is a set of command line programs that encapsulate a write once analytics
db. PCS is designed for fast ad-hoc analysis of heterogenous data.

what pcs does
-------------

PCS reads records containing strings, integers and sets off stdin; PCS then
collates and writes the records into large blocks (aka Vectors) organized by
column into files on disk.

Organization by column allows for only loading columns necessary when running
queries, as well as per column compression optimizations. If your data never
needs to be updated (it's event data or log store, for example) - a column
store can save space over traditional data stores

more info on column stores: http://db.csail.mit.edu/pubs/abadi-column-stores.pdf


running
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
    # use -ints to cast strings (in JSON records) as int columns
    # use -exclude to exclude columns from being ingested
    ./bin/ingest -table test1 < example/single_record.json

    # turn it into the column store format
    ./bin/digest -table test1


profiling
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

