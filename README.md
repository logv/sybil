PCS is a write once analytics backend designed for fast ad-hoc analysis of
heterogenous data.

what pcs does
-------------

PCS ingests records containing strings and integers and save large blocks of
these records (65Kish) into column based files - 1 file per column per block.
This allows for partial loading and analysis of data off disk, as well as fast
aggregation operations of many records at a time.

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

