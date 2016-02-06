pcs
--------

a personal column store + session analysis backend



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

