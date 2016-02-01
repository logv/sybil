ebc
--------

a column store + session analysis backend



RUNNING
-------

    make

    # add some data to test0 table
    make testdata

    # run our first query on the table
    ./bin/query -table test0

    # make some new tables / data and store them in column form
    ./bin/fakedata -add 100000 -table smalltable

    # query that small little table we just made
    ./bin/query -table smalltable -int age -group state -op hist

    # use the writer to load a single JSON record into the ingestion log
    ./bin/ingest -table test1 < example/single_record.json

    # use the digester to serialize columns into column store
    ./bin/digest -table test1



PROFILING
---------

    make profile

    # ADD PROFILE FLAG
    ./bin/query -profile -table test0 -group age -int age
    go tool pprof ./bin/query cpu.pprof


    ./bin/fakedata -profile -table test0 -add 10000
    go tool pprof ./bin/fakedata cpu.pprof

