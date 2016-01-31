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

    # make some new tables / data
    ./bin/fakedata -add 100000 -table smalltable

    ./bin/query -table smalltable -int age -group state -op hist

    # use the writer to load a single JSON record
    ./bin/writer < example/single_record.json



PROFILING
---------

    make profile

    # ADD PROFILE FLAG
    ./bin/query -profile -table test0 -group age -int age
    go tool pprof ./bin/query cpu.pprof


    ./bin/fakedata -profile -table test0 -add 10000
    go tool pprof ./bin/fakedata cpu.pprof

