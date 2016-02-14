running from source
-------------------

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

