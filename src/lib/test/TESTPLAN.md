Test Plan:
----------

  x Create a table
  * Write to row store
  * Load from row store
  * Querying loads from row store (if we want it to)
  * Reading info for DB
  x Write to column store
  x Load from column store
    x Verify ints, strs, sets
  * Filters:
    * Int: gt, lt, !eq, ne
    * Str: !eq, ne, re, nre
    * Set: in nin
    * Work with samples
  * Aggregation Line
    * Histograms
      * Outliers
    * Time Bucketing
    * Avgs
  * Group By
    * Strs
    * Ints
  * Order By
    
    
To Verify
---------


  * int64 bit integers stay from ingestion -> query
  * sets exist and can be queried
  * time queries work
  * JSON output
  * different types of columns can be packed and unpacked
    * bucket encoded
    * delta encoded
