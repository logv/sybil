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
    x Int: gt, lt, !eq, ne
    x Str: !eq, ne, re, nre
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

  
  * test for weighting columns / sample count
  * tests for printing JSON
  * int64 bit integers stay from ingestion -> query
  * time queries work: test each bucket looks reasonable
  * sets exist and can be queried
  * different types of columns can be packed and unpacked
    * bucket encoded
    * delta encoded
    * serialized array


Investigate
-----------

Why does testing feel clunky? How to make it feel 'smooth' to write tests?
