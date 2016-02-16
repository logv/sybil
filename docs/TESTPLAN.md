Test Plan:
----------

  x Create a table
  x Write to row store
  x Load from row store
  x Querying loads from row store (if we want it to)
  * Reading info for DB
  x Write to column store
  x Load from column store
    x Verify ints, strs, sets
  * Filters:
    x Int: gt, lt, !eq, ne
    x Str: !eq, ne, re, nre
    * Set: in nin
    * Work with samples
      * migrate f_SAMPLES into querySpec
  * Aggregation Line
    * Histograms
      * Outliers
    * Time Bucketing
    x Avgs
  x Group By (these are covered by aggregation tests)
    x Strs
    x Ints
  * Order By

Failure Plans
-------------

  * Mixed Key Tables
  * Table Corruption
  * Re-Ingestion
    
    
To Verify
---------

  * test blocks are properly skipped when aggregating with int filters
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
