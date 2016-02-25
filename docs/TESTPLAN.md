Test Plan:
----------

  * [x] Create a table 
  * [x] Write to row store 
  * [x] Load from row store 
  * [x] Querying loads from row store (if we want it to) 
  * [x] Reading info for DB
  * [x] Write to column store 
  * [x] Load from column store 
    * [x] Verify ints, strs, sets 
  * Filters:
    * [x] Int: gt, lt, !eq, ne 
    * [x] Str: !eq, ne, re, nre 
    * [o] Set: in nin 
    * [o]Work with samples
      * [o]migrate f_SAMPLES into querySpec
  * [x] Aggregation Line
    * [x] Histograms 
      * [o]Outliers
    * [o] Time Bucketing
    * [x] Avgs 
  * [x]Group By (these are covered by aggregation tests)
    * [x] Strs 
    * [x] Ints 
  * [o]Order By

Failure Plans
-------------

  * [o] Mixed Key Tables
  * [o] Table Corruption
  * [o] Re-Ingestion
    
    
Integration / E2E Tests
------------------

  * [x] stress test for multiple ingesters / digesters
  * [o] test blocks are properly skipped when aggregating with int filters
  * [o] test for weighting columns / sample count
  * [o] tests for printing JSON
  * [o] int64 bit integers stay from ingestion -> query
  * [o] time queries work: test each bucket looks reasonable
  *  [o] sets exist and can be queried 
  * [o] different types of columns can be packed and unpacked
    * [o] bucket encoded
    * [o] delta encoded
    * [o] serialized array
