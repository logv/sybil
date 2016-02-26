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
    * [ ] Set: in nin
    * [ ] Work with samples
      * [ ] migrate f_SAMPLES into querySpec
  * [x] Aggregation Line
    * [x] Histograms
      * [ ] Outliers
    * [ ] Time Bucketing
    * [ ] Avgs
  * [x] Group By (these are covered by aggregation tests)
    * [x] Strs
    * [x] Ints
  * [ ] Order By
  * [ ] Digestion
    * [ ] Open Partial Blocks and re-fill them
    * [ ] Auto Digest during ingestion
    * [ ] Digestion can fail gracefully

Failure Plans
-------------

  * [ ] Mixed Key Tables
  * [ ] Table Corruption
  * [ ] Re-Ingestion


Integration / E2E Tests
------------------

  * [x] stress test for multiple ingesters / digesters
  * [ ] test blocks are properly skipped when aggregating with int filters
  * [ ] int64 bit integers stay from ingestion -> query
  * [ ] time queries work: test each bucket looks reasonable
  *  [ ] sets exist and can be queried
  * [ ] different types of columns can be packed and unpacked
    * [ ] bucket encoded
    * [ ] delta encoded
    * [ ] serialized array
