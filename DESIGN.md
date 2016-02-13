design
------

Sybil consists of three parts:

* the ingestion phase (writes data from stdin into the DB as records)
* the digestion phase (collates records into blocks of columns)
* and the query phase (reads blocks from row and column store)

FEATURES
========

supported
---------

* group by
* group by time
* percentiles
* avg / min / max
* insert
* int filters
* str filters
* set fields & filters


to be implemented
-----------------

* AdHoc Column definitions
* Sessionization: (using a single join key) 
  * aggregation
  * filtering using event ordering

 
unsupported
-----------

* update
* join
* delete
* sql queries
* acid


wishlist / tbd
--------------

*  MapReduce execution model of custom code (embedded engine?)

