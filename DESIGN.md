design
------

PCS consists of three main parts:

* the ingestion binary (writes blocks in row form)
* the digestion binary (writes blocks in column form from row form)
* and the query binary (reads blocks in column form)

In general, records are inserted through the ingestion binary into the row
store.  Every so often (30 seconds?), the digestion binary should be called to
split the records into columns and optimizes their space usage on disk based on
their cardinality.


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


to be implemented
-----------------

* Array / Set fields
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

*  MapReduce execution model
