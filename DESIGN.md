design
------

PCS consists of three main parts:

* the ingestion binary (writes blocks in row form)
* the digestion binary (writes blocks in column form from row form)
* and the query binary (reads blocks in column form)

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
* set fields


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

*  MapReduce execution model
