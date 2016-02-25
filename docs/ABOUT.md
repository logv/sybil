how sybil works
---------------

Sybil has two main parts:

* the storage engine, which consists of:
  * the ingestion phase (writes data from stdin into the DB as rows)
  * the digestion phase (collates records into blocks of columns)
* and the query engine (reads blocks from row and column store)

During the ingestion and digestion phases, Sybil reads records of data
containing strings, integers and sets off stdin (ingestion phase), then
collates (digestion phase) and writes the records into blocks (directories) of
columns (files) on disk. 

Once records are on disk, the query engine lets you compose and run queries on
the saved blocks for fast and iterative analysis. The main query operations
supported are filtering, grouping and aggregating.

Sybil can be used by itself, but it really works well as part of a real-time
analytic pipeline. In general, Sybil is a good place to store transient,
ephemeral or meta data - as such, it's a supplement to traditional DBs

cool parts of sybil
-------------------

* sybil is fast!
* sybil runs on a single machine; no cluster headaches. 
* schemas are dynamic: No table creation, no schema design. Just throw data in
  a table and query. If you were using NoSQL for analytics previously, sybil is
  probably for you
* support for 3 query types: rollups (aka group by with aggregates),
  time series (everyone loves time series) and raw sample queries
* full histograms and outliers can be calculated for any rollup
* did i say sybil is fast? cause it's multi-threaded, too; table scans and
  aggregations are done in parallel
* it's a command line program, not a server - memory is returned to the OS as
  soon as each query is done
* a block by block execution model releases memory to the OS as soon as each
  block is finished processing, meaning memory stays under control


features sybil is lacking
--------------------------

* Sybil does not support DELETE, JOIN or UPDATEs
* No ACID model or transaction log

differences from other DBs
--------------------------

* the data stored in sybil is write once: once data is inside sybil, it can only really be deleted - not updated
* queries and table scans are done in parallel, instead of one thread per query
* sybil is serverless - queries and ingestions are run through a binary that exits when its done
* sybil doesn't support JOINs or custom SQL aggregations (yet) - instead, it supports a subset of useful queries for analytic purposes
* sybil can perform time series analysis, but it does full aggregations (no
  caching!) of records of raw data - it doesn't store roll ups or minute by
  minute data, meaning that you can re-arrange queries and dimensions on the
  fly

more info on column stores
--------------------------

Organization by column allows for loading columns only when necessary , as well
as per column compression optimizations. If your data never needs to be updated
(it's event data or log store, for example) - a column store can save space
over traditional data stores, as well as run faster queries

more info on column stores: http://db.csail.mit.edu/pubs/abadi-column-stores.pdf

