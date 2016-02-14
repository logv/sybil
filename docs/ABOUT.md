how sybil works
---------------

Sybil has two parts: the ingestion / digestion system and the query engine

During the ingestion (and digestion) phase, Sybil reads records of data
containing strings, integers and sets off stdin, then collates and writes the
records into blocks (directories) of columns (files) on disk. 

Once records are on disk, the query engine lets you compose and run queries on
the saved blocks for fast and iterative analysis. The main query operations
supported are filtering, grouping and aggregating.

Sybil can be used by itself, but it really works well as part of a real-time
analytic pipeline. In general, Sybil is a good place to store transient,
ephemeral or meta data - as such, it's a supplement to traditional DBs


features sybil is lacking
--------------------------

* Sybil does not support DELETE, JOIN or UPDATEs
* Sybil is not a server


cool parts of sybil
-------------------

* sybil runs on a single machine; no cluster headaches. 
* schemas are dynamic: you ingest records into any DB with mixed schema and
  issue queries on them. No table creation, no schema design. Just throw data
  at it and query. If you were using NoSQL for analytics previously, sybil is
  probably for you
* it's fast & multi-threaded; table scans and aggregations are done in parallel
* it's a command line program, not a server - memory is returned to the OS as
  soon as each query is done. 
* A block by block execution model releases memory to the OS as soon as each
  block is finished processing, meaning memory stays under control
* support for 3 query types: rollups (aka group by with aggregates),
  time series (everyone loves time series) and raw sample queries
* full histograms and outliers can be calculated for any rollup



more info on column stores
--------------------------

Organization by column allows for loading columns only when necessary , as well
as per column compression optimizations. If your data never needs to be updated
(it's event data or log store, for example) - a column store can save space
over traditional data stores, as well as run faster queries

more info on column stores: http://db.csail.mit.edu/pubs/abadi-column-stores.pdf

