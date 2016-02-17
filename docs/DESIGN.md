Sybil has two main parts:

* the storage engine
  * the ingestion phase (writes data from stdin into the DB as rows)
  * the digestion phase (collates records into blocks of columns)
* and the query engine (reads blocks from row and column store)

storage engine
==============


Sybil ingests data and creates records via a command line API and Stdin. 

These new records are stored as rows on disk until a digestion is initiated, at
which point all records in the row store are collated into column form.

In Column form, records are stored in large blocks of column values. Depending
on the column type and the cardinality of the column, the column can be stored
as a bucketed value set (for low cardinality values) or as an array of values
(for high cardinality columns: e.g. time column). When stored in bucket form,
the record IDs are delta encoded

In Row and Column form, string values are actually stored as int32 values and a
separate StringTable is kept alongside each file.

NOTE: All data is stored using the `encoding/gob` module.


supported
---------

* str, int64, set fields
* column / row form

to be implemented
-----------------

* delete blocks older than
* compressed column info


multiple process file safety
----------------------------

In order for the DB to be safe for multiple processes to read and write, 'lock
files' are used to prevent files from being changed while another process is
reading them. There are 3 main locks in a given DB:

* table info lock: used any time a table is read
* table digestion lock: used to prevent multiple 'digestion' processes from running at once
* block specific lock: used when collating a block in the digestion process

If any of these locks are taken while a process tries to use that resource, the
the command will fail. This has little to no effect on digestion or query
calls, but can potentially lead to lost samples if the ingest command fails.


query engine
============

The query engine (obviously) runs queries on data. It loads both the row blocks
and column blocks off disk to execute queries. Blocks are loaded in parallel
via go-routines and then aggregated and freed, stopping every so often to let a
GC happen.

Typical query execution should look familiar:

    create LoadSpec & QuerySpec
    for block in blocks:
      * test the block extrema against filters to see if it can be skipped
      * allocate records and load pertinent columns off disk using the LoadSpec
      * assemble columns into record form inside the block
      * filter the records in the block
      * group and aggregate the records into per block results
      * remove the block from the table's allocated block list (letting itself be freed at the next GC)

    combine and rollup block results into master result
    print results



supported
---------

* group by
* group by time
* percentiles
* avg / min / max
* insert
* int filters
* str filters
* set filters


 
unsupported
-----------

* update
* join
* delete
* sql queries
* acid



bottlenecks
-----------

in the above execution model there are several performance bottlenecks:

* The memory allocation
* loading data off disk (since the data is encoded using `encoding/gob` it has a Reflection penalty)
* The allocation and assignment of record values
* filtering and aggregating results

In a full table scan of 8.5 million records, an example breakdown of timing would look like: 

    300ms allocate 8.5 mil records
    200ms load a column of low cardinality data off disk
    400ms load a column of high cardinality data off disk
    700ms scan & group full table results
    100ms additional aggregation time per int column

the execution time would roughly be 1,800ms to do a group by + aggregate.
Since the execution model is parallelized, block by block, the actual execution
time can be spread across multiple cores and across 4 cores executes in 780 -
820ms.

to be implemented
-----------------

* AdHoc Column definitions
* Join table with Updateable info (use SQLite or something for it?)
* Sessionization: (using a single join key) 
  * aggregation
  * filtering using event ordering
* MapReduce execution model of custom code with an embedded engine)

