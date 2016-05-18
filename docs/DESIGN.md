storage engine
==============


When sybil ingests data via stdin, it creates new records that are stored as
rows on disk (one file per call to ingest) until a manual digestion is
initiated, at which point all records in the row store are collated into column
form.

In Column form, records are stored in blocks of column values. Each block has
an info file and one or more column files contained within.

Depending on the column type and the cardinality of the column, each column can
be stored as a bucketed value set (for low cardinality values) or as an array
of values (for high cardinality columns: e.g. time column). When stored in
bucket form, the record IDs are delta encoded. When stored in array form, the
values themselves are delta encoded.

In Row and Column form, string values are actually stored as int32 values and a
separate StringTable is kept alongside each file.

NOTE: All data is stored using the `encoding/gob` module.



multiple process file safety
----------------------------

In order for the DB to be safe for multiple processes to read and write, 'lock
files' are used to prevent files from being changed while another process is
reading them. There are 3 main locks in a given DB:

* table info lock: used (and held briefly) any time a table is read
* table digestion lock: used to prevent multiple 'digestion' processes from running at once
* block specific lock: used when collating a block in the digestion process

If any of these locks are taken while a process tries to use that resource, the
the command will fail. This has little to no effect on digestion or query
calls, but can potentially lead to lost samples if the ingest command fails.

When a lock is owned by a PID that is dead (because of program failure or other
reasons), sybil attempts an automatic recovery of the lock, based on its type.

To keep high performance, it's better to have only one process writing to sybil
to reduce lock contentions, but it's completely fine to have multiple writers.


query engine
============

The query engine runs queries on data. It loads both the row blocks
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
* single key joins



unsupported
-----------

* update
* delete
* sql queries
* acid



bottlenecks
-----------

in the query execution model there are several performance bottlenecks:

* memory allocation
* loading data off disk
* allocation and assignment of record values
* filtering and aggregating results

In a full table scan of 8.5 million records, an example breakdown of timing would look like:

    300ms allocate 8.5 mil records
    200ms load a column of low cardinality data off disk
    800ms scan & group full table results
    100ms additional aggregation time per int column

execution time would roughly be ~1,500ms to do a group by + aggregate with a
single core on a single int and single string column, but since the execution
model is parallelized, the execution time can be spread across multiple cores

to be implemented
-----------------

* AdHoc Column definitions
* MapReduce execution model of custom code with an embedded engine)


sessionization engine
=====================

to be documented
