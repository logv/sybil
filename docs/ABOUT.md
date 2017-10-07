what is sybil?
---------------

Sybil is a simple append-only column based data store, meant for storing event
or log data.  Sybil has two main parts, the storage engine (built on
`encoding/gob`) and the query engine

Organization by column allows for loading columns only when necessary , as well
as per column compression optimizations. If your data never needs to be updated
(it's event data or log store, for example) - a column store can save space
over traditional data stores, as well as run faster queries.

Sybil can be used by itself, but it really works well as part of a real-time
analytic pipeline. In general, Sybil is a good place to store transient,
ephemeral or meta data - as such, it's a supplement to traditional DBs


references
----------

* http://db.csail.mit.edu/pubs/abadi-column-stores.pdf
* https://research.facebook.com/publications/scuba-diving-into-data-at-facebook/
* https://research.fb.com/wp-content/uploads/2016/11/fast-database-restarts-at-facebook.pdf
