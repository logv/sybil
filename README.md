ebc
--------

an aggregator pipeline for session analysis explorations

GOALS:

* prototype a filtering language for events
* have a DB that's fast enough for simple queries



OPEN QUESTIONS
--------------


* how to do a group by count?
* how long does loading records off disk take (per million)
* how long does grouping take (by cardinality per million)
* how long does hist vs avg take


PIPELINE FOR EVENTS
-------------------

* ingest event
* validate event data
* normalize event
* (save event data)



PIPELINE FOR REGULAR QUERY
--------------------------

* Load necessary columns off disk
* Filter columns
* Join columns into rows
* Group by and aggregate


PIPELINE FOR SESSION QUERY
--------------------------

* Pre-Session Filter (similar to regular query filter)
* Sessionization
* Post-Session Filter (uses order of events / existence of events)
* Session Aggregation / Summary
* Summary sort / filtering
