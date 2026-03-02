# Tuples

A tuplespace-like store leveraging FoundationDB.

Unlike a real tuplespace:

* tuples are persistent, write-once
* triggers are run only when new tuples are added
* we support schemas for tuples (which makes writing triggers more reliable)

Trigger searches are optimized using a decision tree.


