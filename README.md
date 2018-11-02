# ParGRES - [prototype] PostgreSQL extension for parallel query processing in shared-nothing architectures
## Introduction
This code devoted to demonstration of one approach [1] to parallel query execution.
In accordace to the approach, parallel DBMS uses PostgreSQL core as a container for tuples. PDBMS manages metadata about data distribution. Input query is transfered to the nodes contained some part of tuples of distributed relations, involved into the query. Correctness of JOIN, Aggregate and other operations is provided by a parallel plan generator. It inserts custom `exchange` nodes into the plan positions, that needs tuples shuffling between nodes.
## Authors
Andrey Lepikhov a.lepikhov@postgrespro.ru, Postgres Professional, Moscow, Russia
## Installation

## Links
Lepikhov A.V., Sokolinsky L.B. Query Processing in a DBMS for Cluster Systems // Programming and Computer Software. 2010. Vol. 36. No. 4. P. 205-215. DOI: 10.1134/S0361768810040031
