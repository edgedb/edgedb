<p align="center">
  <a href="https://edgedb.com"><img width="160px" src="logo.svg"></a>
</p>

[![Build Status](https://travis-ci.com/edgedb/edgedb.svg?token=74UsunYVsEQ4qRAHz4Ny&branch=master)](https://travis-ci.com/edgedb/edgedb)


Disclaimer
==========

This is a very early technology preview.  It is not yet intended to be used
for mission-critical applications.  Things may not work, or not work as
expected.  Comments and bug reports are welcome.


What is EdgeDB?
===============

EdgeDB is an **open-source** object-relational database that helps you write
better software with less effort.  EdgeDB organizes data as a graph of
strongly-typed objects and provides an expressive query language which allows
to manipulate complex data hierarchies with ease.

EdgeDB features:

- strict, strongly typed schema;
- powerful and expressive query language;
- built-in support for schema migrations;
- native GraphQL support;
- PostgreSQL as the foundation.


What EdgeDB is not
------------------

EdgeDB is not a graph database: the data is stored and queried using
relational database techniques.  Unlike most graph databases, EdgeDB
maintains a strict schema.

EdgeDB is not a document database, but inserting and querying hierarchical
document-like data is trivial.

EdgeDB is not a traditional object database, despite the classification,
it is not an implementation of OOP persistence.


Getting Started
===============

If you are interested in trying EdgeDB, please refer to the
[Quickstart](https://edgedb.com/docs/quickstart) section of
the documentation.


Documentation
=============

The EdgeDB documentation can be found at
[edgedb.com/docs](https://edgedb.com/docs).


License
=======

The code in this repository is developed and distributed under the
Apache 2.0 license.  See [LICENSE](LICENSE) for details.
