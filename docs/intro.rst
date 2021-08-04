.. eql:section-intro-page:: introduction

Introduction
============

EdgeDB is a relational database that stores and describes the data
as strongly typed objects and relationships between them.

EdgeDB is built on top of PostgreSQL, inheriting all its core strengths:
ACID compliance, performance, and reliability.

.. eql:react-element:: DocsNavTable

EdgeDB features:

.. class:: ticklist

- strict, strongly typed schema;
- powerful and clean query language;
- ability to easily work with complex hierarchical data;
- built-in support for schema migrations.

EdgeDB is not a graph database: the data is stored and queried using
relational database techniques.  Unlike most graph databases, EdgeDB
maintains a strict schema.

EdgeDB is not a document database, but inserting and querying hierarchical
document-like data is trivial.

EdgeDB is not a traditional object database, despite the classification,
it is not an implementation of OOP persistence.
