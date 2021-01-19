.. eql:section-intro-page:: megaphone

Introduction
============

    The relational model is still the most effective method
    of representing data. And SQL as a declarative,
    storage-neutral query language is powerful and versatile.
    We don’t need to abandon either. Instead, we build on 
    what works in a language that affords more power to its 
    users, while being simpler and more consistent.

EdgeDB is a relational database that stores and describes the data
as strongly typed objects and relationships between them.

EdgeDB is built on top of PostgreSQL, inheriting all of its core
strengths: ACID compliance, performance, and reliability.
Building EdgeDB this way lets us offer something completely new
while maintaining decades of established practice in the areas 
where existing database software just works.

.. eql:react-element:: DocsNavTable

**What is EdgeDB?** It has:

.. class:: ticklist

- strict, strongly typed schema;
- powerful and clean query language;
- ability to easily work with complex hierarchical data;
- built-in support for schema migrations.

**What is EdgeDB not?**

EdgeDB is not a graph database: the data is stored and queried using
relational database techniques.  Unlike most graph databases, EdgeDB
maintains a strict schema.

EdgeDB is not a document database, but inserting and querying hierarchical
document-like data is trivial.

EdgeDB is not a traditional object database. Despite the classification,
it is not an implementation of OOP persistence.
