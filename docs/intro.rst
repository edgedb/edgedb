.. eql:section-intro-page:: introduction

Introduction
============

EdgeDB is a next-generation `graph-relational database
</blog/the-graph-relational-database-defined>`_.

It's designed to be a spiritual successor to the SQL-based relation database
and inherits all its core strengths: type safety, performance, reliability,
and ACID compliance. Beyond that, EdgeDB brings with it a more intuitive data
model based on *object types*, *properties*, and *links*, plus a superpowered
query language that solves SQL's biggest usability problems.

How to read the docs
^^^^^^^^^^^^^^^^^^^^

The sidebar is broken up into a number of main sections.

1. Go through the :ref:`quickstart <ref_quickstart>`. It is the simplest way
   to set up EdgeDB on your machine, learn the basics of schema, and write a
   couple queries.

2. Go through the :ref:`Schema <ref_datamodel_index>` docs in order, up to
   :ref:`Constraints <ref_datamodel_constraints>`. This will give you a full
   understanding of EdgeDB's type system and how to declare your schema. The
   last few pages (aliases, annotations, functions, etc) are more advanced; you
   can skip them for now.

3. Go through the :ref:`EdgeQL <ref_datamodel_index>` docs in order. This
   section assumes you have some familiarity with the Schema docs already. Each
   page is written as an accessible guide.   up to :ref:`Constraints
   <ref_datamodel_constraints>`. This will give you a full understanding of how
   to model data in EdgeDB. The last few pages (aliases, annotations,
   functions, etc) are more advanced; you can skip them for now.

4. At this point you'll have a very strong working knowledge of how to write
   schemas and queries with EdgeDB. To start building applications, you'll
   likely use one of our client libraries for JavaScript, Go, or Python; find
   your preferred library under :ref:`Client Libraries <ref_clients_index>`.
   You may instead prefer to use the GraphQL endpoint (documented in the
   :ref:`GraphQL <ref_graphql_index>` section) or execute :ref:`EdgeQL over
   HTTP <ref_edgeql_http>`.

5. As you move forward, more advanced or obscure questions are likely answered
   in the encylopedic :ref:`Reference <ref_reference_index>` and :ref:`Standard
   Library <ref_std>` sections. The Reference section contains formal SDL and
   EdgeQL syntax breakdowns and reference information on our dump format,
   binary protocol, configuration settings, and more. The Standard Library
   section documents all types, functions, and operators.



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


.. toctree::
    :maxdepth: 3
    :hidden:
