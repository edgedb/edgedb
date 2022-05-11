.. eql:section-intro-page:: introduction

Introduction
============

EdgeDB is a next-generation `graph-relational database
</blog/the-graph-relational-database-defined>`_ designed as a spiritual
successor to the relational database.

It inherits the strengths of SQL databases: type safety, performance,
reliability, and transactionality. But instead of modeling data in a
relational (tabular) way, EdgeDB represents data with *object types*
containing *properties* and *links* to other objects. It leverages
this object-oriented model to provide a superpowered query language that
solves some of SQL's biggest usability problems.

How to read the docs
^^^^^^^^^^^^^^^^^^^^

EdgeDB is a complex system, but we've structured the documentation so you can
learn it in "phases". You only need to learn as much as you need to start
building your application.

- **Quickstart** —
  Go through the :ref:`quickstart <ref_quickstart>`. It walks
  through EdgeDB's core workflows: how to install EdgeDB, create an instance,
  write a simple schema, execute a migration, and write some simple queries.

- **Showcases** —
  For a speedrun of EdgeDB's features and syntax, refer to the Showcase pages
  for `Data Modeling </showcase/data-modeling>`_ and
  `EdgeQL </showcase/edgeql>`_. These pages each contain a set of annotated
  examples for all major features; it's a fast, practical way to hit the
  ground running with EdgeDB.

- **Documentation** —
  For a more in-depth understanding of EdgeDB's type system, concepts, and
  advanced features, go through the :ref:`Schema <ref_datamodel_index>`
  and :ref:`EdgeQL <ref_edgeql>` docs in order. The pages are carefully
  structured to teach EdgeDB in a linear way.

- **Reference** —
  As you move forward, more advanced or obscure questions are likely answered
  in the encylopedic :ref:`Standard Library <ref_std>` and :ref:`Reference
  <ref_reference_index>` sections. The Standard Library
  section documents all types, functions, and operators. The Reference
  section contains formal SDL and EdgeQL syntax breakdowns and reference
  information on our dump format, binary protocol, configuration settings,
  and more.


Tooling
^^^^^^^

To actually build apps with EdgeDB, you'll need to know more than SDL and
EdgeQL.

- **CLI** —
  The most commonly used CLI functionality is covered in the :ref:`Quickstart
  <ref_quickstart>`. For additional details, we have dedicated guides for
  :ref:`Migrations <ref_guide_migrations>` and :ref:`Projects
  <ref_guide_using_projects>`. A full CLI reference is available under
  :ref:`CLI <ref_cli_overview>`.

- **Client Libraries** —
  To actually execute queries, you'll use one of our client libraries for
  JavaScript, Go, or Python; find your preferred library under :ref:`Client
  Libraries <ref_clients_index>`. If you're using another language, you can
  still use EdgeDB! You can execute :ref:`queries via HTTP <ref_edgeql_http>`.
- **Deployment** —
  To publish an EdgeDB-backed application, you'll need to deploy EdgeDB. Refer
  to :ref:`Guides > Deployment <ref_guide_deployment>` for step-by-step
  deployment guides for all major cloud hosting platforms, as well as
  instructions for self-hosting with Docker.

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
