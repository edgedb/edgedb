.. _ref_bindings_overview:

================
Client Libraries
================

EdgeDB client libraries are a bit higher level than usual database bindings.
In particular, they contain:

* Structured data retrieval
* Connection pooling
* Retrying of failed transactions and queries

Additionally, client libraries might provide:

* Code generation for type-safe database access
* Query builder

This is a **work-in-progress** reference for writing client libraries for
EdgeDB.

External Links:

* :ref:`Official Client Libraries <ref_clients_index>`
* :ref:`Binary protocol <ref_protocol_overview>`
* `RFC 1004`_ - Robust Client API

Contents:

.. toctree::
    :maxdepth: 3

    datetime


.. lint-off

.. _RFC 1004: https://github.com/edgedb/rfcs/blob/master/text/1004-transactions-api.rst

.. lint-on
