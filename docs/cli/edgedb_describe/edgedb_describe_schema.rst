.. _ref_cli_edgedb_describe_schema:


======================
edgedb describe schema
======================

Give an :ref:`SDL <ref_eql_sdl>` description of the schema of the
database specified by the connection options.

.. cli:synopsis::

    edgedb [<connection-option>...] describe schema


Description
===========

``edgedb describe schema`` is a terminal command equivalent to
:eql:stmt:`DESCRIBE SCHEMA AS SDL <DESCRIBE>` introspection command.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``describe`` command
    runs in the database it is connected to.
