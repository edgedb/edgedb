.. _ref_cli_edgedb_database_create:


======================
edgedb database create
======================

Create a new :ref:`database <ref_datamodel_databases>`.

.. cli:synopsis::

    edgedb database create [<options>] <name>


Description
===========

``edgedb database create`` is a terminal command equivalent to
:eql:stmt:`CREATE DATABASE`.


Options
=======

The ``database create`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the new database.
