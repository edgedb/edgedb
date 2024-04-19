.. _ref_cli_edgedb_database_create:


======================
edgedb database create
======================

Create a new :ref:`database <ref_datamodel_databases>`.

.. cli:synopsis::

    edgedb database create [<options>] <name>

.. note::

    EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. This command works on instances running versions
    prior to EdgeDB 5.0. If you are running a newer version of
    EdgeDB, you will instead use :ref:`ref_cli_edgedb_branch_create`.


Description
===========

``edgedb database create`` is a terminal command equivalent to
:eql:stmt:`create database`.


Options
=======

The ``database create`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the new database.
