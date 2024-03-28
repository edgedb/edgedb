.. _ref_cli_edgedb_database_drop:


====================
edgedb database drop
====================

Drop a :ref:`database <ref_datamodel_databases>`.

.. cli:synopsis::

    edgedb database drop [<options>] <name>

.. note::

    EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. This command works on instances running versions
    prior to EdgeDB 5.0. If you are running a newer version of
    EdgeDB, you will instead use :ref:`ref_cli_edgedb_branch_drop`.


Description
===========

``edgedb database drop`` is a terminal command equivalent to
:eql:stmt:`drop database`.


Options
=======

The ``database drop`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the database to drop.
:cli:synopsis:`--non-interactive`
    Drop the database without asking for confirmation.
