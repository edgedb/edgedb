.. _ref_cli_edgedb_database_wipe:


====================
edgedb database wipe
====================

Destroy the contents of a :ref:`database <ref_datamodel_databases>`

.. cli:synopsis::

    edgedb database wipe [<options>]


Description
===========

``edgedb database wipe`` is a terminal command equivalent to
:eql:stmt:`reset schema to initial`.

The database wiped will be one of these values: the value passed for the
``--database``/``-d`` option, the value of ``EDGEDB_DATABASE``, or ``edgedb``.
The contents of the database will be destroyed and the schema reset to its
state before any migrations, but the database itself will be preserved.


Options
=======

The ``database wipe`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`--non-interactive`
    Destroy the data without asking for confirmation.
