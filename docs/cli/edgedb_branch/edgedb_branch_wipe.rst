.. _ref_cli_edgedb_branch_wipe:


==================
edgedb branch wipe
==================

Destroy the contents of a :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    edgedb branch wipe [<options>] <name>

.. note::

    This CLI command requires CLI version 4.3.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database_wipe` command to wipe a database,
    which branches replaced in EdgeDB 5.0.


Description
===========

The contents of the branch will be destroyed and the schema reset to its
state before any migrations, but the branch itself will be preserved.

``edgedb branch wipe`` is a terminal command equivalent to
:eql:stmt:`reset schema to initial`.


Options
=======

The ``branch wipe`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the branch to wipe.

:cli:synopsis:`--non-interactive`
    Destroy the data without asking for confirmation.
