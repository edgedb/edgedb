.. _ref_cli_gel_database_drop:


=================
gel database drop
=================

.. warning::

    This command is deprecated in |Gel|.
    Use :ref:`ref_cli_gel_branch_drop` instead.

Drop a :ref:`database <ref_datamodel_databases>`.

.. cli:synopsis::

    gel database drop [<options>] <name>

.. note::

    |EdgeDB| 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. This command works on instances running versions
    prior to |EdgeDB| 5.0. If you are running a newer version of
    Gel, you will instead use :ref:`ref_cli_gel_branch_drop`.


Description
===========

:gelcmd:`database drop` is a terminal command equivalent to
:eql:stmt:`drop database`.


Options
=======

The ``database drop`` command runs in the Gel instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<name>`
    The name of the database to drop.
:cli:synopsis:`--non-interactive`
    Drop the database without asking for confirmation.
