.. _ref_cli_gel_database_wipe:


=================
gel database wipe
=================

.. warning::

    This command is deprecated in |Gel|.
    Use :ref:`ref_cli_gel_branch_wipe` instead.

Destroy the contents of a :ref:`database <ref_datamodel_databases>`

.. cli:synopsis::

    gel database wipe [<options>]

.. note::

    |EdgeDB| 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. This command works on instances running versions
    prior to |EdgeDB| 5.0. If you are running a newer version of
    |EdgeDB| or Gel, you will instead use :ref:`ref_cli_gel_branch_wipe`.


Description
===========

:gelcmd:`database wipe` is a terminal command equivalent to
:eql:stmt:`reset schema to initial`.

The database wiped will be one of these values: the value passed for the
``--database``/``-d`` option, the value of :gelenv:`DATABASE`, or |main|.
The contents of the database will be destroyed and the schema reset to its
state before any migrations, but the database itself will be preserved.


Options
=======

The ``database wipe`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`--non-interactive`
    Destroy the data without asking for confirmation.
