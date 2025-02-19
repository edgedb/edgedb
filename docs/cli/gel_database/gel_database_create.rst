.. _ref_cli_gel_database_create:


===================
gel database create
===================

.. warning::

    This command is deprecated in |Gel|.
    Use :ref:`ref_cli_gel_branch_create` instead.

Create a new :ref:`database <ref_datamodel_databases>`.

.. cli:synopsis::

    gel database create [<options>] <name>

.. note::

    |EdgeDB| 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. This command works on instances running versions
    prior to |EdgeDB| 5.0. If you are running a newer version of
    Gel, you will instead use :ref:`ref_cli_gel_branch_create`.


Description
===========

:gelcmd:`database create` is a terminal command equivalent to
:eql:stmt:`create database`.


Options
=======

The ``database create`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<name>`
    The name of the new database.
