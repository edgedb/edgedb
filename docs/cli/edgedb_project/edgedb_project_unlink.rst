.. _ref_cli_edgedb_project_unlink:


=====================
edgedb project unlink
=====================

Remove association with and optionally destroy the linked EdgeDB
instance.

.. cli:synopsis::

    edgedb project unlink [<options>]


Description
===========

This command unlinks the project directory from the instance. By
default the EdgeDB instance remains untouched, but it can also be
destroyed with an explicit option.


Options
=======

:cli:synopsis:`-D, --destroy-server-instance`
    If specified, the associated EdgeDB instance is destroyed by
    running :ref:`ref_cli_edgedb_instance_destroy`.

:cli:synopsis:`--non-interactive`
    Do not prompts user for input.

:cli:synopsis:`--project-dir=<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.
