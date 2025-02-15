.. _ref_cli_gel_project_unlink:


==================
gel project unlink
==================

Remove association with and optionally destroy the linked |Gel|
instance.

.. cli:synopsis::

    gel project unlink [<options>]


Description
===========

This command unlinks the project directory from the instance. By
default the |Gel| instance remains untouched, but it can also be
destroyed with an explicit option.


Options
=======

:cli:synopsis:`-D, --destroy-server-instance`
    If specified, the associated |Gel| instance is destroyed by
    running :ref:`ref_cli_gel_instance_destroy`.

:cli:synopsis:`--non-interactive`
    Do not prompts user for input.

:cli:synopsis:`--project-dir=<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.
