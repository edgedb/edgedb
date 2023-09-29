.. _ref_cli_edgedb_project_info:


===================
edgedb project info
===================

Display various metadata about the project.

.. cli:synopsis::

    edgedb project info [OPTIONS]


Description
============

This command provides information about the project instance, such as
name and the project path.

.. note::

    The ``edgedb project info`` command is not intended for use with production
    instances.


Options
=======

:cli:synopsis:`--instance-name`
    Display only the instance name.

:cli:synopsis:`-j, --json`
    Output in JSON format.

:cli:synopsis:`--project-dir=<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.
