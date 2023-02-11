.. _ref_cli_edgedb_project_init:


===================
edgedb project init
===================

Setup a new project.

.. cli:synopsis::

    edgedb project init [<options>]


Description
===========

This command sets up a new project, creating an instance and a schema
directory for it. It can also be used to convert an existing directory
to a project directory, connecting the existing instance to the
project. Typically this tool will prompt for specific details about
how the project should be setup.


Options
=======

:cli:synopsis:`--link`
    Specifies whether the existing EdgeDB server instance should be
    linked with the project.

    This option is useful for initializing a copy of a project freshly
    downloaded from a repository with a pre-existing project database.

:cli:synopsis:`--no-migrations`
    Skip running migrations.

    There are two main use cases for this option:

    1. With :cli:synopsis:`--link` option to connect to a datastore
       with existing data.
    2. To initialize a new instance but then restore dump to it.

:cli:synopsis:`--non-interactive`
    Run in non-interactive mode (accepting all defaults).

:cli:synopsis:`--project-dir=<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.

:cli:synopsis:`--server-instance=<server-instance>`
    Specifies the EdgeDB server instance to be associated with the
    project.

:cli:synopsis:`--server-version=<server-version>`
    Specifies the EdgeDB server instance to be associated with the
    project.
