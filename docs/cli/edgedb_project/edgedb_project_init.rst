.. _ref_cli_edgedb_project_init:


===================
edgedb project init
===================

Setup a new project.

.. cli:synopsis::

    edgedb project init [OPTIONS]


Description
===========

This command sets up a new project, creating an instance and a schema
directory for it. It can also be used to convert an existing directory
to a project directory, connecting the existing instance to the
project. Typically this tool will prompt for specific details about
how the project should be setup.


Options
=======

:cli:synopsis:`--project-dir=<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.

:cli:synopsis:`--server-install-method=<server-install-method>`
    Specifies which server should be used for this project: server
    installed via the local package system (``package``) or as a docker
    image (``docker``).

:cli:synopsis:`--server-instance=<server-instance>`
    Specifies the EdgeDB server instance to be associated with the
    project.

:cli:synopsis:`--server-version=<server-version>`
    Specifies the EdgeDB server instance to be associated with the
    project.
