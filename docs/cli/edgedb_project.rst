.. _ref_cli_edgedb_project:


==============
edgedb project
==============

EdgeDB provides a way to quickly setup a project. This way the project
directory gets associated with a specific EdgeDB instance and thus
makes it the default instance to connect to. This is done by creating
an ``edgedb.toml`` file in the project directory.


edgedb project init
===================

Setup a new project.

.. cli:synopsis::

    edgedb project init [OPTIONS] [<project-dir>]


Description
-----------

This command sets up a new project, creating an instance and a schema
directory for it. It can also be used to convert an existing directory
to a project directory, connecting the existing instance to the
project. Typically this tool will prompt for specific details about
how the project should be setup.


Options
-------

:cli:synopsis:`<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.

:cli:synopsis:`--server-instance=<server-instance>`
    Specifies the EdgeDB server instance to be associated with the
    project.

:cli:synopsis:`--server-version=<server-version>`
    Specifies the EdgeDB server instance to be associated with the
    project.


edgedb project unlink
=====================

Remove association with and optionally destroy the linked EdgeDB
instance.

.. cli:synopsis::

    edgedb project unlink [OPTIONS] [<project-dir>]


Description
-----------

This command unlinks the project directory from the instance. By
default the EdgeDB instance remains untouched, but it can also be
destroyed with an explicit option.


Options
-------

:cli:synopsis:`<project-dir>`
    The project directory can be specified explicitly. Defaults to the
    current directory.

:cli:synopsis:`-D, --destroy-server-instance`
    If specified, the associated EdgeDB instance is destroyed by
    running ``edgedb server destroy``.
