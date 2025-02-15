.. _ref_cli_gel_project_init:


================
gel project init
================

Setup a new project.

.. cli:synopsis::

    gel project init [<options>]


Description
===========

This command sets up a new project, creating an instance, a schema directory,
and an :ref:`gel.toml <ref_reference_gel_toml>` file. It can also be used
to convert an existing directory to a project directory, connecting the
existing instance to the project. Typically this tool will prompt for specific
details about how the project should be setup.


Gel Cloud
---------

|Gel| Cloud users may use this command to create a Cloud instance after
logging in using :ref:`ref_cli_gel_cloud_login`.

To create a Cloud instance, your instance name should be in the format
``<org-name>/<instance-name>``. Cloud instance names may contain alphanumeric
characters and hyphens (i.e., ``-``). You can provide this Cloud instance name
through the interactive project initiation by running :gelcmd:`project init`
or by providing it via the ``--server-instance`` option.

.. note::

    Please be aware of the following restrictions on |Gel| Cloud instance
    names:

    * can contain only Latin alpha-numeric characters or ``-``
    * cannot start with a dash (``-``) or contain double dashes (``--``)
    * maximum instance name length is 61 characters minus the length of your
      organization name (i.e., length of organization name + length of instance
      name must be fewer than 62 characters)


Options
=======

:cli:synopsis:`--link`
    Specifies whether the existing |Gel| server instance should be
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
    Specifies the |Gel| server instance to be associated with the
    project.

:cli:synopsis:`--server-version=<server-version>`
    Specifies the Gel server instance to be associated with the
    project.

    By default, when you specify a version, the CLI will use the latest release
    in the major version specified. This command, for example, will install the
    latest 6.x release:

    .. code-block:: bash

        $ gel project init --server-version 6.1

    You may pin to a specific version by prepending the version number with an
    equals sign. This command will install version 6.1:

    .. code-block:: bash

        $ gel project init --server-version =6.1

    .. note::

        Some shells like ZSH may require you to escape the equals sign (e.g.,
        ``\=6.1``) or quote the version string (e.g., ``"=6.1"``).
