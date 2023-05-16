.. _ref_cli_edgedb_instance_create:


======================
edgedb instance create
======================

Initialize a new EdgeDB instance.

.. cli:synopsis::

     edgedb instance create [<options>] <name>


Description
===========

``edgedb instance create`` is a terminal command for making a new EdgeDB
instance and creating a corresponding credentials file in
``<edgedb_config_dir>/credentials``. Run ``edgedb info`` to see the path to
``<edgedb_config_dir>`` on your machine.


EdgeDB Cloud
------------

.. TODO: Cloud release
.. Update this after Cloud has released

Users with access to the EdgeDB Cloud beta may use this command to create a
Cloud instance after logging in using :ref:`ref_cli_edgedb_cloud_login`.

To create a Cloud instance, your instance name should be in the format
``<github-username>/<instance-name>``. Cloud instance names may contain
alphanumeric characters and hyphens (i.e., ``-``).


Options
=======

:cli:synopsis:`<name>`
    The new EdgeDB instance name.

:cli:synopsis:`--nightly`
    Use the nightly server for this instance.

:cli:synopsis:`--default-database=<default-database>`
    Specifies the default database name (created during
    initialization, and saved in credentials file). Defaults to
    ``edgedb``.

:cli:synopsis:`--default-user=<default-user>`
    Specifies the default user name (created during initialization,
    and saved in credentials file). Defaults to: ``edgedb``.

:cli:synopsis:`--port=<port>`
    Specifies which port should the instance be configured on. By
    default a random port will be used and recorded in the credentials
    file.

:cli:synopsis:`--start-conf=<start-conf>`
    Configures how the new instance should start: ``auto`` for
    automatic start with the system or user session, ``manual`` to
    turn that off so that the instance can be manually started with
    :ref:`ref_cli_edgedb_instance_start` on demand. Defaults to:
    ``auto``.

:cli:synopsis:`--version=<version>`
    Specifies the version of the EdgeDB server to be used to run the
    new instance. To list the currently available options use
    :ref:`ref_cli_edgedb_server_list_versions`.

    By default, when you specify a version, the CLI will use the latest release
    in the major version specified. This command, for example, will install the
    latest 2.x release:

    .. code-block:: bash

        $ edgedb instance create --version 2.6 demo26

    You may pin to a specific version by prepending the version number with an
    equals sign. This command will install version 2.6:

    .. code-block:: bash

        $ edgedb instance create --version =2.6 demo26

    .. note::

        Some shells like ZSH may require you to escape the equals sign (e.g.,
        ``\=2.6``) or quote the version string (e.g., ``"=2.6"``).
