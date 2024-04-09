.. _ref_cli_edgedb_instance_create:


======================
edgedb instance create
======================

Initialize a new EdgeDB instance.

.. cli:synopsis::

     edgedb instance create [<options>] [<name>] [<default-branch-or-database>]


Description
===========

``edgedb instance create`` is a terminal command for making a new EdgeDB
instance and creating a corresponding credentials file in
``<edgedb_config_dir>/credentials``. Run ``edgedb info`` to see the path to
``<edgedb_config_dir>`` on your machine.

.. note::

    The ``edgedb instance create`` command is not intended for use with
    self-hosted instances. You can follow one of our :ref:`deployment guides
    <ref_guide_deployment>` for information on how to create one of these
    instances.


EdgeDB Cloud
------------

.. note::

    Creating a Cloud instance requires CLI version 3.0 or later.

EdgeDB Cloud users may use this command to create a Cloud instance after
logging in using :ref:`ref_cli_edgedb_cloud_login`.

To create a Cloud instance, your instance name should be in the format
``<org-name>/<instance-name>``. Cloud instance names may contain alphanumeric
characters and hyphens (i.e., ``-``).

.. note::

    Please be aware of the following restrictions on EdgeDB Cloud instance
    names:

    * can contain only Latin alpha-numeric characters or ``-``
    * cannot start with a dash (``-``) or contain double dashes (``--``)
    * maximum instance name length is 61 characters minus the length of your
      organization name (i.e., length of organization name + length of instance
      name must be fewer than 62 characters)


Options
=======

:cli:synopsis:`<name>`
    The new EdgeDB instance name. Asked interactively if not specified.

:cli:synopsis:`<branch-or-database-name>`
    The default branch (or database pre-v5) name on the new instance. Defaults
    to ``main`` or, when creating a pre-v5 instance, ``edgedb``.

:cli:synopsis:`--nightly`
    Use the nightly server for this instance.

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

:cli:synopsis:`--channel=<channel>`
    Indicate the channel of the new instance. Possible values are ``stable``,
    ``testing``, or ``nightly``.

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

EdgeDB Cloud options
--------------------

:cli:synopsis:`--region=<region>`
    The region in which to create the instance (for EdgeDB Cloud instances).
    Possible values are ``aws-us-west-2``, ``aws-us-east-2``, and
    ``aws-eu-west-1``.

:cli:synopsis:`--tier=<tier>`
    Cloud instance subscription tier for the new instance. Possible values are
    ``pro`` and ``free``.

:cli:synopsis:`--compute-size=<number>`
    The size of compute to be allocated for the EdgeDB Cloud instance (in
    Compute Units)

:cli:synopsis:`--storage-size=<GiB>`
    The size of storage to be allocated for the Cloud instance (in Gigabytes)
