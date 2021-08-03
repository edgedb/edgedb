.. _ref_cli_edgedb_instance_create:


======================
edgedb instance create
======================

Initialize a new EdgeDB instance.

.. cli:synopsis::

     edgedb instance create [OPTIONS] <name>


Description
===========

``edgedb instance create`` is a terminal command for making a new EdgeDB
instance and creating a corresponding credentials file in
``<edgedb_config_dir>/credentials``. Run ``edgedb info`` to see the path to
``<edgedb_config_dir>`` on your machine.


Options
=======

:cli:synopsis:`<name>`
    The new EdgeDB instance name.

:cli:synopsis:`-i, --interactive`
    Performs the installation in interactive mode, similar to how
    :ref:`downloading and installing <ref_cli_edgedb_install>` works.

:cli:synopsis:`--nightly`
    Use the nightly server for this instance.

:cli:synopsis:`--overwrite`
    Overwrite data directory and credential file if any of these
    exist. This is mainly useful for recovering from interrupted
    initializations.

:cli:synopsis:`--method=<method>`
    Specifies which EdgeDB server should be used to run the new
    instance: ``package`` or ``docker``. To list the currently
    available options use :ref:`ref_cli_edgedb_server_list_versions`.

:cli:synopsis:`--version=<version>`
    Specifies the version of the EdgeDB server to be used to run the
    new instance. To list the currently available options use
    :ref:`ref_cli_edgedb_server_list_versions`.

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
