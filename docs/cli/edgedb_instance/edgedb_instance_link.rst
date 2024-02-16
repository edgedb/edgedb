.. _ref_cli_edgedb_instance_link:

====================
edgedb instance link
====================

Authenticate a connection to a remote EdgeDB instance and assign an
instance name to simplify future connections.

.. cli:synopsis::

    edgedb instance link [<options>] <name>


Description
===========

``edgedb instance link`` is a terminal command used to bind a set of
connection credentials to an instance name. This is typically used as
a way to simplify connecting to remote EdgeDB database instances.
Usually there's no need to do this for local instances as
:ref:`ref_cli_edgedb_project_init` will already set up a named
instance.

.. note::

    Unlike other ``edgedb instance`` sub-commands, ``edgedb instance link`` is
    recommended to link self-hosted instances. This can make other operations
    like migrations, dumps, and restores more convenient.

    Linking is not required for EdgeDB Cloud instances. They can always be
    accessed via CLI using ``<org-name>/<instance-name>``.

Options
=======

The ``instance link`` command uses the standard :ref:`connection
options <ref_cli_edgedb_connopts>` for specifying the instance to be
linked.

:cli:synopsis:`<name>`
    Specifies a new instance name to associate with the connection
    options. If not present, the interactive mode will ask for the
    name.

:cli:synopsis:`--non-interactive`
    Run in non-interactive mode (accepting all defaults).

:cli:synopsis:`--quiet`
    Reduce command verbosity.

:cli:synopsis:`--trust-tls-cert`
    Trust peer certificate.

:cli:synopsis:`--overwrite`
    Overwrite existing credential file if any.
