.. _ref_cli_edgedb:

======
edgedb
======

:edb-alt-title: edgedb — Interactive Shell

EdgeDB interactive shell:

.. cli:synopsis::

    edgedb [<connection-option>...]

It's also possible to run an EdgeQL script by piping it into the
EdgeDB shell. The shell will then run in non-interactive mode and
print all the responses into the standard output:

.. cli:synopsis::

    cat myscript.edgeql | edgedb [<connection-option>...]

The above command also works on PowerShell in Windows, while the classic
Windows Command Prompt uses a different command as shown below:

.. cli:synopsis::

    type myscript.edgeql | edgedb [<connection-option>...]

Description
===========

``edgedb`` is a terminal-based front-end to EdgeDB.  It allows running
queries and seeing results interactively.


Options
=======

:cli:synopsis:`-h, --help`
    Show help about the command and exit.

:cli:synopsis:`--help-connect`
    Show all available :ref:`connection options <ref_cli_edgedb_connopts>`

:cli:synopsis:`-V, --version`
    Print version.

:cli:synopsis:`--no-cli-update-check`
    Disable version check.

:cli:synopsis:`-I <name>, --instance=<name>`
    Specifies the named instance to connect to. The actual connection
    parameters are stored in ``<edgedb_config_dir>/credentials`` and are
    usually created by :ref:`ref_cli_edgedb_instance_create` or similar
    commands. Run ``edgedb info`` to see the location of
    ``<edgedb_config_dir>`` on your machine.

    This option overrides host and port.

:cli:synopsis:`--dsn=<dsn>`
    Specifies the DSN for EdgeDB to connect to.

    This option overrides all other options except password.

:cli:synopsis:`--credentials-file /path/to/file`
    Path to JSON file containing credentials.

:cli:synopsis:`-H <hostname>, --host=<hostname>`
    Specifies the host name of the machine on which the server is running.
    Defaults to the value of the ``EDGEDB_HOST`` environment variable.

:cli:synopsis:`-P <port>, --port=<port>`
    Specifies the TCP port on which the server is listening for connections.
    Defaults to the value of the ``EDGEDB_PORT`` environment variable or,
    if not set, to ``5656``.

:cli:synopsis:`-u <username>, --user=<username>`
    Connect to the database as the user :cli:synopsis:`<username>`.
    Defaults to the value of the ``EDGEDB_USER`` environment variable, or,
    if not set, to the login name of the current OS user.

:cli:synopsis:`-d <dbname>, --database=<dbname>`
    Specifies the name of the database to connect to. Default to the value
    of the ``EDGEDB_DATABASE`` environment variable, or, if not set, to
    the calculated value of :cli:synopsis:`<username>`.

:cli:synopsis:`-b <branch-name>, --branch=<branch-name>`
    Specifies the name of the branch to connect to. Default to the value
    of the ``EDGEDB_BRANCH`` environment variable, or, if not set, to
    the calculated value of :cli:synopsis:`<username>`.

    .. note::

        EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
        replace databases. This option requires CLI version 4.3.0 or later and
        EdgeDB version 5.0 or later. If you are running an earlier version of
        EdgeDB, you will instead use the ``-d <dbname>, --database=<dbname>``
        option above.

:cli:synopsis:`--password | --no-password`
    If :cli:synopsis:`--password` is specified, force ``edgedb`` to prompt
    for a password before connecting to the database.  This is usually not
    necessary, since ``edgedb`` will prompt for a password automatically
    if the server requires it.

    Specifying :cli:synopsis:`--no-password` disables all password prompts.

:cli:synopsis:`--password-from-stdin`
    Use the first line of standard input as the password.

:cli:synopsis:`--tls-ca-file /path/to/cert`
    Certificate to match server against.

    This might either be full self-signed server certificate or
    certificate authority (CA) certificate that server certificate is
    signed with.

:cli:synopsis:`--tls-security mode`
    Set the TLS security mode.

    ``default``
        Resolves to ``strict`` if no custom certificate is supplied via
        :cli:synopsis:`--tls-ca-file`, environment variable, etc. Otherwise,
        resolves to ``no_host_verification``.

    ``strict``
        Verify TLS certificate and hostname.

    ``no_host_verification``
        This allows using any certificate for any hostname. However,
        certificate must be present and match the root certificate specified
        with  :cli:synopsis:`--tls-ca-file`, credentials file, or system root
        certificates.

    ``insecure``
        Disable all TLS security measures.

:cli:synopsis:`--wait-until-available=<wait_time>`
    In case EdgeDB connection can't be established, keep retrying up
    to :cli:synopsis:`<wait_time>` (e.g. ``30s``).

:cli:synopsis:`--connect-timeout=<timeout>`
    Specifies a :cli:synopsis:`<timeout>` period. In case EdgeDB
    doesn't respond for this period the command will fail (or retry if
    :cli:synopsis:`--wait-until-available` is also specified). The
    :cli:synopsis:`<timeout>` value must be given using time units
    (e.g. ``hr``, ``min``, ``sec``, ``ms``, etc.). The default
    value is ``10s``.


Backslash Commands
==================

Introspection
-------------

The introspection commands share a few common options that are available to
many of the commands:

- ``-v``- Verbose
- ``-s``- Show system objects
- ``-c``- Case-sensitive pattern matching

:cli:synopsis:`\\d [-v] OBJECT-NAME, \\describe [-v] OBJECT-NAME`
  Describe schema object specified by *OBJECT-NAME*.

:cli:synopsis:`\\ds, \\d schema, \\describe schema`
  Describe the entire schema.

:cli:synopsis:`\\l`
  List branches on EdgeDB server 5+ or databases on prior versions.

:cli:synopsis:`\\list databases`
  List databases.

  .. note::

      EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to replace
      databases. If you are running 5.0 or later, you will instead use the
      ``\list branches`` command below.

:cli:synopsis:`\\list branches`
  List branches.

  .. note::

      EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to replace
      databases. This command requires CLI version 4.3.0 or later and EdgeDB
      version 5.0 or later. If you are running an earlier version of EdgeDB,
      you will instead use the ``\list databases`` command above.

:cli:synopsis:`\\ls [-sc] [PATTERN], \\list scalars [-sc] [PATTERN]`
  List scalar types.

:cli:synopsis:`\\lt [-sc] [PATTERN], \\list types [-sc] [PATTERN]`
  List object types.

:cli:synopsis:`\\lr [-c] [PATTERN], \\list roles [-c] [PATTERN]`
  List roles.

:cli:synopsis:`\\lm [-c] [PATTERN], \\list modules [-c] [PATTERN]`
  List modules.

:cli:synopsis:`\\la [-vsc] [PATTERN], \\list aliases [-vsc] [PATTERN]`
  List expression aliases.

:cli:synopsis:`\\lc [-c] [PATTERN], \\list casts [-c] [PATTERN]`
  List available conversions between types.

:cli:synopsis:`\\li [-vsc] [PATTERN], \\list indexes [-vsc] [PATTERN]`
  List indexes.

Database
--------

.. note::

    EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to replace
    databases. If you are running 5.0 or later, you will instead use the
    commands in the "Branch" section below.

:cli:synopsis:`\\database create NAME`
  Create a new database.

Branch
------

.. note::

    EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to replace
    databases. These commands require CLI version 4.3.0 or later and EdgeDB
    version 5.0 or later. If you are running an earlier version of EdgeDB,
    you will instead use the database commands above.

:cli:synopsis:`\\branch create NAME`
  Create a new branch. The backslash command mirrors the options of the CLI's
  :ref:`ref_cli_edgedb_branch_create`.

:cli:synopsis:`\\branch switch NAME`
  Switch to a different branch. The backslash command mirrors the options of
  the CLI's :ref:`ref_cli_edgedb_branch_switch`.

Query Analysis
--------------

:cli:synopsis:`\\analyze QUERY`
  .. note::

      This command is compatible with EdgeDB server 3.0 and above.

  Run a query performance analysis on the given query. Most conveniently used
  without a backslash by just adding ``analyze`` before any query.

:cli:synopsis:`\\expand`
  .. note::

      This command is compatible with EdgeDB server 3.0 and above.

  Print expanded output of last ``analyze`` operation.

Data Operations
---------------

:cli:synopsis:`\\dump FILENAME`
  Dump current database branch to a file at *FILENAME*.

:cli:synopsis:`\\restore FILENAME`
  Restore the database dump at *FILENAME* into the current branch (or currently
  connected database for pre-v5).

Editing
-------

:cli:synopsis:`\\s, \\history`
  Show a history of commands executed in the shell.

:cli:synopsis:`\\e, \\edit [N]`
  Spawn ``$EDITOR`` to edit the most recent history entry or history entry *N*.
  History entries are negative indexed with ``-1`` being the most recent
  command. Use the ``\history`` command (above) to see previous command
  indexes.

  The output of this will then be used as input into the shell.

Settings
--------

:cli:synopsis:`\\set [OPTION [VALUE]]`
  If *VALUE* is omitted, the command will show the current value of *OPTION*.
  With *VALUE*, the option named by *OPTION* will be set to the provided value.
  Use ``\set`` with no arguments for a listing of all available options.

Connection
----------

:cli:synopsis:`\\c, \\connect [DBNAME]`
  Connect to database *DBNAME*.

  .. note::

      EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to replace
      databases. If you are running 5.0 or later, you will instead use the
      ``\branch switch NAME`` command to switch to a different branch.

Migrations
----------

These migration commands are also accessible directly from the command line
without first entering the EdgeDB shell. Their counterpart commands are noted
and linked in their descriptions if you want more detail.

:cli:synopsis:`\\migration create`
  Create a migration script based on differences between the current branch (or
  database for pre-v5) and the schema file, just like running
  :ref:`ref_cli_edgedb_migration_create`.

:cli:synopsis:`\\migrate, \\migration apply`
  Apply your migration, just like running the
  :ref:`ref_cli_edgedb_migrate`.

:cli:synopsis:`\\migration edit`
  Spawn ``$EDITOR`` on the last migration file and fixes the migration ID after
  the editor exits, just like :ref:`ref_cli_edgedb_migration_edit`. This is
  typically used only on migrations that have not yet been applied.

:cli:synopsis:`\\migration log`
  Show the migration history, just like :ref:`ref_cli_edgedb_migration_log`.

:cli:synopsis:`\\migration status`
  Show how the state of the schema in the EdgeDB instance compares to the
  migration stored in the schema directory, just like
  :ref:`ref_cli_edgedb_migration_status`.

Help
----

:cli:synopsis:`\\?, \\h, \\help`
  Show help on backslash commands.

:cli:synopsis:`\\q, \\quit, \\exit`
  Quit the REPL. You can also do this by pressing Ctrl+D.
