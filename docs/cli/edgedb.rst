.. _ref_cli_edgedb:

======
edgedb
======

:edgedb-alt-title: edgedb -- Interactive Shell

EdgeDB interactive shell:

.. cli:synopsis::

    edgedb [<connection-option>...]

It's also possible to run an EdgeQL script by piping it into the
EdgeDB shell. The shell will then run in non-interactive mode and
print all the responses into the standard output:

.. cli:synopsis::

    cat myscript.edgeql | edgedb [<connection-option>...]



Description
===========

``edgedb`` is a terminal-based front-end to EdgeDB.  It allows running
queries and seeing results interactively.


Options
=======

:cli:synopsis:`-h, --help`
    Show help about the command and exit.

:cli:synopsis:`-j, --json`
    Turn on JSON output for the queries (single JSON list per query).

:cli:synopsis:`-t, --tab-separated`
    Turn on tab-separated output mode for the queries. This only works
    with shallow (without nested shapes) object shapes.

:cli:synopsis:`-V, --version`
    Print version.

:cli:synopsis:`-c <query>`
    Execute a query instead of starting REPL (alias to ``edgedb query``)

:cli:synopsis:`-I <name>, --instance=<name>`
    Specifies the named instance to connect to. The actual connection
    parameters are stored in ``$HOME/.edgedb/credentials`` and are usually
    created by ``edgedb server init`` or similar commands.

    This option overrides host and port.

:cli:synopsis:`-d <dbname>, --database=<dbname>`
    Specifies the name of the database to connect to.  Default to the value
    of the ``EDGEDB_DATABASE`` environment variable, or, if not set, to
    the calculated value of :cli:synopsis:`<username>`.

:cli:synopsis:`--dsn=<dsn>`
    Specifies the DSN for EdgeDB to connect to.

    This option overrides all other options except password.

:cli:synopsis:`-H <hostname>, --host=<hostname>`
    Specifies the host name of the machine on which the server is running.
    If :cli:synopsis:`<hostname>` begins with a slash (``/``), it is used
    as the directory where the command looks for the server Unix-domain
    socket.  Defaults to the value of the ``EDGEDB_HOST`` environment
    variable.

:cli:synopsis:`-P <port>, --port=<port>`
    Specifies the TCP port or the local Unix-domain socket file extension
    on which the server is listening for connections.  Defaults to the value
    of the ``EDGEDB_PORT`` environment variable or, if not set, to ``5656``.

:cli:synopsis:`--password | --no-password`
    If :cli:synopsis:`--password` is specified, force ``edgedb`` to prompt
    for a password before connecting to the database.  This is usually not
    necessary, since ``edgedb`` will prompt for a password automatically
    if the server requires it.

    Specifying :cli:synopsis:`--no-password` disables all password prompts.

:cli:synopsis:`--password-from-stdin`
    Use the first line of standard input as the password.

:cli:synopsis:`-u <username>, --user=<username>`
    Connect to the database as the user :cli:synopsis:`<username>`.
    Defaults to the value of the ``EDGEDB_USER`` environment variable, or,
    if not set, to the login name of the current OS user.

:cli:synopsis:`--connect-timeout=<timeout>`
    Specifies a :cli:synopsis:`<timeout>` period. In case EdgeDB
    doesn't respond for this period the command will fail (or retry if
    :cli:synopsis:`--wait-until-available` is also specified). The
    :cli:synopsis:`<timeout>` value must be given using time units
    (e.g. ``hr``, ``min``, ``sec``, ``ms``, etc.). The default
    value is ``10s``.

:cli:synopsis:`--wait-until-available=<wait_time>`
    In case EdgeDB connection can't be established, keep retrying up
    to :cli:synopsis:`<wait_time>` (e.g. ``30s``).
