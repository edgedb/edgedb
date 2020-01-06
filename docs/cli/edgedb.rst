.. _ref_cli_edgedb:

======
edgedb
======

:edgedb-alt-title: edgedb -- Interactive Shell

EdgeDB interactive shell.

.. cli:synopsis::

    edgedb [<connection-option>...]


Description
===========

``edgedb`` is a terminal-based front-end to EdgeDB.  It allows running
queries and seeing results interactively.


Options
=======

:cli:synopsis:`-h, --help`
    Show help about the command and exit.

:cli:synopsis:`-H <hostname>, --host=<hostname>`
    Specifies the host name of the machine on which the server is running.
    If :cli:synopsis:<hostname> begins with a slash (``/``), it is used
    as the directory where the command looks for the server Unix-domain
    socket.  Defaults to the value of the ``EDGEDB_HOST`` environment
    variable.

:cli:synopsis:`-P <port>, --port=<port>`
    Specifies the TCP port or the local Unix-domain socket file extension
    on which the server is listening for connections.  Defaults to the value
    of the ``EDGEDB_PORT`` environment variable or, if not set, to ``5656``.

:cli:synopsis:`-u <username>, --user=<username>`
    Connect to the database as the user :cli:synopsis:`<username>`.
    Defaults to the value of the ``EDGEDB_USER`` environment variable, or,
    if not set, to the login name of the current OS user.

:cli:synopsis:`-d <dbname>, --database=<dbname>`
    Specifies the name of the database to connect to.  Default to the value
    of the ``EDGEDB_DATABASE`` environment variable, or, if not set, to
    the calculated value of :cli:synopsis:`<username>`.

:cli:synopsis:`--admin`
    If specified, attempt to connect to the server via the administrative
    Unix-domain socket.  The user must have permission to access the socket,
    but no other authentication checks are performed.

:cli:synopsis:`--password | --no-password`
    If :cli:synopsis:`--password` is specified, force ``edgedb`` to prompt
    for a password before connecting to the database.  This is usually not
    necessary, since ``edgedb`` will prompt for a password automatically
    if the server requires it.

    Specifying :cli:synopsis:`--no-password` disables all password prompts.

:cli:synopsis:`--password-from-stdin`
    Use the first line of standard input as the password.
