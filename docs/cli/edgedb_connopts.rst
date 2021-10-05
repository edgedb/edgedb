.. _ref_cli_edgedb_connopts:

=========================
Common Connection Options
=========================

Various EdgeDB terminal tools such as :ref:`ref_cli_edgedb` repl,
:ref:`ref_cli_edgedb_configure`, :ref:`ref_cli_edgedb_dump`,
and :ref:`ref_cli_edgedb_restore` use the following connection options:

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

:cli:synopsis:`--credentials-file <credentials_file>`
    Path to JSON file containing credentials.

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

:cli:synopsis:`-u <username>, --user=<username>`
    Connect to the database as the user :cli:synopsis:`<username>`.
    Defaults to the value of the ``EDGEDB_USER`` environment variable, or,
    if not set, to the login name of the current OS user.

:cli:synopsis:`-d <dbname>, --database=<dbname>`
    Specifies the name of the database to connect to.  Default to the value
    of the ``EDGEDB_DATABASE`` environment variable, or, if not set, to
    the calculated value of :cli:synopsis:`<username>`.

:cli:synopsis:`--password | --no-password`
    If :cli:synopsis:`--password` is specified, force ``edgedb`` to prompt
    for a password before connecting to the database.  This is usually not
    necessary, since ``edgedb`` will prompt for a password automatically
    if the server requires it.

    Specifying :cli:synopsis:`--no-password` disables all password prompts.

:cli:synopsis:`--password-from-stdin`
    Use the first line of standard input as the password.

:cli:synopsis:`--tls-ca-file <tls_ca_file>`
    Certificate to match server against.

    This might either be full self-signed server certificate or
    certificate authority (CA) certificate that server certificate is
    signed with.

:cli:synopsis:`--tls-verify-hostname`
    Verify hostname of the server using provided certificate.

    It's useful when certificate authority (CA) is used for handling
    certificate and usually not used for self-signed certificates.

    By default it's enabled when no specific certificate is present
    (via :cli:synopsis:`--tls-ca-file` or in credentials JSON file).

:cli:synopsis:`--no-tls-verify-hostname`
    Do not verify hostname of the server.

    This allows using any certificate for any hostname. However,
    certificate must be present and match certificate specified with
    :cli:synopsis:`--tls-ca-file` or credentials file or signed by one
    of the root certificate authorities.

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
