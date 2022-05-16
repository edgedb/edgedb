.. _ref_cli_edgedb_connopts:

================
Connection flags
================

The ``edgedb`` CLI supports a standard set of connection flags used to specify
the *target* of a given command. The CLI always respects any connection
parameters passed explicitly using flags.

- If no flags are provided, then environment variables will be
  used to determine the instance.
- If no environment variables are present, the CLI will check if the working
  directory is within an instance-linked project directory.
- If none of the above are present, the command fails.

For a detailed breakdown of how connection information is resolved, read the
:ref:`Connection Parameter Resolution <ref_reference_connection>` docs.

################
Connection flags
################

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
