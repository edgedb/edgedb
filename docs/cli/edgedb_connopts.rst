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
    parameters for local and self-hosted instances are stored in
    ``<edgedb_config_dir>/credentials`` and are usually created by
    :ref:`ref_cli_edgedb_instance_create` or similar commands. Run ``edgedb
    info`` to see the location of ``<edgedb_config_dir>`` on your machine.

    EdgeDB Cloud instance names are in the format
    ``<org-name>/<instance-name>``.

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

:cli:synopsis:`--unix-path /path/to/socket`
    Specifies a path to a Unix socket for an EdgeDB connection. If the path is
    a directory, the actual path will be computed using the ``port`` and
    ``admin`` parameters.

:cli:synopsis:`--admin`
    Connect to a password-less Unix socket (specified by the ``unix-path``)
    with superuser privileges by default.

:cli:synopsis:`-u <username>, --user=<username>`
    Connect to the database as the user :cli:synopsis:`<username>`.
    Defaults to the value of the ``EDGEDB_USER`` environment variable, or,
    if not set, ``edgedb``.

:cli:synopsis:`-d <dbname>, --database=<dbname>`
    Specifies the name of the database to connect to. Defaults to the value of
    the ``EDGEDB_DATABASE`` environment variable. If that variable isn't set,
    local instances will default to ``edgedb`` while remote instances will
    default to the name provided when the link was created. This also includes
    EdgeDB Cloud instance links created via :ref:`ref_cli_edgedb_project_init`.

    .. note::

        With EdgeDB 5, databases were refactored as branches. If you're using
        EdgeDB 5+, use the option below instead of this one.

:cli:synopsis:`-b <branch_name>, --branch=<branch_name>`
    Specifies the name of the branch to connect to. Defaults to the value of
    the ``EDGEDB_BRANCH`` environment variable. If that variable isn't set,
    local instances will default to the most recently switched branch or the
    ``main`` branch, while remote instances will default to the name provided
    when the link was created. This also includes EdgeDB Cloud instance links
    created via :ref:`ref_cli_edgedb_project_init`.

:cli:synopsis:`--password | --no-password`
    If :cli:synopsis:`--password` is specified, force ``edgedb`` to prompt
    for a password before connecting to the database. This is usually not
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

:cli:synopsis:`--secret-key <key>`
    Specifies the secret key to use for authentication with EdgeDB Cloud
    instances. This is not required when connecting to your own EdgeDB Cloud
    instance if you have logged in with :ref:`ref_cli_edgedb_cloud_login`.

:cli:synopsis:`--wait-until-available=<wait_time>`
    In case EdgeDB connection can't be established, keep retrying up
    to :cli:synopsis:`<wait_time>` (e.g. ``30s``). The
    :cli:synopsis:`<timeout>` value must be given using time units (e.g.
    ``hr``, ``min``, ``sec``, ``ms``, etc.).

:cli:synopsis:`--connect-timeout=<timeout>`
    Specifies a :cli:synopsis:`<timeout>` period. In the event EdgeDB doesn't
    respond in this period, the command will fail (or retry if
    :cli:synopsis:`--wait-until-available` is also specified). The
    :cli:synopsis:`<timeout>` value must be given using time units (e.g.
    ``hr``, ``min``, ``sec``, ``ms``, etc.). The default value is ``10s``.
