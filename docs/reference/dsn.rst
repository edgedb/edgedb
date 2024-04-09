.. _ref_dsn:

DSN specification
#################

DSNs (data source names) are a convenient and flexible way to specify
connection information with a simple string. It takes the following form:

.. versionchanged:: _default

    .. code-block::

      edgedb://USERNAME:PASSWORD@HOSTNAME:PORT/DATABASE

    For instance, here is a typical DSN:
    ``edgedb://alice:pa$$w0rd@example.com:1234/my_db``.

.. versionchanged:: 5.0

    .. code-block::

      edgedb://USERNAME:PASSWORD@HOSTNAME:PORT/BRANCH

    For instance, here is a typical DSN:
    ``edgedb://alice:pa$$w0rd@example.com:1234/my_branch``.

All components of the DSN are optional; in fact, ``edgedb://`` is a valid DSN.
Any unspecified values will fall back to their defaults:

.. versionchanged:: _default

    .. code-block::

      Host: "localhost"
      Port: 5656
      User: "edgedb"
      Password: null
      Database name: "edgedb"

.. versionchanged:: 5.0

    .. code-block::

      Host: "localhost"
      Port: 5656
      User: "edgedb"
      Password: null
      Branch name: "main"

Query parameters
----------------

DSNs also support query parameters (``?host=myhost.com``) to support advanced
use cases. The value for a given parameter can be specified in three ways:
directly (e.g. ``?host=example.com``), by specifying an environment variable
containing the value (``?host_env=HOST_VAR``), or by specifying a file
containing the value (``?host_file=./hostname.txt``).

.. note::

  For a breakdown of these configuration options, see :ref:`Reference >
  Connection Parameters <ref_reference_connection_granular>`.

.. versionchanged:: _default

    .. list-table::

      * - **Plain param**
        - **File param**
        - **Environment param**
      * - ``host``
        - ``host_file``
        - ``host_env``
      * - ``port``
        - ``port_file``
        - ``port_env``
      * - ``database``
        - ``database_file``
        - ``database_env``
      * - ``user``
        - ``user_file``
        - ``user_env``
      * - ``password``
        - ``password_file``
        - ``password_env``
      * - ``tls_ca_file``
        - ``tls_ca_file_file``
        - ``tls_ca_file_env``
      * - ``tls_security``
        - ``tls_security_file``
        - ``tls_security_env``

.. versionchanged:: 5.0

    .. list-table::

      * - **Plain param**
        - **File param**
        - **Environment param**
      * - ``host``
        - ``host_file``
        - ``host_env``
      * - ``port``
        - ``port_file``
        - ``port_env``
      * - ``branch``
        - ``branch_file``
        - ``branch_env``
      * - ``user``
        - ``user_file``
        - ``user_env``
      * - ``password``
        - ``password_file``
        - ``password_env``
      * - ``tls_ca_file``
        - ``tls_ca_file_file``
        - ``tls_ca_file_env``
      * - ``tls_security``
        - ``tls_security_file``
        - ``tls_security_env``

**Plain params**
  These "plain" parameters can be used to provide values for options that can't
  otherwise be reflected in the DSN, like TLS settings (described in more
  detail below).

  You can't specify the same setting both in the body of the DSN and in a query
  parameter. For instance, the DSN below is invalid, as the port is ambiguous.

  .. code-block::

    edgedb://hostname.com:1234?port=5678

**File params**
  If you prefer to store sensitive credentials in local files, you can use file
  params to specify a path to a local UTF-8 encoded file. This file should
  contain a single line containing the relevant value.

  .. code-block::

    edgedb://hostname.com:1234?user_file=./username.txt

    # ./username.txt
    my_username

  Relative params are resolved relative to the current working directory at the
  time of connection.

**Environment params**
  Environment params lets you specify a *pointer* to another environment
  variable. At runtime, the specified environment variable will be read. If it
  isn't set, an error will be thrown.

  .. code-block::

    MY_PASSWORD=p@$$w0rd
    EDGEDB_DSN=edgedb://hostname.com:1234?password_env=MY_PASSWORD

