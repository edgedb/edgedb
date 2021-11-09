.. _ref_dsn:

DSN specification
#################

DSNs (data source names) are a convenient and flexible way to specify
connection information with a simple string. It takes the following form:

.. code-block::

  edgedb://USERNAME:PASSWORD@HOSTNAME:PORT/DATABASE

For instance, here is a typical DSN:
``edgedb://alice:pa$$w0rd@example.com:1234/my_db``.

All components of the DSN are optional; in fact, ``edgedb://`` is a valid DSN.
Any unspecified values will fall back to their defaults:

.. code-block::

  Host: "localhost"
  Port: 5656
  User: "edgedb"
  Password: null
  Database name: "edgedb"

DSNs also support query parameters (``?host=myhost.com``) to support advanced
use cases. These query parameters fall into three categories: "plain"
parameters (where the parameter contains the value itself), file parameters
(where the param points to a local file containing the actual value), and
environment parameters


.. list-table::

  * - **Plain param**
    - **File param**
    - **Environment param**
  * - ``port``
    - ``port_file``
    - ``port_env``
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
  * - ``tls_cert_file``
    - ``tls_cert_file_file``
    - ``tls_cert_file_env``
  * - ``tls_verify_hostname``
    - ``tls_verify_hostname_file``
    - ``tls_verify_hostname_env``

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

  Relative params are resolved relative to the currect working directory at the
  time of connection.

**Environment params**
  Environment params lets you specify a *pointer* to another environment
  variable. At runtime, the specified environment variable will be read. If it
  isn't set, an error will be thrown.

  .. code-block::

    MY_PASSWORD=p@$$w0rd
    EDGEDB_DSN=edgedb://hostname.com:1234?password_env=MY_PASSWORD

