.. _ref_cli_edgedb_configure:

================
edgedb configure
================

Configure the EdgeDB server.

.. cli:synopsis::

    edgedb [<connection-option>...] configure [ <option> ... ] \
        set <parameter> <value>

    edgedb [<connection-option>...] configure [ <option> ... ] \
        reset <parameter>

    edgedb [<connection-option>...] configure [ <option> ... ] \
        insert <parameter-class> [ --<property>=<value> ... ]

    edgedb [<connection-option>...] configure [ <option> ... ] \
        reset <parameter-class> [ --<property>=<value> ... ]


Description
===========

``edgedb configure`` is a terminal command used to alter the configuration
of an EdgeDB instance.


Options
=======

:cli:synopsis:`<parameter>`
    The name of a primitive configuration parameter.  Available
    configuration parameters are described in the :ref:`ref_admin_config`
    section.

:cli:synopsis:`<value>`
    A value literal for a given configuration parameter or configuration
    object property.

:cli:synopsis:`<parameter-class>`
    The name of a composite configuration value class.  Available
    configuration classes are described in the :ref:`ref_admin_config`
    section.

:cli:synopsis:`--<property>=<value>`
    Set the :cli:synopsis:`<property>` of a configuration object to
    :cli:synopsis:`<value>`.


Connection Options
==================

:cli:synopsis:`-h <hostname>, --host=<hostname>`
    Specifies the host name of the machine on which the server is running.
    If :cli:synopsis:<hostname> begins with a slash (``/``), it is used
    as the directory where the command looks for the server Unix-domain
    socket.  Defaults to the value of the ``EDGEDB_HOST`` environment
    variable.

:cli:synopsis:`-p <port>, --port=<port>`
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
