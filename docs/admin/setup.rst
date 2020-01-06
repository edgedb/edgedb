.. _ref_admin_setup:

=============
Initial Setup
=============

Once EdgeDB has been installed using one of the methods described in the
:ref:`ref_admin_install` section, a couple of initial setup steps are
necessary in order to start using it.


Creating an EdgeDB Instance
===========================

.. note::

    If you have installed EdgeDB using a pre-built package, a default
    system-wide instance would have been created
    (usually in ``/var/lib/edgedb-N/``, where ``N`` is the major version
    of EdgeDB).  In this case you can skip this section and refer to
    the installation section appropriate for your OS.

To use EdgeDB you must first create an *instance*.  An EdgeDB instance
is a collection of *databases* that is managed by a particular running
EdgeDB server.  The data managed by the instance is usually stored in
a single directory, which is called the *data directory*.

The EdgeDB server creates and populates a data directory automatically
on the first run.  The location of the data directory can be specified
using the ``-D`` option:

.. code-block:: bash

    $ edgedb-server -D /data/directory/path

The server would then populate the specified directory with the initial
databases: ``edgedb`` and ``<user>``, where *<user>* the name of
the OS user that has started the server (if different from ``edgedb``).
Two corresponding user roles are also created: ``edgedb`` and ``<user>``,
both with superuser privileges.


Configuring Client Authentication
=================================

By default, EdgeDB requires every connecting client to provide a password
for authentication.  There is no default password, so one must be set on
a newly created instance:

.. code-block:: bash

    $ edgedb --admin -H <edgedb-host> alter-role <username> --password

The ``--admin`` option instructs the ``edgedb`` command to connect to
the server using a dedicated administrative socket that does not require
password authentication, but is protected by the OS permissions.
The ``--admin`` option can only be used by the OS user that created the
server instance (or by OS superuser).


Setting Up Passwordless Connections
-----------------------------------

If you are doing testing and don't want to bother with passwords or other
authentication, it is possible to override the default password authentication
method with the ``trust`` method:

.. code-block:: bash

    $ edgedb --admin -H <edgedb-host> configure insert auth --method=trust


Using remote PostgreSQL cluster as a backend
============================================

By default, EdgeDB creates and manages a local PostgreSQL database instance,
however it is also possible to use a remote PostgreSQL instance, as long as it
is version 12 or later.  Superuser access to the cluster is *required*.

To setup EdgeDB using a remote PostgreSQL instance, instead of ``-D``,
specify the ``--postgres-dsn`` option when starting the EdgeDB server:

.. code-block:: bash

    $ edgedb-server \
        --postgres-dsn 'postgres://user:password@host:port/database?opt=val'

The format of the connection string generally follows that of `libpq`_,
including support for specifying the connection parameters via
`environment variables <postgres envvars>`_ and reading passwords from
`the password file <postgres passfile>`_.  Unlike libpq, EdgeDB will treat
unrecognized options as `PostgreSQL settings <postgres settings>`_ to be used
for the connection.  Multiple hosts in the connection string are unsupported.

.. note::

    PostgreSQL DBaaS providers normally do not allow direct superuser access
    to the database instance, which might prevent EdgeDB from working
    correctly.  At this time, only Amazon RDS for PostgreSQL is supported.


.. _libpq:
    https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

.. _postgres envvars:
    https://www.postgresql.org/docs/current/libpq-envars.html

.. _postgres passfile:
    https://www.postgresql.org/docs/current/libpq-pgpass.html

.. _postgres settings:
    https://www.postgresql.org/docs/current/static/runtime-config.html
