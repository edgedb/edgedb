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

    $ edgedb --admin -h </data/dir> alter role <username> --password

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

    $ edgedb --admin -h </data/dir> configure insert auth --method=trust
