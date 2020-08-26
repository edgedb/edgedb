.. _ref_admin_setup:

=============
Initial Setup
=============

Once EdgeDB has been installed using one of the methods described in the
:ref:`ref_admin_install` section, a couple of initial setup steps are
necessary in order to start using it.


Creating an EdgeDB Instance
===========================

To use EdgeDB you must first create an *instance*.  An EdgeDB instance
is a collection of *databases* that is managed by a particular running
EdgeDB server.  The data managed by the instance is usually stored in
a single directory, which is called the *data directory*.

To create a new EdgeDB instance:

.. code-block:: bash

    $ edgedb server init <instance-name>

The server would then populate the specified directory with the initial
databases: ``edgedb`` and ``<user>``, where *<user>* the name of
the OS user that has started the server (if different from ``edgedb``).
Two corresponding user roles are also created: ``edgedb`` and ``<user>``,
both with superuser privileges.


Configuring Client Authentication
=================================

By default, ``edgedb server init`` saves authentication credentials into
user's home directory.  To use the stored credentials from a client,
specify the name of an instance when connecting (we use ``my_instance`` in
the following examples):

.. code-block:: bash

   $ edgedb -Imy_instance

In Python:

.. code-block:: python

   import edgedb
   pool = await edgedb.create_async_pool('my_instance')
   await pool.query("SELECT 1+1")

In JavaScript:

.. code-block:: javascript

   import {createPool} from "edgedb";
   let pool = await createPool("my_instance")
   await pool.query("SELECT 1+1")


Setting Up Passwordless Connections
-----------------------------------

If you are doing testing and don't want to bother with passwords or other
authentication, it is possible to override the default password authentication
method with the ``Trust`` method:

.. code-block:: bash

    $ edgedb -I<instance-name> --admin configure insert Auth \
             --method=Trust --priority=0
