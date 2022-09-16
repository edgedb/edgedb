.. _ref_intro_instances:

=========
Instances
=========

Let's get to the good stuff. You can spin up an EdgeDB instance with a single
command.

.. code-block:: bash

  $ edgedb instance create my_instance

This creates a new instance named ``my_instance`` that runs the latest stable
version of EdgeDB. (EdgeDB itself will be automatically installed if it isn't
already.) Alternatively you can specify a specific version with
``--version``.

.. code-block:: bash

  $ edgedb instance create my_instance --version 2.1
  $ edgedb instance create my_instance --version nightly

We can execute a query against our new instance with ``edgedb query``. Specify
which instance to connect to by passing an instance name into the ``-I`` flag.

.. code-block:: bash

  $ edgedb query "select 3.14" -I my_instance
  3.14

Creating databases
^^^^^^^^^^^^^^^^^^
A single EdgeDB *instance* can contain multiple *databases*. Upon creation, an
instance contains a single database called ``edgedb``. All queries and CLI
commands are executed against this database unless otherwise specified.

To create a new database:

.. code-block:: bash

  $ edgedb database create newdb -I my_instance

We can now execute queries against this new database by specifying it with the
``--database/-d`` flag.

.. code-block:: bash

  $ edgedb query "select 3.14" -I my_instance -d newdb
  3.14

Managing instances
^^^^^^^^^^^^^^^^^^
Instances can be stopped, started, restarted, and destroyed.

.. code-block:: bash

  $ edgedb instance stop -I my_instance
  $ edgedb instance start -I my_instance
  $ edgedb instance restart -I my_instance
  $ edgedb instance destroy -I my_instance


Listing instances
^^^^^^^^^^^^^^^^^

To list all instances on your machine:

.. code-block:: bash

  $ edgedb instance list
  ┌────────┬──────────────────┬──────────┬────────────────┬──────────┐
  │ Kind   │ Name             │ Port     │ Version        │ Status   │
  ├────────┼──────────────────┼──────────┼────────────────┼──────────┤
  │ local  │ my_instance      │ 10700    │ 2.x+8421216    │ active   │
  │ local  │ my_instance_2    │ 10701    │ 2.x+8421216    │ active   │
  │ local  │ my_instance_3    │ 10702    │ 2.x+8421216    │ active   │
  └────────┴──────────────────┴──────────┴────────────────┴──────────┘

Further reference
^^^^^^^^^^^^^^^^^

For complete documentation on managing instances with the CLI (upgrading,
viewing logs, etc.), refer to the :ref:`edgedb instance
<ref_cli_edgedb_instance>` reference or view the helptext in your shell:

.. code-block:: bash

  $ edgedb instance --help


