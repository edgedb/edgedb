.. _ref_intro_instances:

=========
Instances
=========

test |gelcmd|

test2 :gelcmd:`migrate --help`


Let's get to the good stuff. You can spin up an Gel instance with a single
command.

.. code-block:: bash

  $ gel instance create my_instance

This creates a new instance named ``my_instance`` that runs the latest stable
version of Gel. (Gel itself will be automatically installed if it isn't
already.) Alternatively you can specify a specific version with
``--version``.

.. code-block:: bash

  $ gel instance create my_instance --version 6.1
  $ gel instance create my_instance --version nightly

We can execute a query against our new instance with :gelcmd:`query`. Specify
which instance to connect to by passing an instance name into the ``-I`` flag.

.. code-block:: bash

  $ gel query "select 3.14" -I my_instance
  3.14

Managing instances
^^^^^^^^^^^^^^^^^^
Instances can be stopped, started, restarted, and destroyed.

.. code-block:: bash

  $ gel instance stop -I my_instance
  $ gel instance start -I my_instance
  $ gel instance restart -I my_instance
  $ gel instance destroy -I my_instance


Listing instances
^^^^^^^^^^^^^^^^^

To list all instances on your machine:

.. code-block:: bash

  $ gel instance list
  ┌────────┬──────────────────┬──────────┬────────────────┬──────────┐
  │ Kind   │ Name             │ Port     │ Version        │ Status   │
  ├────────┼──────────────────┼──────────┼────────────────┼──────────┤
  │ local  │ my_instance      │ 10700    │ x.x+cc4f3b5    │ active   │
  │ local  │ my_instance_2    │ 10701    │ x.x+cc4f3b5    │ active   │
  │ local  │ my_instance_3    │ 10702    │ x.x+cc4f3b5    │ active   │
  └────────┴──────────────────┴──────────┴────────────────┴──────────┘

Further reference
^^^^^^^^^^^^^^^^^

For complete documentation on managing instances with the CLI (upgrading,
viewing logs, etc.), refer to the :ref:`gel instance
<ref_cli_gel_instance>` reference or view the help text in your shell:

.. code-block:: bash

  $ gel instance --help
