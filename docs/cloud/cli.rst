.. _ref_guide_cloud_cli:

===
CLI
===

:edb-alt-title: Using EdgeDB Cloud via the CLI

To use EdgeDB Cloud via the CLI, first log in using
:ref:`ref_cli_edgedb_cloud_login`.

.. note::

    This is the way you'll log in interactively on your development machine,
    but when interacting with EdgeDB Cloud via a script or in CI, you'll
    instead set the ``EDGEDB_SECRET_KEY`` environment variable to your secret
    key. Generate a secret key in the EdgeDB Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create`. The ``edgedb cloud login``
    and ``edgedb cloud logout`` commands are not intended for use in this
    context.

Once your login is successful, you will be able to create an instance using
either :ref:`ref_cli_edgedb_instance_create` or
:ref:`ref_cli_edgedb_project_init`, depending on whether you also want to
create a local project linked to your instance.

* :ref:`ref_cli_edgedb_instance_create` with an instance name of
  ``<org-name>/<instance-name>``.

  .. code-block:: bash

      $ edgedb instance create <org-name>/<instance-name>

* :ref:`ref_cli_edgedb_project_init` with the ``--server-instance`` option. Set
  the server instance name to ``<org-name>/<instance-name>``.

  .. code-block:: bash

      $ edgedb project init \
        --server-instance <org-name>/<instance-name>

  Alternatively, you can run ``edgedb project init`` *without* the
  ``--server-instance`` option and enter an instance name in the
  ``<org-name>/<instance-name>`` format when prompted interactively.

.. note::

    Please be aware of the following restrictions on EdgeDB Cloud instance
    names:

    * can contain only Latin alpha-numeric characters or ``-``
    * cannot start with a dash (``-``) or contain double dashes (``--``)
    * maximum instance name length is 61 characters minus the length of your
      organization name (i.e., length of organization name + length of instance
      name must be fewer than 62 characters)

To use ``edgedb instance create``:

.. code-block:: bash

    $ edgedb instance create <org-name>/<instance-name>

To use ``edgedb project init``:

.. code-block:: bash

    $ edgedb project init \
      --server-instance <org-name>/<instance-name>

Alternatively, you can run ``edgedb project init`` *without* the
``--server-instance`` option and enter an instance name in the
``<org-name>/<instance-name>`` format when prompted interactively.
