.. _ref_guide_cloud_cli:

===
CLI
===

:edb-alt-title: Using Gel Cloud via the CLI

To use |Gel| Cloud via the CLI, first log in using
:ref:`ref_cli_gel_cloud_login`.

.. note::

    This is the way you'll log in interactively on your development machine,
    but when interacting with Gel Cloud via a script or in CI, you'll
    instead set the :gelenv:`SECRET_KEY` environment variable to your secret
    key. Generate a secret key in the Gel Cloud UI or by running
    :ref:`ref_cli_gel_cloud_secretkey_create`. The :gelcmd:`cloud
    login` and :gelcmd:`cloud logout` commands are not intended for use
    in this context.

Once your login is successful, you will be able to create an instance using
either :ref:`ref_cli_gel_instance_create` or
:ref:`ref_cli_gel_project_init`, depending on whether you also want to
create a local project linked to your instance.

* :ref:`ref_cli_gel_instance_create` with an instance name of
  ``<org-name>/<instance-name>``.

  .. code-block:: bash

      $ gel instance create <org-name>/<instance-name>

* :ref:`ref_cli_gel_project_init` with the ``--server-instance`` option. Set
  the server instance name to ``<org-name>/<instance-name>``.

  .. code-block:: bash

      $ gel project init \
        --server-instance <org-name>/<instance-name>

  Alternatively, you can run :gelcmd:`project init` *without* the
  ``--server-instance`` option and enter an instance name in the
  ``<org-name>/<instance-name>`` format when prompted interactively.

.. note::

    Please be aware of the following restrictions on |Gel| Cloud instance
    names:

    * can contain only Latin alpha-numeric characters or ``-``
    * cannot start with a dash (``-``) or contain double dashes (``--``)
    * maximum instance name length is 61 characters minus the length of your
      organization name (i.e., length of organization name + length of instance
      name must be fewer than 62 characters)

To use :gelcmd:`instance create`:

.. code-block:: bash

    $ gel instance create <org-name>/<instance-name>

To use :gelcmd:`project init`:

.. code-block:: bash

    $ gel project init \
      --server-instance <org-name>/<instance-name>

Alternatively, you can run :gelcmd:`project init` *without* the
``--server-instance`` option and enter an instance name in the
``<org-name>/<instance-name>`` format when prompted interactively.
