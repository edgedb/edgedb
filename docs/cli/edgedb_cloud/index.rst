.. _ref_cli_edgedb_cloud:


============
edgedb cloud
============

.. note::

    These CLI commands require CLI version 3.0 or later.

In addition to managing your own local and remote instances, the EdgeDB CLI
offers tools to manage your instances running on our EdgeDB Cloud.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_cloud_login
    edgedb_cloud_logout
    edgedb_cloud_secretkey/index

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_cloud_login`
      - Authenticate to the EdgeDB Cloud and remember the access token locally
    * - :ref:`ref_cli_edgedb_cloud_logout`
      - Forget the stored access token
    * - :ref:`ref_cli_edgedb_cloud_secretkey`
      - Manage your secret keys


Usage
=====


1. Log in
---------

To use the CLI with EdgeDB Cloud, start by running
:ref:`ref_cli_edgedb_cloud_login`. This will open a browser and allow you to
log in to EdgeDB Cloud.

.. note::

    During the Cloud beta, you will only be able to successfully complete
    authentication if you have been invited to the beta.


2. Create an instance
---------------------

Once your login is complete, you may use the other CLI commands to create and
interact with Cloud instances. To create an Cloud instance, you can use one of
these commands:

* :ref:`ref_cli_edgedb_instance_create` with an instance name of
  ``<github-username>/<instance-name>``.

  .. code-block:: bash

      $ edgedb instance create <github-username>/<instance-name>

* :ref:`ref_cli_edgedb_project_init` with the ``--server-instance`` option. Set
  the server instance name to ``<github-username>/<instance-name>``.

  .. code-block:: bash

      $ edgedb project init \
        --server-instance <github-username>/<instance-name>

  Alternatively, you can run ``edgedb project init`` *without* the
  ``--server-instance`` option and enter an instance name in the
  ``<github-username>/<instance-name>`` format when prompted interactively.

.. note::

    Please be aware of the following restrictions on EdgeDB Cloud instance
    names:

    * can contain only Latin alpha-numeric characters or ``-``
    * cannot start with a dash (``-``) or contain double dashes (``--``)
    * maximum instance name length is 61 characters minus the length of your
      GitHub username (i.e., length of GitHub username + length of instance
      name must be fewer than 62 characters)



3. Configure your application
-----------------------------

For your production deployment, generate a dedicated secret key for your
instance with :ref:`ref_cli_edgedb_cloud_secretkey_create`. Create two
environment variables accessible to your production application:

* ``EDGEDB_SECRET_KEY``- contains the secret key you generated
* ``EDGEDB_INSTANCE``- the name of your EdgeDB Cloud instance
  (``<github-username>/<instance-name>``)
