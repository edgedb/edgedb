.. _ref_cli_edgedb_cloud:


============
edgedb cloud
============

In addition to managing your own local and remote instances, the EdgeDB CLI
offers tools to manage your instances running on our EdgeDB Cloud.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_cloud_login
    edgedb_cloud_logout
    edgedb_cloud_secretkey/index

Usage
=====

To use the CLI with EdgeDB Cloud, start by running
:ref:`ref_cli_edgedb_cloud_login`. This will open a browser and allow you to
log in to EdgeDB Cloud.

.. note::

    During the Cloud beta, you will only be able to successfully complete
    authentication if you have been invited to the beta.

Once your login is complete, you may use the other CLI commands to create and
interact with Cloud instances.

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_cloud_login`
      - Authenticate to the EdgeDB Cloud and remember the access token locally
    * - :ref:`ref_cli_edgedb_cloud_logout`
      - Forget the stored access token
    * - :ref:`ref_cli_edgedb_cloud_secretkey`
      - Manage your secret keys
