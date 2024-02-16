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

Follow :ref:`our EdgeDB Cloud guide <ref_guide_cloud>` for information on how
to use EdgeDB Cloud.
