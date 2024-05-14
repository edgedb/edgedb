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

.. warning:: CI users and scripters

    The ``edgedb cloud login`` and ``edgedb cloud logout`` commands are not
    intended for use in scripting and CI. Instead, you should generate a secret
    key in the EdgeDB Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create` and set the
    ``EDGEDB_SECRET_KEY`` environment variable to your secret key. Once this
    variable is set to your secret key, logging in and out are no longer
    required.

Follow :ref:`our EdgeDB Cloud guide <ref_guide_cloud>` for information on how
to use EdgeDB Cloud.
