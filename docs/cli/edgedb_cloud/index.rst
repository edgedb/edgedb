.. _ref_cli_edgedb_cloud:


=========
gel cloud
=========


In addition to managing your own local and remote instances, the |Gel| CLI
offers tools to manage your instances running on our Gel Cloud.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_cloud_login
    edgedb_cloud_logout
    edgedb_cloud_secretkey/index

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_cloud_login`
      - Authenticate to the |Gel| Cloud and remember the access token locally
    * - :ref:`ref_cli_edgedb_cloud_logout`
      - Forget the stored access token
    * - :ref:`ref_cli_edgedb_cloud_secretkey`
      - Manage your secret keys

.. warning:: CI users and scripters

    The ``gel cloud login`` and ``gel cloud logout`` commands are not
    intended for use in scripting and CI. Instead, you should generate a secret
    key in the |Gel| Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create` and set the
    ``EDGEDB_SECRET_KEY`` environment variable to your secret key. Once this
    variable is set to your secret key, logging in and out are no longer
    required.

Follow :ref:`our Gel Cloud guide <ref_guide_cloud>` for information on how
to use Gel Cloud.
