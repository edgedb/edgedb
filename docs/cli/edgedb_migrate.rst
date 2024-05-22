.. _ref_cli_edgedb_migrate:

==============
edgedb migrate
==============

This command is an alias for :ref:`ref_cli_edgedb_migration_apply`.
Once the migration scripts are in place, the changes can be applied to the
database using this command.

.. warning:: EdgeDB Cloud CI users and scripters

    When scripting a ``migrate``/``migration apply`` for an EdgeDB Cloud
    instance, do not use ``edgedb login`` to authenticate. Instead, you should
    generate a secret key in the EdgeDB Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create` and set the
    ``EDGEDB_SECRET_KEY`` environment variable to your secret key. Once this
    variable is set to your secret key, logging in is no longer required.
