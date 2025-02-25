.. _ref_cli_gel_migrate:

===========
gel migrate
===========

This command is an alias for :ref:`ref_cli_gel_migration_apply`.
Once the migration scripts are in place, the changes can be applied to the
database using this command.

.. warning:: Gel Cloud CI users and scripters

    When scripting a ``migrate``/``migration apply`` for a |Gel| Cloud
    instance, do not use :gelcmd:`login` to authenticate. Instead, you should
    generate a secret key in the Gel Cloud UI or by running
    :ref:`ref_cli_gel_cloud_secretkey_create` and set the
    :gelenv:`SECRET_KEY` environment variable to your secret key. Once this
    variable is set to your secret key, logging in is no longer required.
