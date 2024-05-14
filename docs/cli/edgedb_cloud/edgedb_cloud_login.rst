.. _ref_cli_edgedb_cloud_login:


==================
edgedb cloud login
==================

.. note::

    This CLI command requires CLI version 3.0 or later.

Authenticate to the EdgeDB Cloud and remember the secret key locally

.. cli:synopsis::

    edgedb cloud login

This command will launch your browser and start the EdgeDB Cloud authentication
flow. Once authentication is successful, the CLI will log a success message:

.. code-block::

    Successfully logged in to EdgeDB Cloud as <your-email>

If you are unable to complete authentication in the browser, you can interrupt
the command by pressing Ctrl-C.

.. warning:: CI users and scripters

    This command is not intended for use in scripting and CI. Instead, you
    should generate a secret key in the EdgeDB Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create` and set the
    ``EDGEDB_SECRET_KEY`` environment variable to your secret key. Once this
    variable is set to your secret key, logging in is no longer required.
