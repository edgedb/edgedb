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

.. note::

    During the Cloud beta, you will only be able to successfully complete
    authentication if you have been invited to the beta.
