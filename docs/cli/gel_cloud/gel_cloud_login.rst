.. _ref_cli_gel_cloud_login:


===============
gel cloud login
===============

Authenticate to the |Gel| Cloud and remember the secret key locally

.. cli:synopsis::

    gel cloud login

This command will launch your browser and start the |Gel| Cloud authentication
flow. Once authentication is successful, the CLI will log a success message:

.. code-block::

    Successfully logged in to |Gel| Cloud as <your-email>

If you are unable to complete authentication in the browser, you can interrupt
the command by pressing Ctrl-C.

.. warning:: CI users and scripters

    This command is not intended for use in scripting and CI. Instead, you
    should generate a secret key in the |Gel| Cloud UI or by running
    :ref:`ref_cli_gel_cloud_secretkey_create` and set the
    :gelenv:`SECRET_KEY` environment variable to your secret key. Once this
    variable is set to your secret key, logging in is no longer required.
