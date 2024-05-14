.. _ref_cli_edgedb_cloud_logout:


===================
edgedb cloud logout
===================

.. note::

    This CLI command requires CLI version 3.0 or later.

Forget the stored access token

.. cli:synopsis::

    edgedb cloud logout [<options>]

.. warning:: CI users and scripters

    This command is not intended for use in scripting and CI. Instead, to
    authenticate to your EdgeDB Cloud account, you should generate a secret key
    in the EdgeDB Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create` and set the
    ``EDGEDB_SECRET_KEY`` environment variable to your secret key. Logging out
    is not necessary.

Options
=======

:cli:synopsis:`--all-profiles`
    Logout from all Cloud profiles
:cli:synopsis:`--force`
    Force log out from all profiles, even if linked to a project
:cli:synopsis:`--non-interactive`
    Do not ask questions, assume user wants to log out of all profiles not
    linked to a project
