.. _ref_cli_edgedb_cloud_secretkey_create:


=============================
edgedb cloud secretkey create
=============================

.. note::

    This CLI command requires CLI version 3.0 or later.

Create a new secret key

.. cli:synopsis::

    edgedb cloud secretkey create [<options>]

.. note::

    This command works only if you have already authenticated using
    :ref:`ref_cli_edgedb_cloud_login`.

Options
=======

:cli:synopsis:`--json`
    Output results as JSON
:cli:synopsis:`-n, --name <name>`
    Friendly key name
:cli:synopsis:`--description <description>`
    Long key description
:cli:synopsis:`--expires <<duration> | "never">`
    Key expiration, in duration units, for example "1 hour 30 minutes". If set
    to "never", the key would not expire.
:cli:synopsis:`--scopes <scopes>`
    Comma-separated list of key scopes. Mutually exclusive with
    ``--inherit-scopes``.
:cli:synopsis:`--inherit-scopes`
    Inherit key scopes from the currently used key.  Mutually exclusive with
    ``--scopes``.
:cli:synopsis:`-y, --non-interactive`
    Do not ask questions, assume default answers to all inputs that have a
    default.  Requires key TTL and scopes to be explicitly specified via
    ``--ttl`` or ``--no-expiration``, and ``--scopes`` or ``--inherit-scopes``.
