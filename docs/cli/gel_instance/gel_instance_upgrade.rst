.. _ref_cli_gel_instance_upgrade:


====================
gel instance upgrade
====================

Upgrade |Gel| instance or installation.

.. cli:synopsis::

    gel instance upgrade [<options>] [<name>]


Description
===========

This command is used to upgrade |Gel| instances individually or in
bulk.

.. note::

    The :gelcmd:`instance upgrade` command is not intended for use with
    self-hosted instances.


Options
=======

:cli:synopsis:`<name>`
    The |Gel| instance name to upgrade.

:cli:synopsis:`--force`
    Force upgrade process even if there is no new version.

:cli:synopsis:`--to-latest`
    Upgrade specified instance to the latest major version.

:cli:synopsis:`--to-nightly`
    Upgrade specified instance to a latest nightly version.

:cli:synopsis:`--local-minor`
    Upgrade all local instances to the latest minor versions.

:cli:synopsis:`--to-version=<version>`
    Upgrade to a specified major version.

:cli:synopsis:`-v, --verbose`
    Verbose output.
