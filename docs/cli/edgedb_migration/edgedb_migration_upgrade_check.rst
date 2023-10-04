.. _ref_cli_edgedb_migration_upgrade_check:


==============================
edgedb migration upgrade-check
==============================

Checks your schema against a different EdgeDB version.

.. cli:synopsis::

    edgedb migration upgrade-check [<options>]

.. note::

    The upgrade check is performed automatically when you perform an upgrade.

Description
===========

By default, ``upgrade-check`` checks your schema against the latest stable
release of EdgeDB. You can add ``--to-version <version>``, ``--to-testing``,
``--to-nightly``, or ``--to-channel <channel>`` to check against a specific
version.

Options
=======

The ``migration upgrade-check`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.


:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to ``./dbschema``.

:cli:synopsis:`--to-version <to_version>`
    Check the upgrade to a specified version

:cli:synopsis:`--to-nightly`
    Check the upgrade to a latest nightly version

:cli:synopsis:`--to-testing`
    Check the upgrade to a latest testing version

:cli:synopsis:`--to-channel <to_channel>`
    Check the upgrade to the latest version in the channel [possible values:
    stable, testing, nightly]

:cli:synopsis:`--watch`
    Monitor schema changes and check again on change
