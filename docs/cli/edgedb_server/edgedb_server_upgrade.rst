.. _ref_cli_edgedb_server_upgrade:


=====================
edgedb server upgrade
=====================

Upgrade EdgeDB installations and instances.

.. cli:synopsis::

     edgedb server upgrade [OPTIONS] [<name>]


Description
===========

There are few modes of operation of this command:

* :cli:synopsis:`edgedb server upgrade`

  Without arguments this command upgrades all instances which aren't
  running nightly EdgeDB to a latest minor version of the server.

* :cli:synopsis:`edgedb server upgrade <name>`

  Upgrades a specific instance to the specified major version of the
  server or to the latest nightly, by default upgrades to the latest
  stable. This only works for instances that initially aren't running
  nightly. One of the :cli:synopsis:`--to-version=<version>` or
  :cli:synopsis:`--to-nightly` options must be used for this mode.

* :cli:synopsis:`edgedb server upgrade --nightly`

  Upgrades all existing nightly instances to the latest EdgeDB
  nightly version.


Options
=======

:cli:synopsis:`<name>`
    Only upgrade specified database instance.

:cli:synopsis:`--nightly`
    Upgrade all nightly instances.

:cli:synopsis:`--force`
    Force upgrade process even if there is no new version.

:cli:synopsis:`-v, --verbose`
    Produce a more verbose output.

:cli:synopsis:`--to-version=<version>`
    Upgrade to the specified major version.

:cli:synopsis:`--to-nightly`
    Upgrade to the latest nightly version.
