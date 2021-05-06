.. _ref_cli_edgedb_server_uninstall:


=======================
edgedb server uninstall
=======================

Uninstall EdgeDB server.

.. cli:synopsis::

     edgedb server uninstall [OPTIONS]


Description
===========

``edgedb server uninstall`` is a terminal command for removing a
specific EdgeDB server version from your system.


Options
=======

:cli:synopsis:`--all`
    Uninstalls all server versions.

:cli:synopsis:`--nightly`
    Uninstalls the nightly server version.

:cli:synopsis:`--unused`
    Uninstalls server versions that are not used to run any instances.

:cli:synopsis:`--version=<version>`
    Specifies the version of the server to be uninstalled.

:cli:synopsis:`-v, --verbose`
    Produce a more verbose output.
