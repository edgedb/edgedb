.. _ref_cli_edgedb_server_install:


=====================
edgedb server install
=====================

Install EdgeDB server.

.. cli:synopsis::

     edgedb server install [<options>]


Description
===========

``edgedb server install`` is a terminal command for installing a
specific EdgeDB server version.


Options
=======

:cli:synopsis:`-i, --interactive`
    Performs the installation in interactive mode, similar to how
    :ref:`downloading and installing <ref_cli_edgedb_install>` works.

:cli:synopsis:`--nightly`
    Installs the nightly server version.

:cli:synopsis:`--version=<version>`
    Specifies the version of the server to be installed. Defaults to
    the most recent release.
