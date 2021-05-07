.. _ref_cli_edgedb_server_install:


=====================
edgedb server install
=====================

Install EdgeDB server.

.. cli:synopsis::

     edgedb server install [OPTIONS]


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

:cli:synopsis:`--method=<method>`
    Specifies whether the server should be installed via the local
    package system (``package``) or as a docker image (``docker``).

:cli:synopsis:`--version=<version>`
    Specifies the version of the server to be installed. Defaults to
    the most recent release.
