.. _ref_cli_edgedb_server_info:


==================
edgedb server info
==================

Show server information.

.. cli:synopsis::

     edgedb server info [OPTIONS]


Description
===========

``edgedb server info`` is a terminal command for displaying the
information about installed EdgeDB servers.


Options
=======

:cli:synopsis:`--json`
    Format output as JSON.

:cli:synopsis:`--nightly`
    Display the information about the nightly server version.

:cli:synopsis:`--bin-path`
    Display only the server binary path (if applicable).

:cli:synopsis:`--method=<method>`
    Display the information about the servers installed as either a
    local package (``package``) or as a docker image (``docker``).

:cli:synopsis:`--version=<version>`
    Display the information about a specific server version.
