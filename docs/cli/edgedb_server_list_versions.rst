.. _ref_cli_edgedb_server_list_versions:


===========================
edgedb server list-versions
===========================

List available and installed versions of the EdgeDB server.

.. cli:synopsis::

     edgedb server list-versions [OPTIONS]


Description
===========

``edgedb server list-versions`` is a terminal command for displaying
all the available EdgeDB server versions along with indicating whether
or not and how they are currently installed.


Options
=======

:cli:synopsis:`--json`
    Format output as JSON.

:cli:synopsis:`--installed-only`
    Display only the installed versions.

:cli:synopsis:`--column=<column>`
    Format output as a single column displaying only one aspect of the
    server: ``major-version``, ``installed``, ``available``.
