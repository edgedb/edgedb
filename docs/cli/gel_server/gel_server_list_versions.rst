.. _ref_cli_gel_server_list_versions:


========================
gel server list-versions
========================

List available and installed versions of the |Gel| server.

.. cli:synopsis::

     gel server list-versions [<options>]


Description
===========

:gelcmd:`server list-versions` is a terminal command for displaying
all the available |Gel| server versions along with indicating whether
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
