.. _ref_cli_edgedb_server_status:


====================
edgedb server status
====================

Show instance information.

.. cli:synopsis::

     edgedb server status [OPTIONS] [<name>]


Description
===========

``edgedb server status`` is a terminal command for displaying the
information about EdgeDB instances.


Options
=======

:cli:synopsis:`<name>`
    Show only the status of the specific EdgeDB instance.

:cli:synopsis:`--json`
    Format output as JSON.

:cli:synopsis:`--extended`
    Output more debug info about each instance.

:cli:synopsis:`--service`
    Show current systems service information.
