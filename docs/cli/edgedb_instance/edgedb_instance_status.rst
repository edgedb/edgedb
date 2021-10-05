.. _ref_cli_edgedb_instance_status:


======================
edgedb instance status
======================

Show instance information.

.. cli:synopsis::

     edgedb instance status [<options>] [<name>]


Description
===========

``edgedb instance status`` is a terminal command for displaying the
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
