.. _ref_cli_edgedb_server_start:


===================
edgedb server start
===================

Start an EdgeDB instance.

.. cli:synopsis::

     edgedb server start [--foreground] <name>


Description
===========

``edgedb server start`` is a terminal command for starting a new
EdgeDB server instance.


Options
=======

:cli:synopsis:`<name>`
    The EdgeDB instance name.

:cli:synopsis:`--foreground`
    Start the server in the foreground rather than using systemd to
    manage the process (note you might need to stop non-foreground
    instance first).
