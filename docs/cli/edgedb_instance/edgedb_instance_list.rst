.. _ref_cli_edgedb_instance_list:


====================
edgedb instance list
====================

Show all EdgeDB instances.

.. cli:synopsis::

    edgedb instance list [<options>]


Description
===========

``edgedb instance list`` is a terminal command that shows all the
registered EdgeDB instances and some relevant information about them
(status, port, etc.).

.. note::

    The ``edgedb instance list`` command is not intended for use with
    production instances.


Options
=======

:cli:synopsis:`--extended`
    Output more debug info about each instance.

:cli:synopsis:`-j, --json`
    Output in JSON format.
