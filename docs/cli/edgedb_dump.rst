.. _ref_cli_edgedb_dump:


===========
edgedb dump
===========

Backup an EdgeDB database to a file.

.. cli:synopsis::

    edgedb [<connection-option>...] dump [--all] <path>


Description
===========

``edgedb dump`` is a terminal command used to backup an EdgeDB database
into a file.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``dump`` command backups
    the database it is connected to.

:cli:synopsis:`<path>`
    The name of the file to backup the database into.

:cli:synopsis:`--all`
    Dump all databases and the server configuration using the
    directory specified by the :cli:synopsis:`<path>`.

:cli:synopsis:`--format=<format>`
    Choose dump format. For normal dumps this parameter should be
    omitted. For :cli:synopsis:`--all` only
    :cli:synopsis:`--format=dir` is required.
