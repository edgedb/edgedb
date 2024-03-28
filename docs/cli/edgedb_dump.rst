.. _ref_cli_edgedb_dump:


===========
edgedb dump
===========

Backup an EdgeDB branch (or database pre-v5) to a file.

.. cli:synopsis::

    edgedb dump [<options>] <path>


Options
=======

The ``dump`` command creates a backup of the currently active database branch
or, in pre-v5 instances, the currently connected database.
For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`<path>`
    The name of the file to backup the database branch into.

:cli:synopsis:`--all`
    Dump all branches (databases pre-v5) and the server configuration using the
    directory specified by the :cli:synopsis:`<path>`.

:cli:synopsis:`--format=<format>`
    Choose dump format. For normal dumps this parameter should be
    omitted. For :cli:synopsis:`--all` only
    :cli:synopsis:`--format=dir` is required.
