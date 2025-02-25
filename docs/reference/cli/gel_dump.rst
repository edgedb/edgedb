.. _ref_cli_gel_dump:


========
gel dump
========

Backup a |Gel| |branch| to a file.

.. cli:synopsis::

    gel dump [<options>] <path>


Options
=======

The ``dump`` command creates a backup of the currently active database |branch|.
For specifying the connection target see :ref:`connection options
<ref_cli_gel_connopts>`.

:cli:synopsis:`<path>`
    The name of the file to backup the database branch into.

:cli:synopsis:`--all`
    Dump all |branches| and the server configuration using the
    directory specified by the :cli:synopsis:`<path>`.

:cli:synopsis:`--format=<format>`
    Choose dump format. For normal dumps this parameter should be
    omitted. For :cli:synopsis:`--all` only
    :cli:synopsis:`--format=dir` is required.
