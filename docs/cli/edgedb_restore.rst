.. _ref_cli_edgedb_restore:


==============
edgedb restore
==============

Restore an EdgeDB database from a backup file.

.. cli:synopsis::

    edgedb restore [<options>] <path>


Description
===========

``edgedb restore`` is a terminal command used to restore an EdgeDB database
from a backup file.  An empty target database must be created before using
this command.


Options
=======

The ``restore`` command restores the backup file into the database it is
connected to. For specifying the connection target see :ref:`connection
options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<path>`
    The name of the backup file to restore the database from.

:cli:synopsis:`--all`
    Restore all databases and the server configuration using the
    directory specified by the :cli:synopsis:`<path>`.

:cli:synopsis:`-v, --verbose`
    Verbose output.
