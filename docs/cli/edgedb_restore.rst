.. _ref_cli_edgedb_restore:


==============
edgedb restore
==============

Restore an EdgeDB database from a backup file.

.. cli:synopsis::

    edgedb restore [<connection-option>...] [--allow-non-empty] FILENAME


Description
===========

``edgedb restore`` is a terminal command used to restore an EdgeDB database
from a backup file.  An empty target database must be created before using
this command.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``restore`` command restores
    the backup file into the database it is connected to.

:cli:synopsis:`FILENAME`
    The name of the backup file to restore the database from.

:cli:synopsis:`--allow-non-empty`
    By default the command will not attempt to restore into a non-empty
    database.
