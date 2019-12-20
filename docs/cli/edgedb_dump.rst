.. _ref_cli_edgedb_dump:


===========
edgedb dump
===========

Backup an EdgeDB database to a file.

.. cli:synopsis::

    edgedb dump [<connection-option>...] FILENAME


Description
===========

``edgedb dump`` is a terminal command used to backup an EdgeDB database
into a file.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``dump`` command backups
    the database it is connected to.

:cli:synopsis:`FILENAME`
    The name of the file to backup the database into.
