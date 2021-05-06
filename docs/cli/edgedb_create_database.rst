.. _ref_cli_edgedb_create_db:


======================
edgedb create-database
======================

Create a new :ref:`database <ref_datamodel_databases>`.

.. cli:synopsis::

    edgedb [<connection-option>...] create-database <name>


Description
===========

``edgedb create-database`` is a terminal command equivalent to
:eql:stmt:`CREATE DATABASE`.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The
    ``create-database`` command runs in the EdgeDB instance it is
    connected to.

:cli:synopsis:`<name>`
    The name of the new database.
