.. _ref_datamodel_databases:

=========
Databases
=========

An EdgeDB instance can have multiple databases in it. The
:eql:stmt:`CREATE DATABASE` EdgeQL command adds a new database to the
EdgeDB instance.

The following command will get a list of all databases present in the
instance:

.. code-block:: edgeql

    SELECT sys::Database.name;


See Also
--------

:eql:stmt:`CREATE DATABASE`,
:eql:stmt:`DROP DATABASE`.
