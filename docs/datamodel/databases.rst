.. _ref_datamodel_databases:

=========
Databases
=========

An EdgeDB cluster can have multiple databases in it. The
:eql:stmt:`CREATE DATABASE` EdgeQL command adds a new database to the
EdgeDB cluster.

The following command will get a list of all databases present in the
cluster:

.. code-block:: edgeql

    SELECT schema::Database.name;
