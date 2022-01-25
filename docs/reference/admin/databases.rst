.. _ref_admin_databases:

========
Database
========

:edb-alt-title: Databases


This section describes the administrative commands pertaining to
:ref:`databases <ref_datamodel_databases>`.


Create database
===============

:eql-statement:

Create a new database.

.. eql:synopsis::

    create database <name> ;

Description
-----------

The command ``create database`` creates a new EdgeDB database.

The new database will be created with all standard schemas prepopulated.

Examples
--------

Create a new database:

.. code-block:: edgeql

    create database appdb;


Drop database
=============

:eql-statement:

Remove a database.

.. eql:synopsis::

    drop database <name> ;

Description
-----------

The command ``drop database`` removes an existing database.  It cannot
be executed while there are existing connections to the target
database.

.. warning::

    Executing ``drop database`` removes data permanently and cannot be undone.

Examples
--------

Remove a database:

.. code-block:: edgeql

    drop database appdb;
