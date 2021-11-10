.. _ref_admin_databases:

========
DATABASE
========

:edb-alt-title: Databases


This section describes the administrative commands pertaining to
:ref:`databases <ref_datamodel_databases>`.


CREATE DATABASE
===============

:eql-statement:

Create a new database.

.. eql:synopsis::

    CREATE DATABASE <name> ;

Description
-----------

``CREATE DATABASE`` creates a new EdgeDB database.

The new database will be created with all standard schemas prepopulated.

Examples
--------

Create a new database:

.. code-block:: edgeql

    CREATE DATABASE appdb;


DROP DATABASE
=============

:eql-statement:

Remove a database.

.. eql:synopsis::

    DROP DATABASE <name> ;

Description
-----------

``DROP DATABASE`` removes an existing database.  It cannot be executed
while there are existing connections to the target database.

.. warning::

    ``DROP DATABASE`` removes data permanently and cannot be undone.

Examples
--------

Remove a database:

.. code-block:: edgeql

    DROP DATABASE appdb;
