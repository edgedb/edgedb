.. versionadded:: 5.0

.. _ref_admin_branches:

======
Branch
======

:edb-alt-title: Branches


This section describes the administrative commands pertaining to
:ref:`branches <ref_datamodel_branches>`.


Create empty branch
===================

:eql-statement:

Create a new branch without schema or data.

.. eql:synopsis::

    create empty branch <name> ;

Description
-----------

The command ``create empty branch`` creates a new EdgeDB branch without schema
or data, aside from standard schemas.

Examples
--------

Create a new empty branch:

.. code-block:: edgeql

    create empty branch newbranch;


Create schema branch
====================

:eql-statement:

Create a new branch copying the schema of an existing branch.

.. eql:synopsis::

    create schema branch <newbranch> from <oldbranch> ;

Description
-----------

The command ``create schema branch`` creates a new EdgeDB branch with schema
copied from an already existing branch.

Examples
--------

Create a new schema branch:

.. code-block:: edgeql

    create schema branch feature from main;


Create data branch
==================

:eql-statement:

Create a new branch copying the schema and data of an existing branch.

.. eql:synopsis::

    create data branch <newbranch> from <oldbranch> ;

Description
-----------

The command ``create data branch`` creates a new EdgeDB branch with schema and
data copied from an already existing branch.

Examples
--------

Create a new data branch:

.. code-block:: edgeql

    create data branch feature from main;


Drop branch
===========

:eql-statement:

Remove a branch.

.. eql:synopsis::

    drop branch <name> ;

Description
-----------

The command ``drop branch`` removes an existing branch. It cannot be executed
while there are existing connections to the target branch.

.. warning::

    Executing ``drop branch`` removes data permanently and cannot be undone.

Examples
--------

Remove a branch:

.. code-block:: edgeql

    drop branch appdb;


Alter branch
============

:eql-statement:

Rename a branch.

.. eql:synopsis::

    alter branch <oldname> rename to <newname> ;

Description
-----------

The command ``alter branch … rename`` changes the name of an existing branch.
It cannot be executed while there are existing connections to the target
branch.

Examples
--------

Rename a branch:

.. code-block:: edgeql

    alter branch featuer rename to feature;
