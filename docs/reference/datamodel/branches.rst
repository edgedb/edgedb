.. _ref_datamodel_branches:
.. _ref_datamodel_databases:

.. versionadded:: 5.0

========
Branches
========

Gel's |branches| are equivalent to PostgreSQL's *databases* and map to
them directly. Gel comes with tooling to help manage branches and build
a development workflow around them. E.g. when developing locally you can
map your Gel branches to your Git branches, and when using Gel Cloud and
GitHub you can have a branch per PR.


CLI commands
============

Refer to the :ref:`gel branch <ref_cli_gel_branch>` command group for
details on the CLI commands for managing branches.


.. _ref_admin_branches:

DDL commands
============

These are low-level commands that are used to create, alter, and drop branches.
You can use them when experimenting in REPL, of if you want to create your own
tools to manage Gel branches.


Create empty branch
-------------------

:eql-statement:

Create a new branch without schema or data.

.. eql:synopsis::

    create empty branch <name> ;

Description
^^^^^^^^^^^

The command ``create empty branch`` creates a new Gel branch without schema
or data, aside from standard schemas.

Example
^^^^^^^

Create a new empty branch:

.. code-block:: edgeql

    create empty branch newbranch;


Create schema branch
--------------------

:eql-statement:

Create a new branch copying the schema (without data)of an existing branch.

.. eql:synopsis::

    create schema branch <newbranch> from <oldbranch> ;

Description
^^^^^^^^^^^

The command ``create schema branch`` creates a new Gel branch with schema
copied from an already existing branch.

Example
^^^^^^^

Create a new schema branch:

.. code-block:: edgeql

    create schema branch feature from main;


Create data branch
------------------

:eql-statement:

Create a new branch copying the schema and data of an existing branch.

.. eql:synopsis::

    create data branch <newbranch> from <oldbranch> ;

Description
^^^^^^^^^^^

The command ``create data branch`` creates a new Gel branch with schema and
data copied from an already existing branch.

Example
^^^^^^^

Create a new data branch:

.. code-block:: edgeql

    create data branch feature from main;


Drop branch
-----------

:eql-statement:

Remove a branch.

.. eql:synopsis::

    drop branch <name> ;

Description
^^^^^^^^^^^

The command ``drop branch`` removes an existing branch. It cannot be executed
while there are existing connections to the target branch.

.. warning::

    Executing ``drop branch`` removes data permanently and cannot be undone.

Example
^^^^^^^

Remove a branch:

.. code-block:: edgeql

    drop branch appdb;


Alter branch
------------

:eql-statement:

Rename a branch.

.. eql:synopsis::

    alter branch <oldname> rename to <newname> ;

Description
^^^^^^^^^^^

The command ``alter branch … rename`` changes the name of an existing branch.
It cannot be executed while there are existing connections to the target
branch.

Example
^^^^^^^

Rename a branch:

.. code-block:: edgeql

    alter branch featuer rename to feature;
