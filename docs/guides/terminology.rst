.. _ref_guides_terminology:

===========
Terminology
===========

Instances
---------

A running EdgeDB server is called an **instance**. You can :ref:`start
<ref_cli_edgedb_instance_start>`, :ref:`stop
<ref_cli_edgedb_instance_stop>`, and otherwise manage your instances
using :ref:`ref_cli_edgedb_instance` commands.

.. _ref_datamodel_databases:

Databases
---------

An EdgeDB instance can contain multiple "databases". Upon creation, an empty
database called "edgedb" is created in every instance upon initialization. You
can create or delete  If you wish to add more, you can use the
:eql:stmt:`CREATE DATABASE` EdgeQL command.
Conversely, the :eql:stmt:`DROP DATABASE` command removes a database.

The following command will get a list of all databases present in the
instance:

.. code-block:: edgeql

    SELECT sys::Database.name;

If you're using the :ref:`ref_cli_edgedb` interactive shell the
command ``\l`` will list all databases as well.


Modules
-------

Every database contains several modules. By default, all databases are
pre-populated with a set of built-in modules:

* ``std``: standard types, functions and other elements of the
  :ref:`standard library <ref_std>`
* ``schema``: types describing the :ref:`introspection <ref_eql_introspection>`
  schema
* ``sys``: system-wide entities, such as user roles and
  :ref:`databases <ref_datamodel_databases>`
* ``cfg``: configuration and settings
* ``math``: algebraic and statistical :ref:`functions <ref_std_math>`
* ``default``: the default module for user-defined types, functions, etc.

Modules are useful for organizing large schemas into logical subunits.
