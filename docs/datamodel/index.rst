.. eql:section-intro-page:: datamodel

==========
Data Model
==========

In this section you'll find an overview of EdgeDB, a relational
database with strongly typed schema, starting from the database server
all the way to the fundamental types, objects, and concepts for
EdgeDB.

.. toctree::
    :maxdepth: 3
    :hidden:

    typesystem
    objects/index
    scalars
    colltypes
    functions
    links
    props
    linkprops
    computables
    indexes
    constraints
    aliases
    annotations
    extensions


Instances
=========

A running EdgeDB server is called an **instance**. Typically, you
would create an instance for a :ref:`project
<ref_cli_edgedb_project>`. You can :ref:`start
<ref_cli_edgedb_instance_start>`, :ref:`stop
<ref_cli_edgedb_instance_stop>`, and otherwise manage your instances
using :ref:`ref_cli_edgedb_instance` commands.

.. _ref_datamodel_databases:

Databases
=========

An EdgeDB instance can have multiple databases in it. Every instance
is created with an empty database called "edgedb". If you wish to add
more, you can use the :eql:stmt:`CREATE DATABASE` EdgeQL command.
Conversely, the :eql:stmt:`DROP DATABASE` command removes a database.

The following command will get a list of all databases present in the
instance:

.. code-block:: edgeql

    SELECT sys::Database.name;

If you're using the :ref:`ref_cli_edgedb` interactive shell the
command ``\l`` will list all databases as well.


.. _ref_datamodel_modules:

Modules
=======

Every database has a schema that fully describes it. Schema consists
of logical units called  **modules**, which act as namespaces.
Modules contain user-defined types, functions, etc. A module has a
name that is unique inside a database. The same schema object name can
be used in different modules without conflict.  For example, both
``module1`` and ``module2`` can contain a ``User`` object type.

Schema objects can be referred to by a fully-qualified name using the
``module::Name`` notation.

Every EdgeDB schema contains the following standard modules:

* ``std``: standard types, functions and other elements of the
  :ref:`standard library <ref_std>`
* ``schema``: types describing the :ref:`introspection <ref_eql_introspection>`
  schema
* ``sys``: system-wide entities, such as user roles and
  :ref:`databases <ref_datamodel_databases>`
* ``cfg``: configuration and settings
* ``math``: algebraic and statistical :ref:`functions <ref_std_math>`
* ``default``: the default module for user-defined types, functions, etc.

EdgeDB provides :ref:`migration tools <ref_cli_edgedb_migration>` that
use a a high-level declarative :ref:`schema definition
language<ref_eql_sdl>` to manage the schema state. The EdgeDB SDL is
designed to be a concise and readable representation of the schema.
Most of the examples and synopses in this documentation use the SDL
notation.

Here's a quick overview of the kind of things that can be defined in
modules:

Types in EdgeDB include your own **Object Types** (e.g. *User*) and
**Abstract Types** for other types to extend (e.g. ``HasEmailAddress``
for ``User`` and others can inherit), plus **Scalar Types** with
single values (``str``, ``int64``, etc.) and **Collection Types** like
**arrays** and **tuples** for multiple values.

Putting your **Object Types** together are **properties** and
**links**. You can build on them with items like **annotations**
(readable notes for others), **constraints** to set limits (e.g.
maximum length, minimum value, or even create your own), **indexes**
for faster querying, and **computed** properties or links to define
useful expressions (e.g. ``property email := .user_name ++ '@' ++
.provider_name``).

**Expression Aliases** let you use existing types under new names to
build on them without touching the original -- both in your schema or
on the fly inside a query.

You can also create your own **functions**, strongly typed along with
everything else in EdgeDB.
