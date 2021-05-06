.. eql:section-intro-page:: cheatsheet

===========
Cheatsheets
===========

:edb-alt-title: Cheatsheets: EdgeDB by example

Just getting started? Keep an eye on this collection of cheatsheets with
handy examples for what you'll need to get started with EdgeDB.
After familiarizing yourself with them, feel free to dive into more EdgeDB
via our longer `interactive tutorial <https://www.edgedb.com/tutorial>`_ and
**much** longer `Easy EdgeDB textbook </easy-edgedb>`_.

CLI/Admin:

* :ref:`CLI Usage <ref_cheatsheet_cli>` -- Getting your database started.
* :ref:`REPL <ref_cheatsheet_repl>` -- Shortcuts for frequently used
  commands in the EdgeDB REPL.
* :ref:`Administration <ref_cheatsheet_admin>` -- Database and role creation,
  passwords, port configuration, etc.

EdgeQL:

* :ref:`SELECT <ref_cheatsheet_select>` -- Retrieve or compute a set of values.
* :ref:`INSERT <ref_cheatsheet_insert>` -- Create new database objects.
* :ref:`UPDATE <ref_cheatsheet_update>` -- Update database objects.
* :ref:`DELETE <ref_cheatsheet_delete>` -- Remove objects from the database.
* :ref:`GraphQL <ref_cheatsheet_graphql>` -- GraphQL queries supported natively
  out of the box.
* :ref:`Special Syntax <ref_cheatsheet_syntax>` -- Info on types of strings,
  number literals, etc.

Schema:

* :ref:`Types <ref_cheatsheet_types>` -- Make your own object and abstract
  types on top of existing system types.
* :ref:`Functions <ref_cheatsheet_functions>` -- Write and overload your own
  strongly typed functions.
* :ref:`Aliases <ref_cheatsheet_aliases>` -- Use aliases to create new types
  and modify existing ones on the fly.
* :ref:`Annotations <ref_cheatsheet_annotations>` -- Add human readable
  descriptions to items in your schema.
* :ref:`Migrations <ref_cheatsheet_migrations>` -- Describe your final
  schema and let EdgeDB put it together for you.


.. toctree::
    :maxdepth: 3
    :hidden:

    repl
    admin
    cli
    select
    insert
    update
    delete
    functions
    boolean
    types
    aliases
    annotations
    migrations
    syntax
    graphql
