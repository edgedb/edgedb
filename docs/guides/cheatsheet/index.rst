.. _ref_cheatsheets:

===========
Cheatsheets
===========

:edb-alt-title: Cheatsheets: EdgeDB by example

Just getting started? Keep an eye on this collection of cheatsheets with
handy examples for what you'll need to get started with EdgeDB.
After familiarizing yourself with them, feel free to dive into more EdgeDB
via our longer `interactive tutorial <https://www.edgedb.com/tutorial>`_ and
**much** longer `Easy EdgeDB textbook </easy-edgedb>`_.

EdgeQL:

* :ref:`SELECT <ref_cheatsheet_select>` -- Retrieve or compute a set of values.
* :ref:`INSERT <ref_cheatsheet_insert>` -- Create new database objects.
* :ref:`UPDATE <ref_cheatsheet_update>` -- Update database objects.
* :ref:`DELETE <ref_cheatsheet_delete>` -- Remove objects from the database.
* :ref:`GraphQL <ref_cheatsheet_graphql>` -- GraphQL queries supported natively
  out of the box.

Schema:

* :ref:`Object Types <ref_cheatsheet_object_types>` -- Make your own object
  and abstract types on top of existing system types.
* :ref:`User Defined Functions <ref_cheatsheet_functions>` -- Write and
  overload your own strongly typed functions.
* :ref:`Expression Aliases <ref_cheatsheet_aliases>` -- Use aliases to create
  new types and modify existing ones on the fly.
* :ref:`Schema Annotations <ref_cheatsheet_annotations>` -- Add human readable
  descriptions to items in your schema.

CLI/Admin:

* :ref:`CLI Usage <ref_cheatsheet_cli>` -- Getting your database started.
* :ref:`Interactive Shell <ref_cheatsheet_repl>` -- Shortcuts for
  frequently used commands in the EdgeDB Interactive Shell.
* :ref:`Administration <ref_cheatsheet_admin>` -- Database and role creation,
  passwords, port configuration, etc.


.. toctree::
    :maxdepth: 3
    :hidden:


    select
    insert
    update
    delete
    boolean
    objects
    functions
    aliases
    annotations
    cli
    repl
    admin
