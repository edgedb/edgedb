.. _ref_eql_ddl_object_types:

============
Object Types
============

This section describes the DDL commands pertaining to
:ref:`object types <ref_datamodel_object_types>`.


Create type
===========

:eql-statement:
:eql-haswith:


:ref:`Define <ref_eql_sdl_object_types>` a new object type.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    create [abstract] type <name> [ extending <supertype> [, ...] ]
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      create annotation <annotation-name> := <value>
      create link <link-name> ...
      create property <property-name> ...
      create constraint <constraint-name> ...
      create index on <index-expr>

Description
-----------

The command ``create type`` defines a new object type for use in the
current :versionreplace:`database;5.0:branch`.

If *name* is qualified with a module name, then the type is created
in that module, otherwise it is created in the current module.
The type name must be distinct from that of any existing schema item
in the module.

Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL object type declaration <ref_eql_sdl_object_types_syntax>`,
with some additional features listed below:

:eql:synopsis:`with <with-item> [, ...]`
    Alias declarations.

    The ``with`` clause allows specifying module aliases
    that can be referenced by the command.  See :ref:`ref_eql_statements_with`
    for more information.

The following subcommands are allowed in the ``create type`` block:

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set object type :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`create link <link-name> ...`
    Define a new link for this object type.  See
    :eql:stmt:`create link` for details.

:eql:synopsis:`create property <property-name> ...`
    Define a new property for this object type.  See
    :eql:stmt:`create property` for details.

:eql:synopsis:`create constraint <constraint-name> ...`
    Define a concrete constraint for this object type.  See
    :eql:stmt:`create constraint` for details.

:eql:synopsis:`create index on <index-expr>`
    Define a new :ref:`index <ref_datamodel_indexes>`
    using *index-expr* for this object type.  See
    :eql:stmt:`create index` for details.

Examples
--------

Create an object type ``User``:

.. code-block:: edgeql

    create type User {
        create property name -> str;
    };


.. _ref_eql_ddl_object_types_alter:

Alter type
==========

:eql-statement:
:eql-haswith:


Change the definition of an
:ref:`object type <ref_datamodel_object_types>`.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter type <name>
    [ "{" <subcommand>; [...] "}" ] ;

    [ with <with-item> [, ...] ]
    alter type <name> <subcommand> ;

    # where <subcommand> is one of

      rename to <newname>
      extending <parent> [, ...]
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>
      create link <link-name> ...
      alter link <link-name> ...
      drop link <link-name> ...
      create property <property-name> ...
      alter property <property-name> ...
      drop property <property-name> ...
      create constraint <constraint-name> ...
      alter constraint <constraint-name> ...
      drop constraint <constraint-name> ...
      create index on <index-expr>
      drop index on <index-expr>


Description
-----------

The command ``alter type`` changes the definition of an object type.
*name* must be a name of an existing object type, optionally qualified
with a module name.

Parameters
----------

The following subcommands are allowed in the ``alter type`` block:

:eql:synopsis:`with <with-item> [, ...]`
    Alias declarations.

    The ``with`` clause allows specifying module aliases
    that can be referenced by the command.  See :ref:`ref_eql_statements_with`
    for more information.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the type being altered.

:eql:synopsis:`extending <parent> [, ...]`
    Alter the supertype list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         extending <parent> [, ...]
            [ first | last | before <exparent> | after <exparent> ]

    This subcommand makes the type a subtype of the specified list
    of supertypes.  The requirements for the parent-child relationship
    are the same as when creating an object type.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``first`` -- insert parent(s) at the beginning of the
      parent list,
    * ``last`` -- insert parent(s) at the end of the parent list,
    * ``before <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``after <parent>`` -- insert parent(s) after an existing
      *parent*.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter object type annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>`
    Remove object type :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`alter link <link-name> ...`
    Alter the definition of a link for this object type.  See
    :eql:stmt:`alter link` for details.

:eql:synopsis:`drop link <link-name>`
    Remove a link item from this object type.  See
    :eql:stmt:`drop link` for details.

:eql:synopsis:`alter property <property-name> ...`
    Alter the definition of a property item for this object type.
    See :eql:stmt:`alter property` for details.

:eql:synopsis:`drop property <property-name>`
    Remove a property item from this object type.  See
    :eql:stmt:`drop property` for details.

:eql:synopsis:`alter constraint <constraint-name> ...`
    Alter the definition of a constraint for this object type.  See
    :eql:stmt:`alter constraint` for details.

:eql:synopsis:`drop constraint <constraint-name>;`
    Remove a constraint from this object type.  See
    :eql:stmt:`drop constraint` for details.

:eql:synopsis:`drop index on <index-expr>`
    Remove an :ref:`index <ref_datamodel_indexes>` defined as *index-expr*
    from this object type.  See :eql:stmt:`drop index` for details.

All the subcommands allowed in the ``create type`` block are also
valid subcommands for ``alter type`` block.

Examples
--------

Alter the ``User`` object type to make ``name`` required:

.. code-block:: edgeql

    alter type User {
        alter property name {
            set required;
        }
    };


Drop type
=========

:eql-statement:
:eql-haswith:


Remove the specified object type from the schema.

.. eql:synopsis::

    drop type <name> ;

Description
-----------

The command ``drop type`` removes the specified object type from the
schema. schema.  All subordinate schema items defined on this type,
such as links and indexes, are removed as well.

Examples
--------

Remove the ``User`` object type:

.. code-block:: edgeql

    drop type User;

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Object types <ref_datamodel_object_types>`
  * - :ref:`SDL > Object types <ref_eql_sdl_object_types>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
  * - :ref:`Cheatsheets > Object types <ref_cheatsheet_object_types>`
