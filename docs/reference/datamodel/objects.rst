.. _ref_datamodel_object_types:

============
Object Types
============

.. index:: type, tables, models

*Object types* are the primary components of a Gel schema. They are
analogous to SQL *tables* or ORM *models*, and consist of :ref:`properties
<ref_datamodel_props>` and :ref:`links <ref_datamodel_links>`.

Properties
==========

Properties are used to attach primitive/scalar data to an object type.
For the full documentation on properties, see :ref:`ref_datamodel_props`.

.. code-block:: sdl

   type Person {
     email: str;
   }

Using in a query:

.. code-block:: edgeql

   select Person {
     email
   };


Links
=====

Links are used to define relationships between object types. For the full
documentation on links, see :ref:`ref_datamodel_links`.

.. code-block:: sdl

   type Person {
     email: str;
     best_friend: Person;
   }

Using in a query:

.. code-block:: edgeql

   select Person {
     email,
     best_friend: {
       email
     }
   };

ID
==

.. index:: uuid, primary key

There's no need to manually declare a primary key on your object types. All
object types automatically contain a property ``id`` of type ``UUID`` that's
*required*, *globally unique*, *readonly*, and has an index on it.
The ``id`` is assigned upon creation and cannot be changed.

Using in a query:

.. code-block:: edgeql

   select Person { id };
   select Person { email } filter .id = <uuid>'123e4567-e89b-...';


Abstract types
==============

.. index:: abstract, inheritance

Object types can either be *abstract* or *non-abstract*. By default all object
types are non-abstract. You can't create or store instances of abstract types
(a.k.a. mixins), but they're a useful way to share functionality and
structure among other object types.

.. code-block:: sdl

   abstract type HasName {
     first_name: str;
     last_name: str;
   }

.. _ref_datamodel_objects_inheritance:
.. _ref_eql_sdl_object_types_inheritance:

Inheritance
===========

.. index:: extending, extends, subtypes, supertypes

Object types can *extend* other object types. The extending type (AKA the
*subtype*) inherits all links, properties, indexes, constraints, etc. from its
*supertypes*.

.. code-block:: sdl

   abstract type HasName {
     first_name: str;
     last_name: str;
   }

   type Person extending HasName {
     email: str;
     best_friend: Person;
   }

Using in a query:

.. code-block:: edgeql

   select Person {
     first_name,
     email,
     best_friend: {
       last_name
     }
   };


.. _ref_datamodel_objects_multiple_inheritance:

Multiple Inheritance
====================

Object types can extend more than one type — that's called
*multiple inheritance*. This mechanism allows building complex object
types out of combinations of more basic types.

.. note::

   Gel's multiple inheritance should not be confused with the multiple
   inheritance of C++ or Python, where the complexity usually arises
   from fine-grained mixing of logic. Gel's multiple inheritance is
   structural and allows for natural composition.

.. code-block:: sdl-diff

      abstract type HasName {
        first_name: str;
        last_name: str;
      }

   +  abstract type HasEmail {
   +    email: str;
   +  }

   -  type Person extending HasName {
   +  type Person extending HasName, HasEmail {
   -    email: str;
        best_friend: Person;
     }

If multiple supertypes share links or properties, those properties must be
of the same type and cardinality.


.. _ref_eql_sdl_object_types:
.. _ref_eql_sdl_object_types_syntax:


Defining object types
=====================

This section describes the syntax to declare object types in your schema.

Syntax
------

.. sdl:synopsis::

   [abstract] type <TypeName> [extending <supertype> [, ...] ]
   [ "{"
       [ <annotation-declarations> ]
       [ <property-declarations> ]
       [ <link-declarations> ]
       [ <constraint-declarations> ]
       [ <index-declarations> ]
       ...
     "}" ]

Description
^^^^^^^^^^^

This declaration defines a new object type with the following options:

:eql:synopsis:`abstract`
    If specified, the created type will be *abstract*.

:eql:synopsis:`<TypeName>`
    The name (optionally module-qualified) of the new type.

:eql:synopsis:`extending <supertype> [, ...]`
    Optional clause specifying the *supertypes* of the new type.

    Use of ``extending`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

    References to supertypes in queries will also include objects of
    the subtype.

    If the same *link* name exists in more than one supertype, or
    is explicitly defined in the subtype and at least one supertype,
    then the data types of the link targets must be *compatible*.
    If there is no conflict, the links are merged to form a single
    link in the new type.

These sub-declarations are allowed in the ``Type`` block:

:sdl:synopsis:`<annotation-declarations>`
    Set object type :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<property-declarations>`
    Define a concrete :ref:`property <ref_eql_sdl_props>` for this object type.

:sdl:synopsis:`<link-declarations>`
    Define a concrete :ref:`link <ref_eql_sdl_links>` for this object type.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` for this
    object type.

:sdl:synopsis:`<index-declarations>`
    Define an :ref:`index <ref_eql_sdl_indexes>` for this object type.


.. _ref_eql_ddl_object_types:

DDL commands
============

This section describes the low-level DDL commands for creating, altering, and
dropping object types. You typically don't need to use these commands directly,
but knowing about them is useful for reviewing migrations.

Create type
-----------

:eql-statement:
:eql-haswith:

Define a new object type.

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
^^^^^^^^^^^

The command ``create type`` defines a new object type for use in the
current |branch|.

If *name* is qualified with a module name, then the type is created
in that module, otherwise it is created in the current module.
The type name must be distinct from that of any existing schema item
in the module.

Parameters
^^^^^^^^^^

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

Example
^^^^^^^

Create an object type ``User``:

.. code-block:: edgeql

   create type User {
       create property name: str;
   };


Alter type
----------

:eql-statement:
:eql-haswith:

Change the definition of an object type.

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
^^^^^^^^^^^

The command ``alter type`` changes the definition of an object type.
*name* must be a name of an existing object type, optionally qualified
with a module name.

Parameters
^^^^^^^^^^

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
valid subcommands for the ``alter type`` block.

Example
^^^^^^^

Alter the ``User`` object type to make ``name`` required:

.. code-block:: edgeql

   alter type User {
       alter property name {
           set required;
       }
   };


Drop type
---------

:eql-statement:
:eql-haswith:

Remove the specified object type from the schema.

.. eql:synopsis::

   drop type <name> ;

Description
^^^^^^^^^^^

The command ``drop type`` removes the specified object type from the
schema.  All subordinate schema items defined on this type,
such as links and indexes, are removed as well.

Example
^^^^^^^

Remove the ``User`` object type:

.. code-block:: edgeql

   drop type User;

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
  * - :ref:`Cheatsheets > Object types <ref_cheatsheet_object_types>`
