.. _ref_eql_ddl_props:

==========
Properties
==========

This section describes the DDL commands pertaining to
:ref:`properties <ref_datamodel_props>`.


CREATE PROPERTY
===============

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_props>` a new property.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} {TYPE|LINK} <SourceName> "{"
      [ ... ]
      CREATE [ REQUIRED ] [{SINGLE | MULTI}] PROPERTY <name>
        [ EXTENDING <base> [, ...] ] -> <type>
        [ "{" <subcommand>; [...] "}" ] ;
      [ ... ]
    "}"

    # Computable property form:

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} {TYPE|LINK} <SourceName> "{"
      [ ... ]
      CREATE [REQUIRED] [{SINGLE | MULTI}]
        PROPERTY <name> := <expression>;
      [ ... ]
    "}"

    # Abstract property form:

    [ WITH <with-item> [, ...] ]
    CREATE ABSTRACT PROPERTY [<module>::]<name> [EXTENDING <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]

    # where <subcommand> is one of

      SET default := <expression>
      SET readonly := {true | false}
      SET ANNOTATION <annotation-name> := <value>
      CREATE CONSTRAINT <constraint-name> ...


Description
-----------

:eql:synopsis:`{CREATE|ALTER} {TYPE|LINK} ... CREATE PROPERTY` defines a new
concrete property for a given object type or link.

There are three forms of ``CREATE PROPERTY``, as shown in the syntax synopsis
above.  The first form is the canonical definition form, the second
form is a syntax shorthand for defining a
:ref:`computable property <ref_datamodel_computables>`, and the third is a
form to define an abstract property item.  The abstract form allows creating
the property in the specified :eql:synopsis:`<module>`.  Concrete link forms
are always created in the same module as the containing object or link.

.. _ref_eql_ddl_props_syntax:

Parameters
----------

:eql:synopsis:`REQUIRED`
    If specified, the link is considered *required* for the parent
    object type.  It is an error for an object to have a required
    link resolve to an empty value.  Child links **always** inherit
    the *required* attribute, i.e it is not possible to make a
    required link non-required by extending it.

:eql:synopsis:`MULTI`
    Specifies that there may be more than one instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size greater than one.

:eql:synopsis:`SINGLE`
    Specifies that there may be at most *one* instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size not greater than one.  ``SINGLE`` is assumed if nether
    ``MULTI`` nor ``SINGLE`` qualifier is specified.

:eql:synopsis:`EXTENDING <base> [, ...]`
    Optional clause specifying the *parents* of the new link item.

    Use of ``EXTENDING`` creates a persistent schema relationship
    between the new link and its parents.  Schema modifications
    to the parent(s) propagate to the child.

    If the same *property* name exists in more than one parent, or
    is explicitly defined in the new link and at least one parent,
    then the data types of the property targets must be *compatible*.
    If there is no conflict, the link properties are merged to form a
    single property in the new link item.

:eql:synopsis:`<type>`
    The type must be a valid :ref:`type expression <ref_eql_types>`
    denoting a non-abstract scalar or a container type.

The following subcommands are allowed in the ``CREATE PROPERTY`` block:

:eql:synopsis:`SET default := <expression>`
    Specifies the default value for the property as an EdgeQL expression.
    The default value is used in an ``INSERT`` statement if an explicit
    value for this property is not specified.

:eql:synopsis:`SET readonly := {true | false}`
    If ``true``, the property is considered *read-only*.  Modifications
    of this property are prohibited once an object is created.

:eql:synopsis:`SET ANNOTATION <annotation-name> := <value>`
    Set property :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`SET ANNOTATION` for details.

:eql:synopsis:`CREATE CONSTRAINT`
    Define a concrete constraint on the property.
    See :eql:stmt:`CREATE CONSTRAINT` for details.


ALTER PROPERTY
==============

:eql-statement:
:eql-haswith:


Change the definition of a :ref:`property <ref_datamodel_props>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    {CREATE | ALTER} {TYPE | LINK} <source> "{"
      [ ... ]
      ALTER PROPERTY <name>
      [ "{" ] <subcommand>; [...] [ "}" ];
      [ ... ]
    "}"


    [ WITH <with-item> [, ...] ]
    ALTER ABSTRACT PROPERTY [<module>::]<name>
    [ "{" ] <subcommand>; [...] [ "}" ];

    # where <subcommand> is one of

      SET default := <expression>
      SET readonly := {true | false}
      RENAME TO <newname>
      EXTENDING ...
      SET SINGLE
      SET MULTI
      ALTER TARGET <typename> [, ...]
      SET ANNOTATION <annotation-name> := <value>
      DROP ANNOTATION <annotation-name>
      CREATE CONSTRAINT <constraint-name> ...
      ALTER CONSTRAINT <constraint-name> ...
      DROP CONSTRAINT <constraint-name> ...


Description
-----------

:eql:synopsis:`{CREATE|ALTER} {TYPE|LINK} ... CREATE PROPERTY` defines a new
concrete property for a given object type or link.

:eql:synopsis:`ALTER ABSTRACT PROPERTY` changes the definition of an abstract
property item.


Parameters
----------

:eql:synopsis:`<source>`
    The name of an object type or link on which the property is defined.
    May be optionally qualified with module.

:eql:synopsis:`<name>`
    The unqualified name of the property to modify.

:eql:synopsis:`<module>`
    Optional name of the module to create or alter the abstract property in.
    If not specified, the current module is used.

The following subcommands are allowed in the ``ALTER LINK`` block:

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the property to :eql:synopsis:`<newname>`.
    All concrete properties inheriting from this property are
    also renamed.

:eql:synopsis:`EXTENDING ...`
    Alter the property parent list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         EXTENDING <name> [, ...]
            [ FIRST | LAST | BEFORE <parent> | AFTER <parent> ]

    This subcommand makes the property a child of the specified list
    of parent property items.  The requirements for the parent-child
    relationship are the same as when creating a property.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``FIRST`` -- insert parent(s) at the beginning of the
      parent list,
    * ``LAST`` -- insert parent(s) at the end of the parent list,
    * ``BEFORE <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``AFTER <parent>`` -- insert parent(s) after an existing
      *parent*.

:eql:synopsis:`SET SINGLE`
    Change the maximum cardinality of the property set to *one*.  Only
    valid for concrete properties.

:eql:synopsis:`SET MULTI`
    Change the maximum cardinality of the property set to
    *greater than one*.  Only valid for concrete properties;

:eql:synopsis:`ALTER TARGET <typename> [, ...]`
    Change the target type of the property to the specified type or
    a union of types.  Only valid for concrete properties.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove property :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.

:eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
    Alter the definition of a constraint for this property.  See
    :eql:stmt:`ALTER CONSTRAINT` for details.

:eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
    Remove a constraint from this property.  See
    :eql:stmt:`DROP CONSTRAINT` for details.

All the subcommands allowed in the ``CREATE PROPERTY`` block are also
valid subcommands for ``ALTER PROPERTY`` block.


DROP PROPERTY
=============

:eql-statement:
:eql-haswith:

Remove a :ref:`property <ref_datamodel_props>` from the
schema.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} TYPE <TypeName> "{"
      [ ... ]
      DROP LINK <name>
      [ ... ]
    "}"


    [ WITH <with-item> [, ...] ]
    DROP ABSTRACT PROPERTY <name> ;

Description
-----------

:eql:synopsis:`ALTER {TYPE|LINK} DROP PROPERTY` removes the specified property
from its containing object type or link.  All properties that inherit from this
property are also removed.

:eql:synopsis:`DROP ABSTRACT PROPERTY` removes the specified abstract
property item the schema.

Examples
--------

Remove property ``rank`` from abstract link ``favorites``:

.. code-block:: edgeql

    ALTER ABSTRACT LINK favorites {
        DROP PROPERTY rank;
    };
