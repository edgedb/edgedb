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
      CREATE [{REQUIRED | OPTIONAL}] [{SINGLE | MULTI}]
        PROPERTY <name>
        [ EXTENDING <base> [, ...] ] -> <type>
        [ "{" <subcommand>; [...] "}" ] ;
      [ ... ]
    "}"

    # Computable property form:

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} {TYPE|LINK} <SourceName> "{"
      [ ... ]
      CREATE [{REQUIRED | OPTIONAL}] [{SINGLE | MULTI}]
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
      CREATE ANNOTATION <annotation-name> := <value>
      CREATE CONSTRAINT <constraint-name> ...


Description
-----------

:eql:synopsis:`{CREATE|ALTER} {TYPE|LINK} ... CREATE PROPERTY` defines a new
concrete property for a given object type or link.

There are three forms of ``CREATE PROPERTY``, as shown in the syntax synopsis
above.  The first form is the canonical definition form, the second
form is a syntax shorthand for defining a
:ref:`computable property <ref_datamodel_computables>`, and the third
is a form to define an abstract property item.  The abstract form
allows creating the property in the specified
:eql:synopsis:`<module>`.  Concrete property forms are always
created in the same module as the containing object or property.

.. _ref_eql_ddl_props_syntax:

Parameters
----------

:eql:synopsis:`REQUIRED`
    If specified, the property is considered *required* for the parent
    object type.  It is an error for an object to have a required
    property resolve to an empty value.  Child properties **always**
    inherit the *required* attribute, i.e it is not possible to make a
    required property non-required by extending it.

:eql:synopsis:`OPTIONAL`
    This is the default qualifier assumed when no qualifier is
    specified, but it can also be specified explicitly. The property
    is considered *optional* for the parent object type, i.e. it is
    possible for the property to resolve to an empty value.

:eql:synopsis:`MULTI`
    Specifies that there may be more than one instance of this property
    in an object, in other words, ``Object.property`` may resolve to a set
    of a size greater than one.

:eql:synopsis:`SINGLE`
    Specifies that there may be at most *one* instance of this property
    in an object, in other words, ``Object.property`` may resolve to a set
    of a size not greater than one.  ``SINGLE`` is assumed if nether
    ``MULTI`` nor ``SINGLE`` qualifier is specified.

:eql:synopsis:`EXTENDING <base> [, ...]`
    Optional clause specifying the *parents* of the new property item.

    Use of ``EXTENDING`` creates a persistent schema relationship
    between the new property and its parents.  Schema modifications
    to the parent(s) propagate to the child.

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
    of this property are prohibited once an object is created.  All of the
    derived properties **must** preserve the original *read-only* value.

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>`
    Set property :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`CREATE ANNOTATION` for details.

:eql:synopsis:`CREATE CONSTRAINT`
    Define a concrete constraint on the property.
    See :eql:stmt:`CREATE CONSTRAINT` for details.


Examples
--------

Define a new link ``address`` on the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE PROPERTY address -> str
    };

Define a new property ``number_of_connections`` as a
:ref:`computable <ref_datamodel_computables>` on the ``User``
object type counting the number of interests:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE PROPERTY number_of_connections :=
            count(.interests)
    };

Define a new abstract link ``orderable`` with ``weight`` property:

.. code-block:: edgeql

    CREATE ABSTRACT LINK orderable {
        CREATE PROPERTY weight -> std::int64
    };


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
      RESET default
      SET readonly := {true | false}
      RESET readonly
      RENAME TO <newname>
      EXTENDING ...
      SET REQUIRED
      SET OPTIONAL
      RESET OPTIONALITY
      SET SINGLE
      SET MULTI
      RESET CARDINALITY
      SET TYPE <typename> [USING (<conversion-expr)]
      RESET TYPE
      USING (<computable-expr>)
      CREATE ANNOTATION <annotation-name> := <value>
      ALTER ANNOTATION <annotation-name> := <value>
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

:eql:synopsis:`SET REQUIRED`
    Make the property *required*.

:eql:synopsis:`SET OPTIONAL`
    Make the property no longer *required* (i.e. make it *optional*).

:eql:synopsis:`RESET OPTIONALITY`
    Reset the optionality of the property to the default value (``OPTIONAL``),
    or, if the property is inherited, to the value inherited from properties in
    supertypes.

:eql:synopsis:`SET SINGLE`
    Change the maximum cardinality of the property set to *one*.  Only
    valid for concrete properties.

:eql:synopsis:`SET MULTI`
    Change the maximum cardinality of the property set to
    *greater than one*.  Only valid for concrete properties;

:eql:synopsis:`RESET CARDINALITY`
    Reset the maximum cardinality of the property to the default value
    (``SINGLE``), or, if the property is inherited, to the value inherited
    from properties in supertypes.

:eql:synopsis:`SET TYPE <typename> [USING (<conversion-expr)]`
    Change the type of the property to the specified
    :eql:synopsis:`<typename>`.  The optional ``USING`` clause specifies
    a conversion expression that computes the new property value from the old.
    The conversion expression must return a singleton set and is evaluated
    on each element of ``MULTI`` properties.  A ``USING`` clause must be
    provided if there is no implicit or assignment cast from old to new type.

:eql:synopsis:`RESET TYPE`
    Reset the type of the property to the type inherited from properties
    of the same name in supertypes.  It is an error to ``RESET TYPE`` on
    a property that is not inherited.

:eql:synopsis:`USING (<computable-expr>)`
    Change the expression of a :ref:`computable <ref_datamodel_computables>`
    property.  Only valid for concrete properties.

:eql:synopsis:`ALTER ANNOTATION <annotation-name>;`
    Alter property annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`ALTER ANNOTATION <ALTER ANNOTATION>` for details.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove property :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.

:eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
    Alter the definition of a constraint for this property.  See
    :eql:stmt:`ALTER CONSTRAINT` for details.

:eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
    Remove a constraint from this property.  See
    :eql:stmt:`DROP CONSTRAINT` for details.

:eql:synopsis:`RESET default`
    Remove the default value from this property, or reset it to the value
    inherited from a supertype, if the property is inherited.

:eql:synopsis:`RESET readonly`
    Set property writability to the default value (writable), or, if the
    property is inherited, to the value inherited from properties in
    supertypes.

All the subcommands allowed in the ``CREATE PROPERTY`` block are also
valid subcommands for ``ALTER PROPERTY`` block.


Examples
--------

Set the ``title`` annotation of property ``address`` of object type
``User`` to ``"Home address"``:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER PROPERTY address
            CREATE ANNOTATION title := "Home address";
    };

Add a maximum-length constraint to property ``address`` of object type
``User``:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER PROPERTY address {
            CREATE CONSTRAINT max_len_value(500);
        };
    };

Rename the property ``weight`` of link ``orderable`` to ``sort_by``:

.. code-block:: edgeql

    ALTER ABSTRACT LINK orderable {
        ALTER PROPERTY weight RENAME TO sort_by;
    };

Redefine the :ref:`computable <ref_datamodel_computables>` property
``number_of_connections`` to be the number of friends:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER PROPERTY number_of_connections USING (
            count(.friends)
        )
    };


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
property item from the schema.

Example
-------

Remove property ``address`` from type ``User``:

.. code-block:: edgeql

    ALTER TYPE User {
        DROP PROPERTY address;
    };
