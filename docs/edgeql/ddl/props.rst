.. _ref_eql_ddl_props:

==========
Properties
==========

This section describes the DDL commands pertaining to
:ref:`properties <ref_datamodel_props>`.


CREATE ABSTRACT PROPERTY
========================

:eql-statement:
:eql-haswith:

Define a new :ref:`abstract property <ref_datamodel_props>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE ABSTRACT PROPERTY <name> [ EXTENDING <base> [, ...] ]
    [ "{" <action>; [...] "}" ] ;

Description
-----------

``CREATE ABSTRACT PROPERTY`` defines a new abstract property
item.

If *name* is qualified with a module name, then the property item
is created in that module, otherwise it is created in the current module.
The property name must be distinct from that of any existing schema
item in the module.

:eql:synopsis:`EXTENDING <base> [, ...]`
    Optional clause specifying the *parents* of the new property item.

    Use of ``EXTENDING`` creates a persistent schema relationship
    between the new property and its parents.  Schema modifications
    to the parent(s) propagate to the child.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``CREATE ABSTRACT PROPERTY`` block:

    ``SET ATTRIBUTE <attribute> := <value>;``
        Set link item's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.


ALTER ABSTRACT PROPERTY
=======================

:eql-statement:
:eql-haswith:


Change the definition of an
:ref:`abstract property <ref_datamodel_props>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER ABSTRACT PROPERTY <name>
    "{" <action>; [...] "}" ;

Description
-----------

``ALTER ABSTRACT PROPERTY`` changes the definition of an abstract
property item.  *name* must be a name of an existing abstract
property, optionally qualified with a module name.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``ALTER ABSTRACT PROPERTY`` block:

    :eql:synopsis:`RENAME TO <newname>;`
        Change the name of the property item to *newname*.  All
        concrete link properties inheriting from this property are
        also renamed.

    :eql:synopsis:`EXTENDING ...`
        Alter the property parent list.
        The full syntax of this action is:

        .. eql:synopsis::

             EXTENDING <name> [, ...]
                [ FIRST | LAST | BEFORE <parent> | AFTER <parent> ]

        This action makes the property item a child of the specified
        list of parent property items.  The requirements for the
        parent-child relationship are the same as when creating
        a property.

        It is possible to specify the position in the parent list
        using the following optional keywords:

        * ``FIRST`` -- insert parent(s) at the beginning of the
          parent list,
        * ``LAST`` -- insert parent(s) at the end of the parent list,
        * ``BEFORE <parent>`` -- insert parent(s) before an
          existing *parent*,
        * ``AFTER <parent>`` -- insert parent(s) after an existing
          *parent*.

    :eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
        Set link item's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.

    :eql:synopsis:`DROP ATTRIBUTE <attribute>;`
        Remove link item's *attribute* to *value*.
        See :eql:stmt:`DROP ATTRIBUTE <DROP ATTRIBUTE>` for details.

    :eql:synopsis:`ALTER TARGET <typename>`
        Change the target type of the property to the specified type.

    :eql:synopsis:`CREATE CONSTRAINT <constraint-name> ...`
        Define a new constraint for this property.
        See :eql:stmt:`CREATE CONSTRAINT` for details.

    :eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
        Alter the definition of a constraint for this property.
        See :eql:stmt:`ALTER CONSTRAINT` for details.

    :eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
        Remove a constraint from this property.
        See :eql:stmt:`DROP CONSTRAINT` for details.


DROP ABSTRACT PROPERTY
======================

:eql-statement:
:eql-haswith:

Remove an :ref:`abstract property <ref_datamodel_props>` from the
schema.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP ABSTRACT PROPERTY <name> ;


Description
-----------

``DROP ABSTRACT PROPERTY`` removes an existing property item
from the database schema.


Examples
--------

Drop the abstract property ``rank``:

.. code-block:: edgeql

    DROP ABSTRACT PROPERTY rank;


CREATE PROPERTY
===============

:eql-statement:
:eql-haswith:

Define a concrete property on the specified link.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE [{SINGLE | MULTI}] PROPERTY <name> -> <type>
    [ "{" <action>; [...] "}" ] ;

    [ WITH <with-item> [, ...] ]
    CREATE [{SINGLE | MULTI}] PROPERTY <name> := <expression> ;

Description
-----------

``CREATE PROPERTY`` defines a new concrete property for a
given link.

There are two forms of ``CREATE PROPERTY``, as shown in the syntax
synopsis above.  The first form is the canonical definition form, and
the second form is a syntax shorthand for defining a
:ref:`computable property <ref_datamodel_computables>`.


Canonical Form
--------------

The canonical form of ``CREATE PROPERTY`` defines a concrete
property with the given *name* and referring to the *typename* type.

The optional ``SINGLE`` and ``MULTI`` qualifiers specify how many
instances of the property are allowed per object.  ``SINGLE`` specifies that
there may be at most *one* instance, and ``MULTI`` specifies that there may
be more than one.  ``SINGLE`` is the default.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``CREATE PROPERTY`` block:

    :eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
        Set link item's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.


Computable Property Form
------------------------

The computable form of ``CREATE PROPERTY`` defines a concrete
*computable* property with the given *name*.  The type of the
property is inferred from the *expression*.


ALTER PROPERTY
==============

:eql-statement:
:eql-haswith:

Alter the definition of a concrete property on the specified link.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER PROPERTY <name>
    "{" <action>; [...] "}" ;

    [ WITH <with-item> [, ...] ]
    ALTER PROPERTY <name> <action> ;


Description
-----------

There are two forms of ``ALTER LINK``, as shown in the synopsis above.
The first is the canonical form, which allows specifying multiple
alter actions, while the second form is a shorthand for a single
alter action.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``ALTER PROPERTY`` block:

    :eql:synopsis:`RENAME TO <newname>;`
        Change the name of the concrete link to *newname*.  Renaming
        *inherited* links is not allowed, only non-inherited concrete
        links can be renamed.  When a concrete or abstract link is
        renamed, all concrete links that inherit from it are also
        renamed.

    :eql:synopsis:`SET SINGLE`
        Change the maximum cardinality of the property set to *one*.

    :eql:synopsis:`SET MULTI`
        Change the maximum cardinality of the property set to
        *greater then one*.

    :eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
        Set link item's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.

    :eql:synopsis:`DROP ATTRIBUTE <attribute>;`
        Remove link item's *attribute* to *value*.
        See :eql:stmt:`DROP ATTRIBUTE <DROP ATTRIBUTE>` for details.

    :eql:synopsis:`CREATE PROPERTY <property-name> ...`
        Define a new property item for this link.  See
        :eql:stmt:`CREATE PROPERTY` for details.

    :eql:synopsis:`ALTER PROPERTY <property-name> ...`
        Alter the definition of a property item for this link.  See
        :eql:stmt:`ALTER PROPERTY` for details.

    :eql:synopsis:`DROP PROPERTY <property-name>;`
        Remove a property item from this link.  See
        :eql:stmt:`DROP PROPERTY` for details.

Examples
--------

Set the ``title`` attribute of property ``rank`` of abstract
link ``favorites`` to ``"Rank"``:

.. code-block:: edgeql

    ALTER ABSTRACT LINK favorites {
        ALTER PROPERTY rank SET ATTRIBUTE title := "Rank";
    };


DROP PROPERTY
=============

:eql-statement:
:eql-haswith:


Remove a concrete property from the specified link.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP PROPERTY <name> ;

Description
-----------

``DROP PROPERTY`` removes the specified property from its
containing link.  All link properties that inherit from this link
property are also removed.

Examples
--------

Remove property ``rank`` from abstract link ``favorites``:

.. code-block:: edgeql

    ALTER ABSTRACT LINK favorites {
        DROP PROPERTY rank;
    };
