.. _ref_eql_ddl_scalars:

============
Scalar Types
============

This section describes the DDL commands pertaining to
:ref:`scalar types <ref_datamodel_scalar_types>`.


CREATE SCALAR TYPE
==================

:eql-statement:
:eql-haswith:

Define a new :ref:`scalar type <ref_eql_sdl_scalars>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE [ABSTRACT] SCALAR TYPE <name> [ EXTENDING <supertype> ]
    [ "{" <action>; [...] "}" ] ;


Description
-----------

``CREATE SCALAR TYPE`` defines a new scalar type for use in the
current database.

If *name* is qualified with a module name, then the type is created
in that module, otherwise it is created in the current module.
The type name must be distinct from that of any existing schema item
in the module.

If the ``ABSTRACT`` keyword is specified, the created type will be
*abstract*.

All non-abstract scalar types must have an underlying core
implementation.  For user-defined scalar types this means that
``CREATE SCALAR TYPE`` must specify another non-abstract scalar type
as its *supertype*.

The most common use of ``CREATE SCALAR TYPE`` is to define a scalar
subtype with constraints.

:eql:synopsis:`EXTENDING <supertype>`
    Optional clause specifying the *supertype* of the new type.

    Use of ``EXTENDING`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

:eql:synopsis:`<action>`
    The following actions are allowed in the ``CREATE SCALAR TYPE``
    block:

    :eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
        Set link item's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.

    :eql:synopsis:`CREATE CONSTRAINT`
        Define a concrete constraint on the scalar type.
        See :eql:stmt:`CREATE CONSTRAINT` for details.


Examples
--------

Create a new non-negative integer type:

.. code-block:: edgeql

    CREATE SCALAR TYPE posint64 EXTENDING int64 {
        CREATE CONSTRAINT min(0);
    };


ALTER SCALAR TYPE
=================

:eql-statement:
:eql-haswith:


Alter the definition of a :ref:`scalar type <ref_datamodel_scalar_types>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER SCALAR TYPE <name>
    "{" <action>; [...] "}" ;


Description
-----------

``ALTER SCALAR TYPE`` changes the definition of a scalar type.
*name* must be a name of an existing scalar type, optionally qualified
with a module name.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``ALTER SCALAR TYPE`` block:

    :eql:synopsis:`RENAME TO <newname>;`
        Change the name of the scalar type to *newname*.

    :eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
        Set scalar type's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.

    :eql:synopsis:`DROP ATTRIBUTE <attribute>;`
        Remove scalar type's *attribute* to *value*.
        See :eql:stmt:`DROP ATTRIBUTE <DROP ATTRIBUTE>` for details.

    :eql:synopsis:`CREATE CONSTRAINT <constraint-name> ...`
        Define a new constraint for this scalar type.  See
        :eql:stmt:`CREATE CONSTRAINT` for details.

    :eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
        Alter the definition of a constraint for this scalar type.  See
        :eql:stmt:`ALTER CONSTRAINT` for details.

    :eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
        Remove a constraint from this scalar type.  See
        :eql:stmt:`DROP CONSTRAINT` for details.


Examples
--------

Define a new constraint on a scalar type:

.. code-block:: edgeql

    ALTER SCALAR TYPE posint64 {
        CREATE CONSTRAINT max(100);
    };


DROP SCALAR TYPE
================

:eql-statement:
:eql-haswith:


Remove a scalar type.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP SCALAR TYPE <name> ;


Description
-----------

``DROP SCALAR TYPE`` removes a scalar type.


Parameters
----------

*name*
    The name (optionally qualified with a module name) of an existing
    scalar type.


Examples
--------

Remove a scalar type:

.. code-block:: edgeql

    DROP SCALAR TYPE posint64;
