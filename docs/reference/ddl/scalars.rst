.. _ref_eql_ddl_scalars:

============
Scalar Types
============

This section describes the DDL commands pertaining to
:ref:`scalar types <ref_datamodel_scalar_types>`.


Create scalar type
==================

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_scalars>` a new scalar type.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    create [abstract] scalar type <name> [ extending <supertype> ]
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      create annotation <annotation-name> := <value>
      create constraint <constraint-name> ...


Description
-----------

The command ``create scalar type`` defines a new scalar type for use in the
current :versionreplace:`database;5.0:branch`.

If *name* is qualified with a module name, then the type is created
in that module, otherwise it is created in the current module.
The type name must be distinct from that of any existing schema item
in the module.

If the ``abstract`` keyword is specified, the created type will be
*abstract*.

All non-abstract scalar types must have an underlying core
implementation.  For user-defined scalar types this means that
``create scalar type`` must have another non-abstract scalar type
as its *supertype*.

The most common use of ``create scalar type`` is to define a scalar
subtype with constraints.

Most sub-commands and options of this command are identical to the
:ref:`SDL scalar type declaration <ref_eql_sdl_scalars_syntax>`. The
following subcommands are allowed in the ``create scalar type``
block:

:eql:synopsis:`create annotation <annotation-name> := <value>;`
    Set scalar type's :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`create constraint <constraint-name> ...`
    Define a new constraint for this scalar type.  See
    :eql:stmt:`create constraint` for details.


Examples
--------

Create a new non-negative integer type:

.. code-block:: edgeql

    create scalar type posint64 extending int64 {
        create constraint min_value(0);
    };


Create a new enumerated type:

.. code-block:: edgeql

    create scalar type Color
        extending enum<Black, White, Red>;


Alter scalar type
=================

:eql-statement:
:eql-haswith:


Alter the definition of a :ref:`scalar type <ref_datamodel_scalar_types>`.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter scalar type <name>
    "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      rename to <newname>
      extending ...
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>
      create constraint <constraint-name> ...
      alter constraint <constraint-name> ...
      drop constraint <constraint-name> ...


Description
-----------

The command ``alter scalar type`` changes the definition of a scalar type.
*name* must be a name of an existing scalar type, optionally qualified
with a module name.

The following subcommands are allowed in the ``alter scalar type`` block:

:eql:synopsis:`rename to <newname>;`
    Change the name of the scalar type to *newname*.

:eql:synopsis:`extending ...`
    Alter the supertype list.  It works the same way as in
    :eql:stmt:`alter type`.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter scalar type :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>`
    Remove scalar type's :eql:synopsis:`<annotation-name>` from
    :eql:synopsis:`<value>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`alter constraint <constraint-name> ...`
    Alter the definition of a constraint for this scalar type.  See
    :eql:stmt:`alter constraint` for details.

:eql:synopsis:`drop constraint <constraint-name>`
    Remove a constraint from this scalar type.  See
    :eql:stmt:`drop constraint` for details.

All the subcommands allowed in the ``create scalar type`` block are also
valid subcommands for ``alter scalar type`` block.


Examples
--------

Define a new constraint on a scalar type:

.. code-block:: edgeql

    alter scalar type posint64 {
        create constraint max_value(100);
    };

Add one more label to an enumerated type:

.. code-block:: edgeql

    alter scalar type Color
        extending enum<Black, White, Red, Green>;


Drop scalar type
================

:eql-statement:
:eql-haswith:


Remove a scalar type.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    drop scalar type <name> ;


Description
-----------

The command ``drop scalar type`` removes a scalar type.


Parameters
----------

*name*
    The name (optionally qualified with a module name) of an existing
    scalar type.


Example
-------

Remove a scalar type:

.. code-block:: edgeql

    drop scalar type posint64;
