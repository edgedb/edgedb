.. _ref_eql_ddl_object_types:

============
Object Types
============

This section describes the DDL commands pertaining to
:ref:`object types <ref_datamodel_object_types>`.


CREATE TYPE
===========

:eql-statement:
:eql-haswith:


:ref:`Define <ref_eql_sdl_object_types>` a new object type.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE [ABSTRACT] TYPE <name> [ EXTENDING <supertype> [, ...] ]
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      SET ATTRIBUTE <attribute> := <value>
      CREATE LINK <link-name> ...
      CREATE PROPERTY <property-name> ...
      CREATE INDEX <index-name> ON <index-expr>

Description
-----------

``CREATE TYPE`` defines a new object type for use in the current database.

If *name* is qualified with a module name, then the type is created
in that module, otherwise it is created in the current module.
The type name must be distinct from that of any existing schema item
in the module.

Parameters
----------

:eql:synopsis:`ABSTRACT`
    If specified, the created type will be *abstract*.

:eql:synopsis:`WITH <with-item> [, ...]`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases
    that can be referenced by the command.  See :ref:`ref_eql_with`
    for more information.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the new type.

:eql:synopsis:`EXTENDING <supertype> [, ...]`
    Optional clause specifying the *supertypes* of the new type.

    Use of ``EXTENDING`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

    References to supertypes in queries will also include objects of
    the subtype.

    If the same *link* name exists in more than one supertype, or
    is explicitly defined in the subtype and at least one supertype,
    then the data types of the link targets must be *compatible*.
    If there is no conflict, the links are merged to form a single
    link in the new type.

The following subcommands are allowed in the ``CREATE TYPE`` block:

:eql:synopsis:`SET ATTRIBUTE <attribute> := <value>`
    Set object type *attribute* to *value*.
    See :eql:stmt:`SET ATTRIBUTE` for details.

:eql:synopsis:`CREATE LINK <link-name> ...`
    Define a new link for this object type.  See
    :eql:stmt:`CREATE LINK` for details.

:eql:synopsis:`CREATE PROPERTY <property-name> ...`
    Define a new property for this object type.  See
    :eql:stmt:`CREATE PROPERTY` for details.

:eql:synopsis:`CREATE INDEX <index-name> ON <index-expr>`
    Define a new :ref:`index <ref_datamodel_indexes>` named
    *index-name* using *index-expr* for this object type.  See
    :eql:stmt:`CREATE INDEX` for details.

.. TODO: write examples


.. _ref_eql_ddl_object_types_alter:

ALTER TYPE
==========

:eql-statement:
:eql-haswith:


Change the definition of an
:ref:`object type <ref_datamodel_object_types>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER TYPE <name>
    [ "{" <subcommand>; [...] "}" ] ;

    [ WITH <with-item> [, ...] ]
    ALTER TYPE <name> <subcommand> ;

    # where <subcommand> is one of

      RENAME TO <newname>
      EXTENDING <parent> [, ...]
      SET ATTRIBUTE <attribute> := <value>
      DROP ATTRIBUTE <attribute>
      CREATE LINK <link-name> ...
      ALTER LINK <link-name> ...
      DROP LINK <link-name> ...
      CREATE PROPERTY <property-name> ...
      ALTER PROPERTY <property-name> ...
      DROP PROPERTY <property-name> ...
      CREATE INDEX <index-name> ON <index-expr>
      DROP INDEX <index-name>


Description
-----------

``ALTER TYPE`` changes the definition of an object type.
*name* must be a name of an existing object type, optionally qualified
with a module name.

Parameters
----------

The following subcommands are allowed in the ``ALTER TYPE`` block:

:eql:synopsis:`WITH <with-item> [, ...]`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases
    that can be referenced by the command.  See :ref:`ref_eql_with`
    for more information.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the type being altered.

:eql:synopsis:`EXTENDING <parent> [, ...]`
    Alter the supertype list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         EXTENDING <parent> [, ...]
            [ FIRST | LAST | BEFORE <exparent> | AFTER <exparent> ]

    This subcommand makes the type a subtype of the specified list
    of supertypes.  The requirements for the parent-child relationship
    are the same as when creating an object type.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``FIRST`` -- insert parent(s) at the beginning of the
      parent list,
    * ``LAST`` -- insert parent(s) at the end of the parent list,
    * ``BEFORE <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``AFTER <parent>`` -- insert parent(s) after an existing
      *parent*.

:eql:synopsis:`DROP ATTRIBUTE <attribute>`
    Remove object type *attribute*.
    See :eql:stmt:`DROP ATTRIBUTE <DROP ATTRIBUTE>` for details.

:eql:synopsis:`ALTER LINK <link-name> ...`
    Alter the definition of a link for this object type.  See
    :eql:stmt:`ALTER LINK` for details.

:eql:synopsis:`DROP LINK <link-name>`
    Remove a link item from this object type.  See
    :eql:stmt:`DROP LINK` for details.

:eql:synopsis:`ALTER PROPERTY <property-name> ...`
    Alter the definition of a property item for this object type.
    See :eql:stmt:`ALTER PROPERTY` for details.

:eql:synopsis:`DROP PROPERTY <property-name>`
    Remove a property item from this object type.  See
    :eql:stmt:`DROP PROPERTY` for details.

:eql:synopsis:`DROP INDEX <index-name>`
    Remove an :ref:`index <ref_datamodel_indexes>` named *index-name*
    from this object type.  See :eql:stmt:`DROP INDEX` for details.

All the subcommands allowed in the ``CREATE TYPE`` block are also
valid subcommands for ``ALTER TYPE`` block.


.. TODO: write examples


DROP TYPE
=========

:eql-statement:
:eql-haswith:


Remove the specified object type from the schema.

.. eql:synopsis::

    DROP TYPE <name> ;

Description
-----------

``DROP TYPE`` removes the specified object type from the schema.
schema.  All subordinate schema items defined on this type, such
as links and indexes, are removed as well.

Examples
--------

Remove the ``User`` object type:

.. code-block:: edgeql

    DROP TYPE User;
