.. _ref_eql_ddl_links:

=====
Links
=====

This section describes the DDL commands pertaining to
:ref:`links <ref_datamodel_links>`.


CREATE ABSTRACT LINK
====================

:eql-statement:
:eql-haswith:

Define a new :ref:`abstract link <ref_datamodel_links>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE ABSTRACT LINK <name> [ EXTENDING <base> [, ...] ]
    [ "{" <action>; [...] "}" ] ;

Description
-----------

``CREATE ABSTRACT LINK`` defines a new abstract link item.

If *name* is qualified with a module name, then the link item is created
in that module, otherwise it is created in the current module.
The link name must be distinct from that of any existing schema item
in the module.

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

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``CREATE ABSTRACT LINK`` block:

    :eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
        Set link item's *attribute* to *value*.
        See :eql:stmt:`SET ATTRIBUTE` for details.

    :eql:synopsis:`CREATE PROPERTY`
        Define a concrete property on the link.
        See :eql:stmt:`CREATE PROPERTY` for details.

    :eql:synopsis:`CREATE CONSTRAINT`
        Define a concrete constraint on the link.
        See :eql:stmt:`CREATE CONSTRAINT` for details.


ALTER ABSTRACT LINK
===================

:eql-statement:
:eql-haswith:


Change the definition of an :ref:`abstract link <ref_datamodel_links>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER ABSTRACT LINK <name>
    "{" <action>; [...] "}" ;


Description
-----------

``ALTER ABSTRACT LINK`` changes the definition of an abstract link item.
*name* must be a name of an existing abstract link, optionally qualified
with a module name.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``ALTER ABSTRACT LINK`` block:

    :eql:synopsis:`RENAME TO <newname>;`
        Change the name of the link item to *newname*.  All concrete links
        inheriting from this links are also renamed.

    :eql:synopsis:`EXTENDING ...`
        Alter the link parent list.  The full syntax of this action is:

        .. eql:synopsis::

             EXTENDING <name> [, ...]
                [ FIRST | LAST | BEFORE <parent> | AFTER <parent> ]

        This action makes the link item a child of the specified list
        of parent link items.  The requirements for the parent-child
        relationship are the same as when creating a link.

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
        Remove link item's *attribute*.
        See :eql:stmt:`DROP ATTRIBUTE <DROP ATTRIBUTE>` for details.

    :eql:synopsis:`ALTER TARGET <typename> [, ...]`
        Change the target type of the link to the specified type or
        a union of types.

    :eql:synopsis:`CREATE PROPERTY <property-name> ...`
        Define a new property item for this link.  See
        :eql:stmt:`CREATE PROPERTY` for details.

    :eql:synopsis:`ALTER PROPERTY <property-name> ...`
        Alter the definition of a property item for this link.  See
        :eql:stmt:`ALTER PROPERTY` for details.

    :eql:synopsis:`DROP PROPERTY <property-name>;`
        Remove a property item from this link.  See
        :eql:stmt:`DROP PROPERTY` for details.

    :eql:synopsis:`CREATE CONSTRAINT <constraint-name> ...`
        Define a new constraint for this link.  See
        :eql:stmt:`CREATE CONSTRAINT` for details.

    :eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
        Alter the definition of a constraint for this link.  See
        :eql:stmt:`ALTER CONSTRAINT` for details.

    :eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
        Remove a constraint from this link.  See
        :eql:stmt:`DROP CONSTRAINT` for details.


DROP ABSTRACT LINK
==================

:eql-statement:
:eql-haswith:


Remove an :ref:`abstract link <ref_datamodel_links>` from the schema.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP ABSTRACT LINK <name> ;


Description
-----------

``DROP ABSTRACT LINK`` removes an existing link item from the database
schema.  All subordinate schema items defined on this link, such
as link properties and constraints, are removed as well.


Examples
--------

Drop the link ``friends``:

.. code-block:: edgeql

    DROP ABSTRACT LINK friends;


CREATE LINK
===========

:eql-statement:
:eql-haswith:


Define a new :ref:`concrete link <ref_datamodel_links>` for the
specified *object type*.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE [ REQUIRED ] [{SINGLE | MULTI}] LINK <name> TO <type>
    [ "{" <action>; [...] "}" ] ;

    [ WITH <with-item> [, ...] ]
    CREATE [ REQUIRED ] [{SINGLE | MULTI}] LINK <name> := <expression> ;


Description
-----------

``CREATE LINK`` defines a new concrete link for a given object type.

There are two forms of ``CREATE LINK``, as shown in the syntax synopsis
above.  The first form is the canonical definition form, and the second
form is a syntax shorthand for defining a
:ref:`computable link <ref_datamodel_computables>`.


Canonical Form
--------------

The canonical form of ``CREATE LINK`` defines a concrete link *name*
referring to the *typename* type.  If the optional ``REQUIRED``
keyword is specified, the link is considered required.

The optional ``SINGLE`` and ``MULTI`` qualifiers specify how many
instances of the link are allowed per object.  ``SINGLE`` specifies that
there may be at most *one* instance, and ``MULTI`` specifies that there may
be more than one.  ``SINGLE`` is the default.

:eql:synopsis:`<action>`
    The following actions are allowed in the ``CREATE LINK`` block:

    * :eql:stmt:`SET ATTRIBUTE`
    * ``ON TARGET DELETE RESTRICT``
    * ``ON TARGET DELETE ALLOW``
    * ``ON TARGET DELETE DELETE SOURCE``
    * ``ON TARGET DELETE DEFERRED RESTRICT``

    The details of what ``ON TARGET DELETE`` options mean are
    described in :ref:`this section <ref_datamodel_links>`.


Computable Link Form
--------------------

The computable form of ``CREATE LINK`` defines a concrete *computable*
link *name*.  The type of the link is inferred from the *expression*.


Examples
--------

Define a new string link ``interests`` on the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE MULTI LINK interests -> str;
    };

Define a new computable link ``followers_count`` on the
``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE LINK friends_count := count(__source__.friends);
    };


ALTER LINK
==========

:eql-statement:
:eql-haswith:


Change the definition of a :ref:`concrete link <ref_datamodel_links>`
on a given object type.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER LINK <name>
    "{" <action>; [...] "}" ;

    [ WITH <with-item> [, ...] ]
    ALTER LINK <name> <action> ;


Description
-----------

There are two forms of ``ALTER LINK``, as shown in the synopsis above.
The first is the canonical form, which allows specifying multiple
alter actions, while the second form is a shorthand for a single
alter action.

:eql:synopsis:`<action>`
    The following actions are allowed in the
    ``ALTER LINK`` block:

    :eql:synopsis:`RENAME TO <newname>;`
        Change the name of the concrete link to *newname*.  Renaming
        *inherited* links is not allowed, only non-inherited concrete
        links can be renamed.  When a concrete or abstract link is
        renamed, all concrete links that inherit from it are also
        renamed.

    :eql:synopsis:`SET SINGLE`
        Change the maximum cardinality of the link set to *one*.

    :eql:synopsis:`SET MULTI`
        Change the maximum cardinality of the link set to *greater then one*.

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

    :eql:synopsis:`CREATE CONSTRAINT <constraint-name> ...`
        Define a new constraint for this link.  See
        :eql:stmt:`CREATE CONSTRAINT` for details.

    :eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
        Alter the definition of a constraint for this link.  See
        :eql:stmt:`ALTER CONSTRAINT` for details.

    :eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
        Remove a constraint from this link.  See
        :eql:stmt:`DROP CONSTRAINT` for details.


Examples
--------

Set the ``title`` attribute of link ``interests`` of object type ``User``
``"Interests"``:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER LINK interests SET ATTRIBUTE title := "Interests";
    };

Add a minimum-length constraint to link ``name`` of object type ``User``:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER LINK name {
            CREATE CONSTRAINT minlength(3);
        };
    };


DROP LINK
=========

:eql-statement:
:eql-haswith:


Remove a concrete link from the specified object type.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP LINK <name> ;

Description
-----------

``DROP LINK`` removes the specified link from its
containing object type.  All links that inherit from this link
are also removed.

Examples
--------

Remove link ``interests`` from object type ``User``:

.. code-block:: edgeql

    ALTER TYPE User DROP LINK interests;
