.. _ref_eql_ddl_links:

=====
Links
=====

This section describes the DDL commands pertaining to
:ref:`links <ref_datamodel_links>`.


CREATE LINK
===========

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_links>` a new link.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} TYPE <TypeName> "{"
      [ ... ]
      CREATE [{REQUIRED | OPTIONAL}] [{SINGLE | MULTI}]
        LINK <name>
        [ EXTENDING <base> [, ...] ] -> <type>
        [ "{" <subcommand>; [...] "}" ] ;
      [ ... ]
    "}"

    # Computable link form:

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} TYPE <TypeName> "{"
      [ ... ]
      CREATE [{REQUIRED | OPTIONAL}] [{SINGLE | MULTI}]
        LINK <name> := <expression>;
      [ ... ]
    "}"

    # Abstract link form:

    [ WITH <with-item> [, ...] ]
    CREATE ABSTRACT LINK [<module>::]<name> [EXTENDING <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]

    # where <subcommand> is one of

      SET default := <expression>
      SET readonly := {true | false}
      CREATE ANNOTATION <annotation-name> := <value>
      CREATE PROPERTY <property-name> ...
      CREATE CONSTRAINT <constraint-name> ...
      ON TARGET DELETE <action>
      CREATE INDEX ON <index-expr>


Description
-----------

``CREATE TYPE ... CREATE LINK`` and ``ALTER TYPE ... CREATE LINK`` define
a new concrete link for a given object type.

There are three forms of ``CREATE LINK``, as shown in the syntax synopsis
above.  The first form is the canonical definition form, the second
form is a syntax shorthand for defining a
:ref:`computable link <ref_datamodel_computables>`, and the third is a
form to define an abstract link item.  The abstract form allows creating
the link in the specified :eql:synopsis:`<module>`.  Concrete link forms
are always created in the same module as the containing object type.


.. _ref_eql_ddl_links_syntax:

Parameters
----------

:eql:synopsis:`REQUIRED`
    If specified, the link is considered *required* for the parent
    object type.  It is an error for an object to have a required
    link resolve to an empty value.  Child links **always** inherit
    the *required* attribute, i.e it is not possible to make a
    required link non-required by extending it.

:eql:synopsis:`OPTIONAL`
    This is the default qualifier assumed when no qualifier is
    specified, but it can also be specified explicitly. The link is
    considered *optional* for the parent object type, i.e. it is
    possible for the link to resolve to an empty value.

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

The following subcommands are allowed in the ``CREATE LINK`` block:

:eql:synopsis:`SET default := <expression>`
    Specifies the default value for the link as an EdgeQL expression.
    The default value is used in an ``INSERT`` statement if an explicit
    value for this link is not specified.

:eql:synopsis:`SET readonly := {true | false}`
    If ``true``, the link is considered *read-only*.  Modifications
    of this link are prohibited once an object is created.  All of the
    derived links **must** preserve the original *read-only* value.

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>;`
    Add an annotation :eql:synopsis:`<annotation-name>`
    set to :eql:synopsis:`<value>` to the type.

    See :eql:stmt:`CREATE ANNOTATION` for details.

:eql:synopsis:`CREATE PROPERTY <property-name> ...`
    Define a concrete property item for this link.  See
    :eql:stmt:`CREATE PROPERTY` for details.

:eql:synopsis:`CREATE CONSTRAINT <constraint-name> ...`
    Define a concrete constraint for this link.  See
    :eql:stmt:`CREATE CONSTRAINT` for details.

:eql:synopsis:`ON TARGET DELETE <action>`
    Valid values for *action* are: ``RESTRICT``, ``DELETE
    SOURCE``, ``ALLOW``, and ``DEFERRED RESTRICT``. The details of
    what ``ON TARGET DELETE`` options mean are described in
    :ref:`this section <ref_datamodel_links>`.

:eql:synopsis:`CREATE INDEX ON <index-expr>`
    Define a new :ref:`index <ref_datamodel_indexes>`
    using *index-expr* for this link.  See
    :eql:stmt:`CREATE INDEX` for details.


Examples
--------

Define a new link ``interests`` on the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE MULTI LINK friends -> User
    };

Define a new link ``special_group`` as a
:ref:`computable <ref_datamodel_computables>` on the ``User``
object type, which contains all the friends from the same town:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE LINK special_group := (
            SELECT __source__.friends
            FILTER .town = __source__.town
        )
    };

Define a new abstract link ``orderable``, and then a concrete link
``interests`` that extends it, inheriting the ``weight`` property:

.. code-block:: edgeql

    CREATE ABSTRACT LINK orderable {
        CREATE PROPERTY weight -> std::int64
    };

    ALTER TYPE User {
        CREATE MULTI LINK interests EXTENDING orderable -> Interest
    };



ALTER LINK
==========

:eql-statement:
:eql-haswith:


Change the definition of a :ref:`link <ref_datamodel_links>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} TYPE <TypeName> "{"
      [ ... ]
      ALTER LINK <name>
      [ "{" ] <subcommand>; [...] [ "}" ];
      [ ... ]
    "}"


    [ WITH <with-item> [, ...] ]
    ALTER ABSTRACT LINK [<module>::]<name>
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
      CREATE PROPERTY <property-name> ...
      ALTER PROPERTY <property-name> ...
      DROP PROPERTY <property-name> ...
      CREATE CONSTRAINT <constraint-name> ...
      ALTER CONSTRAINT <constraint-name> ...
      DROP CONSTRAINT <constraint-name> ...
      ON TARGET DELETE <action>
      CREATE INDEX ON <index-expr>
      DROP INDEX ON <index-expr>

Description
-----------

``CREATE TYPE ... ALTER LINK`` and ``ALTER TYPE ... ALTER LINK`` change
the definition of a concrete link for a given object type.

``ALTER ABSTRACT LINK`` changes the definition of an abstract link item.
*name* must be a name of an existing abstract link, optionally qualified
with a module name.

Parameters
----------

The following subcommands are allowed in the ``ALTER LINK`` block:

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the link item to *newname*.  All concrete links
    inheriting from this links are also renamed.

:eql:synopsis:`EXTENDING ...`
    Alter the link parent list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         EXTENDING <name> [, ...]
            [ FIRST | LAST | BEFORE <parent> | AFTER <parent> ]

    This subcommand makes the link a child of the specified list
    of parent links.  The requirements for the parent-child
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

:eql:synopsis:`SET REQUIRED`
    Make the link *required*.

:eql:synopsis:`SET OPTIONAL`
    Make the link no longer *required* (i.e. make it *optional*).

:eql:synopsis:`RESET OPTIONALITY`
    Reset the optionality of the link to the default value (``OPTIONAL``),
    or, if the link is inherited, to the value inherited from links in
    supertypes.

:eql:synopsis:`SET SINGLE`
    Change the maximum cardinality of the link set to *one*.  Only
    valid for concrete links.

:eql:synopsis:`SET MULTI`
    Change the maximum cardinality of the link set to *greater than one*.
    Only valid for concrete links;

:eql:synopsis:`RESET CARDINALITY`
    Reset the maximum cardinality of the link to the default value
    (``SINGLE``), or, if the link is inherited, to the value inherited
    from links in supertypes.

:eql:synopsis:`SET TYPE <typename> [USING (<conversion-expr)]`
    Change the type of the link to the specified
    :eql:synopsis:`<typename>`.  The optional ``USING`` clause specifies
    a conversion expression that computes the new link value from the old.
    The conversion expression must return a singleton set and is evaluated
    on each element of ``MULTI`` links.  A ``USING`` clause must be provided
    if there is no implicit or assignment cast from old to new type.

:eql:synopsis:`RESET TYPE`
    Reset the type of the link to the type inherited from links of the same
    name in supertypes.  It is an error to ``RESET TYPE`` on a link that is
    not inherited.

:eql:synopsis:`USING (<computable-expr>)`
    Change the expression of a :ref:`computable <ref_datamodel_computables>`
    link.  Only valid for concrete links.

:eql:synopsis:`ALTER ANNOTATION <annotation-name>;`
    Alter link annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`ALTER ANNOTATION <ALTER ANNOTATION>` for details.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove link item's annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.

:eql:synopsis:`ALTER PROPERTY <property-name> ...`
    Alter the definition of a property item for this link.  See
    :eql:stmt:`ALTER PROPERTY` for details.

:eql:synopsis:`DROP PROPERTY <property-name>;`
    Remove a property item from this link.  See
    :eql:stmt:`DROP PROPERTY` for details.

:eql:synopsis:`ALTER CONSTRAINT <constraint-name> ...`
    Alter the definition of a constraint for this link.  See
    :eql:stmt:`ALTER CONSTRAINT` for details.

:eql:synopsis:`DROP CONSTRAINT <constraint-name>;`
    Remove a constraint from this link.  See
    :eql:stmt:`DROP CONSTRAINT` for details.

:eql:synopsis:`DROP INDEX ON <index-expr>`
    Remove an :ref:`index <ref_datamodel_indexes>` defined on *index-expr*
    from this link.  See :eql:stmt:`DROP INDEX` for details.

:eql:synopsis:`RESET default`
    Remove the default value from this link, or reset it to the value
    inherited from a supertype, if the link is inherited.

:eql:synopsis:`RESET readonly`
    Set link writability to the default value (writable), or, if the link is
    inherited, to the value inherited from links in supertypes.

All the subcommands allowed in the ``CREATE LINK`` block are also
valid subcommands for ``ALTER LINK`` block.


Examples
--------

Set the ``title`` annotation of link ``friends`` of object type ``User`` to
``"Friends"``:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER LINK interests CREATE ANNOTATION title := "Interests";
    };

Rename the abstract link ``orderable`` to ``sorted``:

.. code-block:: edgeql

    ALTER ABSTRACT LINK orderable RENAME TO sorted;

Redefine the :ref:`computable <ref_datamodel_computables>` link
``special_group`` to be those who have some shared interests:

.. code-block:: edgeql

    ALTER TYPE User {
        CREATE LINK special_group := (
            SELECT __source__.friends
            # at least one of the friend's interests
            # must match the user's
            FILTER .interests IN __source__.interests
        )
    };


DROP LINK
=========

:eql-statement:
:eql-haswith:


Remove the specified link from the schema.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    {CREATE|ALTER} TYPE <TypeName> "{"
      [ ... ]
      DROP LINK <name>
      [ ... ]
    "}"


    [ WITH <with-item> [, ...] ]
    DROP ABSTRACT LINK [<module>]::<name>


Description
-----------

``DROP ABSTRACT LINK`` removes an existing link item from the database
schema.  All subordinate schema items defined on this link, such
as link properties and constraints, are removed as well.

``DROP LINK`` removes the specified link from its
containing object type.  All links that inherit from this link
are also removed.


Examples
--------

Remove link ``friends`` from object type ``User``:

.. code-block:: edgeql

    ALTER TYPE User DROP LINK friends;


Drop abstract link ``orderable``:

.. code-block:: edgeql

    DROP ABSTRACT LINK orderable;
