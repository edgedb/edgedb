.. _ref_eql_ddl_links:

=====
Links
=====

This section describes the DDL commands pertaining to
:ref:`links <ref_datamodel_links>`.


Create link
===========

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_links>` a new link.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    {create|alter} type <TypeName> "{"
      [ ... ]
      create [{required | optional}] [{single | multi}]
        link <name>
        [ extending <base> [, ...] ] -> <type>
        [ "{" <subcommand>; [...] "}" ] ;
      [ ... ]
    "}"

    # Computed link form:

    [ with <with-item> [, ...] ]
    {create|alter} type <TypeName> "{"
      [ ... ]
      create [{required | optional}] [{single | multi}]
        link <name> := <expression>;
      [ ... ]
    "}"

    # Abstract link form:

    [ with <with-item> [, ...] ]
    create abstract link [<module>::]<name> [extending <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]

    # where <subcommand> is one of

      set default := <expression>
      set readonly := {true | false}
      create annotation <annotation-name> := <value>
      create property <property-name> ...
      create constraint <constraint-name> ...
      on target delete <action>
      on source delete <action>
      reset on target delete
      create index on <index-expr>


Description
-----------

The combinations of ``create type ... create link`` and ``alter type
... create link`` define a new concrete link for a given object type.

There are three forms of ``create link``, as shown in the syntax synopsis
above.  The first form is the canonical definition form, the second
form is a syntax shorthand for defining a
:ref:`computed link <ref_datamodel_computed>`, and the third is a
form to define an abstract link item.  The abstract form allows creating
the link in the specified :eql:synopsis:`<module>`.  Concrete link forms
are always created in the same module as the containing object type.


.. _ref_eql_ddl_links_syntax:

Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL link declaration <ref_eql_sdl_links_syntax>`. The following
subcommands are allowed in the ``create link`` block:

:eql:synopsis:`set default := <expression>`
    Specifies the default value for the link as an EdgeQL expression.
    Other than a slight syntactical difference this is the same as the
    corresponding SDL declaration.

:eql:synopsis:`set readonly := {true | false}`
    Specifies whether the link is considered *read-only*. Other than a
    slight syntactical difference this is the same as the
    corresponding SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>;`
    Add an annotation :eql:synopsis:`<annotation-name>`
    set to :eql:synopsis:`<value>` to the type.

    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`create property <property-name> ...`
    Define a concrete property item for this link.  See
    :eql:stmt:`create property` for details.

:eql:synopsis:`create constraint <constraint-name> ...`
    Define a concrete constraint for this link.  See
    :eql:stmt:`create constraint` for details.

:eql:synopsis:`on target delete <action>`
    Valid values for *action* are: ``restrict``, ``DELETE
    SOURCE``, ``allow``, and ``deferred restrict``. The details of
    what ``on target delete`` options mean are described in
    :ref:`this section <ref_datamodel_links>`.

:eql:synopsis:`reset on target delete`
    Reset the delete policy to either the inherited value or to the
    default ``restrict``. The details of what ``on target delete``
    options mean are described in :ref:`this section <ref_datamodel_links>`.

:eql:synopsis:`create index on <index-expr>`
    Define a new :ref:`index <ref_datamodel_indexes>`
    using *index-expr* for this link.  See
    :eql:stmt:`create index` for details.


Examples
--------

Define a new link ``friends`` on the ``User`` object type:

.. code-block:: edgeql

    alter type User {
        create multi link friends -> User
    };

Define a new :ref:`computed link <ref_datamodel_computed>`
``special_group`` on the ``User`` object type, which contains all the
friends from the same town:

.. code-block:: edgeql

    alter type User {
        create link special_group := (
            select __source__.friends
            filter .town = __source__.town
        )
    };

Define a new abstract link ``orderable`` and a concrete link
``interests`` that extends it, inheriting its ``weight`` property:

.. code-block:: edgeql

    create abstract link orderable {
        create property weight -> std::int64
    };

    alter type User {
        create multi link interests extending orderable -> Interest
    };



Alter link
==========

:eql-statement:
:eql-haswith:


Change the definition of a :ref:`link <ref_datamodel_links>`.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    {create|alter} type <TypeName> "{"
      [ ... ]
      alter link <name>
      [ "{" ] <subcommand>; [...] [ "}" ];
      [ ... ]
    "}"


    [ with <with-item> [, ...] ]
    alter abstract link [<module>::]<name>
    [ "{" ] <subcommand>; [...] [ "}" ];

    # where <subcommand> is one of

      set default := <expression>
      reset default
      set readonly := {true | false}
      reset readonly
      rename to <newname>
      extending ...
      set required
      set optional
      reset optionality
      set single
      set multi
      reset cardinality
      set type <typename> [using (<conversion-expr)]
      reset type
      using (<computed-expr>)
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>
      create property <property-name> ...
      alter property <property-name> ...
      drop property <property-name> ...
      create constraint <constraint-name> ...
      alter constraint <constraint-name> ...
      drop constraint <constraint-name> ...
      on target delete <action>
      on source delete <action>
      create index on <index-expr>
      drop index on <index-expr>

Description
-----------

The combinations of``create type ... alter link`` and ``alter type ...
alter link`` change the definition of a concrete link for a given
object type.

The command ``alter abstract link`` changes the definition of an
abstract link item. *name* must be the identity of an existing
abstract link, optionally qualified with a module name.

Parameters
----------

The following subcommands are allowed in the ``alter link`` block:

:eql:synopsis:`rename to <newname>`
    Change the name of the link item to *newname*.  All concrete links
    inheriting from this links are also renamed.

:eql:synopsis:`extending ...`
    Alter the link parent list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         extending <name> [, ...]
            [ first | last | before <parent> | after <parent> ]

    This subcommand makes the link a child of the specified list
    of parent links.  The requirements for the parent-child
    relationship are the same as when creating a link.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``first`` -- insert parent(s) at the beginning of the
      parent list,
    * ``last`` -- insert parent(s) at the end of the parent list,
    * ``before <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``after <parent>`` -- insert parent(s) after an existing
      *parent*.

:eql:synopsis:`set required`
    Make the link *required*.

:eql:synopsis:`set optional`
    Make the link no longer *required* (i.e. make it *optional*).

:eql:synopsis:`reset optionality`
    Reset the optionality of the link to the default value (``optional``),
    or, if the link is inherited, to the value inherited from links in
    supertypes.

:eql:synopsis:`set single`
    Change the link set's maximum cardinality to *one*.  Only
    valid for concrete links.

:eql:synopsis:`set multi`
    Remove the upper limit on the link set's cardinality. Only valid for
    concrete links.

:eql:synopsis:`reset cardinality`
    Reset the link set's maximum cardinality to the default value
    (``single``), or to the value inherited from the link's supertypes.

:eql:synopsis:`set type <typename> [using (<conversion-expr)]`
    Change the type of the link to the specified
    :eql:synopsis:`<typename>`.  The optional ``using`` clause specifies
    a conversion expression that computes the new link value from the old.
    The conversion expression must return a singleton set and is evaluated
    on each element of ``multi`` links.  A ``using`` clause must be provided
    if there is no implicit or assignment cast from old to new type.

:eql:synopsis:`reset type`
    Reset the type of the link to be strictly the inherited type. This only
    has an effect on links that have been :ref:`overloaded
    <ref_eql_sdl_links_overloading>` in order to change their inherited
    type. It is an error to ``reset type`` on a link that is not inherited.

:eql:synopsis:`using (<computed-expr>)`
    Change the expression of a :ref:`computed link
    <ref_datamodel_computed>`.  Only valid for concrete links.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter link annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove link item's annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`alter property <property-name> ...`
    Alter the definition of a property item for this link.  See
    :eql:stmt:`alter property` for details.

:eql:synopsis:`drop property <property-name>;`
    Remove a property item from this link.  See
    :eql:stmt:`drop property` for details.

:eql:synopsis:`alter constraint <constraint-name> ...`
    Alter the definition of a constraint for this link.  See
    :eql:stmt:`alter constraint` for details.

:eql:synopsis:`drop constraint <constraint-name>;`
    Remove a constraint from this link.  See
    :eql:stmt:`drop constraint` for details.

:eql:synopsis:`drop index on <index-expr>`
    Remove an :ref:`index <ref_datamodel_indexes>` defined on *index-expr*
    from this link.  See :eql:stmt:`drop index` for details.

:eql:synopsis:`reset default`
    Remove the default value from this link, or reset it to the value
    inherited from a supertype, if the link is inherited.

:eql:synopsis:`reset readonly`
    Set link writability to the default value (writable), or, if the link is
    inherited, to the value inherited from links in supertypes.

All the subcommands allowed in the ``create link`` block are also
valid subcommands for ``alter link`` block.


Examples
--------

On the object type ``User``, set the ``title`` annotation of its
``friends`` link to ``"Friends"``:

.. code-block:: edgeql

    alter type User {
        alter link friends create annotation title := "Friends";
    };

Rename the abstract link ``orderable`` to ``sorted``:

.. code-block:: edgeql

    alter abstract link orderable rename to sorted;

Redefine the :ref:`computed link <ref_datamodel_computed>`
``special_group`` to be those who have some shared interests:

.. code-block:: edgeql

    alter type User {
        create link special_group := (
            select __source__.friends
            # at least one of the friend's interests
            # must match the user's
            filter .interests IN __source__.interests
        )
    };


Drop link
=========

:eql-statement:
:eql-haswith:


Remove the specified link from the schema.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter type <TypeName> "{"
      [ ... ]
      drop link <name>
      [ ... ]
    "}"


    [ with <with-item> [, ...] ]
    drop abstract link [<module>]::<name>


Description
-----------

The combination of ``alter type`` and ``drop link`` removes the
specified link from its containing object type.  All links that
inherit from this link are also removed.

The command ``drop abstract link`` removes an existing link item from
the database schema.  All subordinate schema items defined on this
link, such as link properties and constraints, are removed as well.


Examples
--------

Remove link ``friends`` from object type ``User``:

.. code-block:: edgeql

    alter type User drop link friends;


Drop abstract link ``orderable``:

.. code-block:: edgeql

    drop abstract link orderable;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Links <ref_datamodel_links>`
  * - :ref:`SDL > Links <ref_eql_sdl_links>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
