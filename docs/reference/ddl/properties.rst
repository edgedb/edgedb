.. _ref_eql_ddl_props:

==========
Properties
==========

This section describes the DDL commands pertaining to
:ref:`properties <ref_datamodel_props>`.


Create property
===============

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_props>` a new property.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    {create|alter} {type|link} <SourceName> "{"
      [ ... ]
      create [{required | optional}] [{single | multi}]
        property <name>
        [ extending <base> [, ...] ] -> <type>
        [ "{" <subcommand>; [...] "}" ] ;
      [ ... ]
    "}"

    # Computed property form:

    [ with <with-item> [, ...] ]
    {create|alter} {type|link} <SourceName> "{"
      [ ... ]
      create [{required | optional}] [{single | multi}]
        property <name> := <expression>;
      [ ... ]
    "}"

    # Abstract property form:

    [ with <with-item> [, ...] ]
    create abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]

    # where <subcommand> is one of

      set default := <expression>
      set readonly := {true | false}
      create annotation <annotation-name> := <value>
      create constraint <constraint-name> ...


Description
-----------

The combination :eql:synopsis:`{create|alter} {type|link} ... create property`
defines a new concrete property for a given object type or link.

There are three forms of ``create property``, as shown in the syntax synopsis
above.  The first form is the canonical definition form, the second
form is a syntax shorthand for defining a
:ref:`computed property <ref_datamodel_computed>`, and the third
is a form to define an abstract property item.  The abstract form
allows creating the property in the specified
:eql:synopsis:`<module>`.  Concrete property forms are always
created in the same module as the containing object or property.

.. _ref_eql_ddl_props_syntax:

Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL property declaration <ref_eql_sdl_props_syntax>`. The
following subcommands are allowed in the ``create property`` block:

:eql:synopsis:`set default := <expression>`
    Specifies the default value for the property as an EdgeQL expression.
    Other than a slight syntactical difference this is the same as the
    corresponding SDL declaration.

:eql:synopsis:`set readonly := {true | false}`
    Specifies whether the property is considered *read-only*. Other
    than a slight syntactical difference this is the same as the
    corresponding SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set property :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`create constraint`
    Define a concrete constraint on the property.
    See :eql:stmt:`create constraint` for details.


Examples
--------

Define a new link ``address`` on the ``User`` object type:

.. code-block:: edgeql

    alter type User {
        create property address -> str
    };

Define a new :ref:`computed property <ref_datamodel_computed>`
``number_of_connections`` on the ``User`` object type counting the
number of interests:

.. code-block:: edgeql

    alter type User {
        create property number_of_connections :=
            count(.interests)
    };

Define a new abstract link ``orderable`` with ``weight`` property:

.. code-block:: edgeql

    create abstract link orderable {
        create property weight -> std::int64
    };


Alter property
==============

:eql-statement:
:eql-haswith:


Change the definition of a :ref:`property <ref_datamodel_props>`.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    {create | alter} {type | link} <source> "{"
      [ ... ]
      alter property <name>
      [ "{" ] <subcommand>; [...] [ "}" ];
      [ ... ]
    "}"


    [ with <with-item> [, ...] ]
    alter abstract property [<module>::]<name>
    [ "{" ] <subcommand>; [...] [ "}" ];

    # where <subcommand> is one of

      set default := <expression>
      reset default
      set readonly := {true | false}
      reset readonly
      rename to <newname>
      extending ...
      set required [using (<conversion-expr)]
      set optional
      reset optionality
      set single [using (<conversion-expr)]
      set multi
      reset cardinality [using (<conversion-expr)]
      set type <typename> [using (<conversion-expr)]
      reset type
      using (<computed-expr>)
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>
      create constraint <constraint-name> ...
      alter constraint <constraint-name> ...
      drop constraint <constraint-name> ...


Description
-----------

The combination :eql:synopsis:`{create|alter} {type|link} ... create property`
defines a new concrete property for a given object type or link.

The command :eql:synopsis:`alter abstract property` changes the
definition of an abstract property item.


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

The following subcommands are allowed in the ``alter link`` block:

:eql:synopsis:`rename to <newname>`
    Change the name of the property to :eql:synopsis:`<newname>`.
    All concrete properties inheriting from this property are
    also renamed.

:eql:synopsis:`extending ...`
    Alter the property parent list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         extending <name> [, ...]
            [ first | last | before <parent> | after <parent> ]

    This subcommand makes the property a child of the specified list
    of parent property items.  The requirements for the parent-child
    relationship are the same as when creating a property.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``first`` -- insert parent(s) at the beginning of the
      parent list,
    * ``last`` -- insert parent(s) at the end of the parent list,
    * ``before <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``after <parent>`` -- insert parent(s) after an existing
      *parent*.

:eql:synopsis:`set required [using (<conversion-expr)]`
    Make the property *required*.

:eql:synopsis:`set optional`
    Make the property no longer *required* (i.e. make it *optional*).

:eql:synopsis:`reset optionality`
    Reset the optionality of the property to the default value (``optional``),
    or, if the property is inherited, to the value inherited from properties in
    supertypes.

:eql:synopsis:`set single [using (<conversion-expr)]`
    Change the maximum cardinality of the property set to *one*.  Only
    valid for concrete properties.

:eql:synopsis:`set multi`
    Change the maximum cardinality of the property set to
    *greater than one*.  Only valid for concrete properties.

:eql:synopsis:`reset cardinality [using (<conversion-expr)]`
    Reset the maximum cardinality of the property to the default value
    (``single``), or, if the property is inherited, to the value inherited
    from properties in supertypes.

:eql:synopsis:`set type <typename> [using (<conversion-expr)]`
    Change the type of the property to the specified
    :eql:synopsis:`<typename>`.  The optional ``using`` clause specifies
    a conversion expression that computes the new property value from the old.
    The conversion expression must return a singleton set and is evaluated
    on each element of ``multi`` properties.  A ``using`` clause must be
    provided if there is no implicit or assignment cast from old to new type.

:eql:synopsis:`reset type`
    Reset the type of the property to the type inherited from properties
    of the same name in supertypes.  It is an error to ``reset type`` on
    a property that is not inherited.

:eql:synopsis:`using (<computed-expr>)`
    Change the expression of a :ref:`computed property
    <ref_datamodel_computed>`.  Only valid for concrete properties.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter property annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove property annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`alter constraint <constraint-name> ...`
    Alter the definition of a constraint for this property.  See
    :eql:stmt:`alter constraint` for details.

:eql:synopsis:`drop constraint <constraint-name>;`
    Remove a constraint from this property.  See
    :eql:stmt:`drop constraint` for details.

:eql:synopsis:`reset default`
    Remove the default value from this property, or reset it to the value
    inherited from a supertype, if the property is inherited.

:eql:synopsis:`reset readonly`
    Set property writability to the default value (writable), or, if the
    property is inherited, to the value inherited from properties in
    supertypes.

All the subcommands allowed in the ``create property`` block are also
valid subcommands for ``alter property`` block.


Examples
--------

Set the ``title`` annotation of property ``address`` of object type
``User`` to ``"Home address"``:

.. code-block:: edgeql

    alter type User {
        alter property address
            create annotation title := "Home address";
    };

Add a maximum-length constraint to property ``address`` of object type
``User``:

.. code-block:: edgeql

    alter type User {
        alter property address {
            create constraint max_len_value(500);
        };
    };

Rename the property ``weight`` of link ``orderable`` to ``sort_by``:

.. code-block:: edgeql

    alter abstract link orderable {
        alter property weight rename to sort_by;
    };

Redefine the :ref:`computed property <ref_datamodel_computed>`
``number_of_connections`` to be the number of friends:

.. code-block:: edgeql

    alter type User {
        alter property number_of_connections using (
            count(.friends)
        )
    };


Drop property
=============

:eql-statement:
:eql-haswith:

Remove a :ref:`property <ref_datamodel_props>` from the schema.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    {create|alter} type <TypeName> "{"
      [ ... ]
      drop link <name>
      [ ... ]
    "}"


    [ with <with-item> [, ...] ]
    drop abstract property <name> ;

Description
-----------

The combination :eql:synopsis:`alter {type|link} drop property`
removes the specified property from its containing object type or
link.  All properties that inherit from this property are also
removed.

The command :eql:synopsis:`drop abstract property` removes the
specified abstract property item from the schema.

Example
-------

Remove property ``address`` from type ``User``:

.. code-block:: edgeql

    alter type User {
        drop property address;
    };


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Properties <ref_datamodel_props>`
  * - :ref:`SDL > Properties <ref_eql_sdl_props>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
