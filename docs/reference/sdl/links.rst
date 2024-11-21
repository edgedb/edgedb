.. _ref_eql_sdl_links:

=====
Links
=====

This section describes the SDL declarations pertaining to
:ref:`links <ref_datamodel_links>`.


Examples
--------

Declare an *abstract* link "friends_base" with a helpful title:

.. code-block:: sdl

    abstract link friends_base {
        # declare a specific title for the link
        annotation title := 'Close contacts';
    }

Declare a *concrete* link "friends" within a "User" type:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
        required property name -> str;
        property address -> str;
        # define a concrete link "friends"
        multi link friends extending friends_base -> User;

        index on (__subject__.name);
    }

.. code-block:: sdl

    type User {
        required name: str;
        address: str;
        # define a concrete link "friends"
        multi friends: User {
            extending friends_base;
        };

        index on (__subject__.name);
    }

.. _ref_eql_sdl_links_overloading:

Overloading
~~~~~~~~~~~

Any time that the SDL declaration refers to an inherited link that is
being overloaded (by adding more constraints or changing the target
type, for example), the ``overloaded`` keyword must be used. This is
to prevent unintentional overloading due to name clashes:

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Friendly {
        # this type can have "friends"
        multi link friends -> Friendly;
    }

    type User extending Friendly {
        # overload the link target to be User, specifically
        overloaded multi link friends -> User;
        # ... other links and properties
    }

.. code-block:: sdl

    abstract type Friendly {
        # this type can have "friends"
        multi friends: Friendly;
    }

    type User extending Friendly {
        # overload the link target to be User, specifically
        overloaded multi friends: User;
        # ... other links and properties
    }

.. _ref_eql_sdl_links_syntax:

Syntax
------

Define a new link corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_links>`.

.. sdl:synopsis::
    :version-lt: 3.0

    # Concrete link form used inside type declaration:
    [ overloaded ] [{required | optional}] [{single | multi}]
      link <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ on target delete <action> ; ]
          [ on source delete <action> ; ]
          [ <annotation-declarations> ]
          [ <property-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]


    # Computed link form used inside type declaration:
    [{required | optional}] [{single | multi}]
      link <name> := <expression>;

    # Computed link form used inside type declaration (extended):
    [ overloaded ] [{required | optional}] [{single | multi}]
      link <name>
      [ extending <base> [, ...] ] [-> <type>]
      [ "{"
          using (<expression>) ;
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Abstract link form:
    abstract link <name> [extending <base> [, ...]]
    [ "{"
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        [ <property-declarations> ]
        [ <constraint-declarations> ]
        [ <index-declarations> ]
        ...
      "}" ]

.. sdl:synopsis::
    :version-lt: 4.0

    # Concrete link form used inside type declaration:
    [ overloaded ] [{required | optional}] [{single | multi}]
      link <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ on target delete <action> ; ]
          [ on source delete <action> ; ]
          [ <annotation-declarations> ]
          [ <property-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]


    # Computed link form used inside type declaration:
    [{required | optional}] [{single | multi}]
      link <name> := <expression>;

    # Computed link form used inside type declaration (extended):
    [ overloaded ] [{required | optional}] [{single | multi}]
      link <name>
      [ extending <base> [, ...] ] [-> <type>]
      [ "{"
          using (<expression>) ;
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Abstract link form:
    abstract link <name> [extending <base> [, ...]]
    [ "{"
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        [ <property-declarations> ]
        [ <constraint-declarations> ]
        [ <index-declarations> ]
        ...
      "}" ]

.. sdl:synopsis::

    # Concrete link form used inside type declaration:
    [ overloaded ] [{required | optional}] [{single | multi}]
      [ link ] <name> : <type>
      [ "{"
          [ extending <base> [, ...] ; ]
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ on target delete <action> ; ]
          [ on source delete <action> ; ]
          [ <annotation-declarations> ]
          [ <property-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]


    # Computed link form used inside type declaration:
    [{required | optional}] [{single | multi}]
      [ link ] <name> := <expression>;

    # Computed link form used inside type declaration (extended):
    [ overloaded ] [{required | optional}] [{single | multi}]
      link <name> [: <type>]
      [ "{"
          using (<expression>) ;
          [ extending <base> [, ...] ; ]
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Abstract link form:
    abstract link <name>
    [ "{"
        [extending <base> [, ...] ; ]
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        [ <property-declarations> ]
        [ <constraint-declarations> ]
        [ <index-declarations> ]
        ...
      "}" ]


Description
-----------

There are several forms of link declaration, as shown in the syntax synopsis
above. The first form is the canonical definition form, the second form is used
for defining a :ref:`computed link <ref_datamodel_computed>`, and the last form
is used to define an abstract link. The abstract form allows declaring the link
directly inside a :ref:`module <ref_eql_sdl_modules>`. Concrete link forms are
always used as sub-declarations of an :ref:`object type
<ref_eql_sdl_object_types>`.

The following options are available:

:eql:synopsis:`overloaded`
    If specified, indicates that the link is inherited and that some
    feature of it may be altered in the current object type.  It is an
    error to declare a link as *overloaded* if it is not inherited.

:eql:synopsis:`required`
    If specified, the link is considered *required* for the parent
    object type.  It is an error for an object to have a required
    link resolve to an empty value.  Child links **always** inherit
    the *required* attribute, i.e it is not possible to make a
    required link non-required by extending it.

:eql:synopsis:`optional`
    This is the default qualifier assumed when no qualifier is
    specified, but it can also be specified explicitly. The link is
    considered *optional* for the parent object type, i.e. it is
    possible for the link to resolve to an empty value.

:eql:synopsis:`multi`
    Specifies that there may be more than one instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size greater than one.

:eql:synopsis:`single`
    Specifies that there may be at most *one* instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size not greater than one.  ``single`` is assumed if nether
    ``multi`` nor ``single`` qualifier is specified.

:eql:synopsis:`extending <base> [, ...]`
    Optional clause specifying the *parents* of the new link item.

    Use of ``extending`` creates a persistent schema relationship
    between the new link and its parents.  Schema modifications
    to the parent(s) propagate to the child.

    If the same *property* name exists in more than one parent, or
    is explicitly defined in the new link and at least one parent,
    then the data types of the property targets must be *compatible*.
    If there is no conflict, the link properties are merged to form a
    single property in the new link item.

    .. versionadded:: 3.0

        As of EdgeDB 3.0, the ``extending`` clause is now a sub-declaration of
        the link and included inside the curly braces rather than an option as
        in earlier versions.

:eql:synopsis:`<type>`
    The type must be a valid :ref:`type expression <ref_eql_types>`
    denoting an object type.

The valid SDL sub-declarations are listed below:

:eql:synopsis:`default := <expression>`
    Specifies the default value for the link as an EdgeQL expression.
    The default value is used in an ``insert`` statement if an explicit
    value for this link is not specified.

    The expression must be :ref:`Stable <ref_reference_volatility>`.

:eql:synopsis:`readonly := {true | false}`
    If ``true``, the link is considered *read-only*.  Modifications
    of this link are prohibited once an object is created.  All of the
    derived links **must** preserve the original *read-only* value.

:sdl:synopsis:`<annotation-declarations>`
    Set link :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<property-declarations>`
    Define a concrete :ref:`property <ref_eql_sdl_props>` on the link.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` on the link.

:sdl:synopsis:`<index-declarations>`
    Define an :ref:`index <ref_eql_sdl_indexes>` for this abstract
    link. Note that this index can only refer to link properties.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Links <ref_datamodel_links>`
  * - :ref:`DDL > Links <ref_eql_ddl_links>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
