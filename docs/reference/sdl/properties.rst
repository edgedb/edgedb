.. _ref_eql_sdl_props:

==========
Properties
==========

This section describes the SDL declarations pertaining to
:ref:`properties <ref_datamodel_props>`.


Examples
--------

Declare an *abstract* property "address_base" with a helpful title:

.. code-block:: sdl

    abstract property address_base {
        # declare a specific title for the link
        annotation title := 'Mailing address';
    }

Declare *concrete* properties "name" and "address" within a "User" type:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
        # define concrete properties
        required property name -> str;
        property address extending address_base -> str;

        multi link friends -> User;

        index on (__subject__.name);
    }

.. code-block:: sdl

    type User {
        # define concrete properties
        required name: str;
        address: str {
            extending address_base;
        };

        multi friends: User;

        index on (__subject__.name);
    }

Any time that the SDL declaration refers to an inherited property that
is being overloaded (by adding more constraints, for example), the
``overloaded`` keyword must be used. This is to prevent unintentional
overloading due to name clashes:

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Named {
        property name -> str;
    }

    type User extending Named {
        # define concrete properties
        overloaded required property name -> str;
        # ... other links and properties
    }

.. code-block:: sdl

    abstract type Named {
        name: str;
    }

    type User extending Named {
        # define concrete properties
        overloaded required name: str;
        # ... other links and properties
    }

.. _ref_eql_sdl_props_syntax:

Syntax
------

Define a new property corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_props>`.

.. sdl:synopsis::
    :version-lt: 3.0

    # Concrete property form used inside type declaration:
    [ overloaded ] [{required | optional}] [{single | multi}]
      property <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Computed property form used inside type declaration:
    [{required | optional}] [{single | multi}]
      property <name> := <expression>;

    # Computed property form used inside type declaration (extended):
    [ overloaded ] [{required | optional}] [{single | multi}]
      property <name>
      [ extending <base> [, ...] ] [-> <type>]
      [ "{"
          using (<expression>) ;
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Abstract property form:
    abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{"
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        ...
      "}" ]

.. sdl:synopsis::
    :version-lt: 4.0

    # Concrete property form used inside type declaration:
    [ overloaded ] [{required | optional}] [{single | multi}]
      property <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Computed property form used inside type declaration:
    [{required | optional}] [{single | multi}]
      property <name> := <expression>;

    # Computed property form used inside type declaration (extended):
    [ overloaded ] [{required | optional}] [{single | multi}]
      property <name>
      [ extending <base> [, ...] ] [-> <type>]
      [ "{"
          using (<expression>) ;
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Abstract property form:
    abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{"
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        ...
      "}" ]

.. sdl:synopsis::

    # Concrete property form used inside type declaration:
    [ overloaded ] [{required | optional}] [{single | multi}]
      [ property ] <name> : <type>
      [ "{"
          [ extending <base> [, ...] ; ]
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Computed property form used inside type declaration:
    [{required | optional}] [{single | multi}]
      [ property ] <name> := <expression>;

    # Computed property form used inside type declaration (extended):
    [ overloaded ] [{required | optional}] [{single | multi}]
      property <name> [: <type>]
      [ "{"
          using (<expression>) ;
          [ extending <base> [, ...] ; ]
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Abstract property form:
    abstract property [<module>::]<name>
    [ "{"
        [extending <base> [, ...] ; ]
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        ...
      "}" ]


Description
-----------

There are several forms of ``property`` declaration, as shown in the
syntax synopsis above.  The first form is the canonical definition
form, the second and third forms are used for defining a
:ref:`computed property <ref_datamodel_computed>`, and the last
one is a form to define an ``abstract property``.  The abstract
form allows declaring the property directly inside a :ref:`module
<ref_eql_sdl_modules>`.  Concrete property forms are always used
as sub-declarations for an :ref:`object type
<ref_eql_sdl_object_types>` or a :ref:`link <ref_eql_sdl_links>`.

The following options are available:

:eql:synopsis:`overloaded`
    If specified, indicates that the property is inherited and that some
    feature of it may be altered in the current object type.  It is an
    error to declare a property as *overloaded* if it is not inherited.

:eql:synopsis:`required`
    If specified, the property is considered *required* for the parent
    object type.  It is an error for an object to have a required
    property resolve to an empty value.  Child properties **always**
    inherit the *required* attribute, i.e it is not possible to make a
    required property non-required by extending it.

:eql:synopsis:`optional`
    This is the default qualifier assumed when no qualifier is
    specified, but it can also be specified explicitly. The property
    is considered *optional* for the parent object type, i.e. it is
    possible for the property to resolve to an empty value.

:eql:synopsis:`multi`
    Specifies that there may be more than one instance of this
    property in an object, in other words, ``Object.property`` may
    resolve to a set of a size greater than one.

:eql:synopsis:`single`
    Specifies that there may be at most *one* instance of this
    property in an object, in other words, ``Object.property`` may
    resolve to a set of a size not greater than one.  ``single`` is
    assumed if nether ``multi`` nor ``single`` qualifier is specified.

:eql:synopsis:`extending <base> [, ...]`
    Optional clause specifying the *parents* of the new property item.

    Use of ``extending`` creates a persistent schema relationship
    between the new property and its parents.  Schema modifications
    to the parent(s) propagate to the child.

    .. versionadded:: 3.0

        As of EdgeDB 3.0, the ``extended`` clause is now a sub-declaration of
        the property and included inside the curly braces rather than an option
        as in earlier versions.

:eql:synopsis:`<type>`
    The type must be a valid :ref:`type expression <ref_eql_types>`
    denoting a non-abstract scalar or a container type.

The valid SDL sub-declarations are listed below:

:eql:synopsis:`default := <expression>`
    Specifies the default value for the property as an EdgeQL expression.
    The default value is used in an ``insert`` statement if an explicit
    value for this property is not specified.

    The expression must be :ref:`Stable <ref_reference_volatility>`.

:eql:synopsis:`readonly := {true | false}`
    If ``true``, the property is considered *read-only*.
    Modifications of this property are prohibited once an object is
    created.  All of the derived properties **must** preserve the
    original *read-only* value.

:sdl:synopsis:`<annotation-declarations>`
    Set property :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` on
    the property.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Properties <ref_datamodel_props>`
  * - :ref:`DDL > Properties <ref_eql_ddl_props>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
