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

    type User {
        required property name -> str;
        property address -> str;
        # define a concrete link "friends"
        multi link friends extending friends_base-> User;

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

    abstract type Friendly {
        # this type can have "friends"
        link friends -> Friendly;
    }

    type User extending Friendly {
        # overload the link target to be User, specifically
        overloaded multi link friends -> User;
        # ... other links and properties
    }


Syntax
------

Define a new link corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_links>`.

.. sdl:synopsis::

    # Concrete link form used inside type declaration:
    [ overloaded ] [ required ] [{single | multi}] link <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ on target delete <action> ; ]
          [ <annotation-declarations> ]
          [ <property-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]


    # Computable link form used inside type declaration:
    [ required ] [{single | multi}] link <name> := <expression>;

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

Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE LINK`,
while the valid SDL sub-declarations are listed below:

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
