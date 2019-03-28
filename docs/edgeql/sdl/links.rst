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
        attribute title := 'Close contacts';
    }

Declare a *concrete* link "friends" within a "User" type:

.. code-block:: sdl

    type User {
        property name -> str;
        property address -> str;
        # define a concrete link "friends"
        multi link friends extending friends_base-> User;

        index user_name_idx on (__subject__.name);
    }


Syntax
------

Define a new link corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_links>`.

.. sdl:synopsis::

    # Concrete link form used inside type declaration:
    [ required ] [{single | multi}] link <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{" <subcommand>; [...] "}" ] ;

    # Computable link form used inside type declaration:
    [ required ] [{single | multi}] link <name> := <expression>;

    # Abstract link form:
    abstract link [<module>::]<name> [extending <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]

