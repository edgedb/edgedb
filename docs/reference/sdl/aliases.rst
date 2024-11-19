.. _ref_eql_sdl_aliases:

==================
Expression Aliases
==================

This section describes the SDL declarations pertaining to
:ref:`expression aliases <ref_datamodel_aliases>`.

Example
-------

Declare a "UserAlias" that provides additional information for a "User"
via a :ref:`computed link <ref_datamodel_computed>` "friend_of":

.. code-block:: sdl

    alias UserAlias := User {
        # declare a computed link
        friend_of := User.<friends[is User]
    };

.. _ref_eql_sdl_aliases_syntax:

Syntax
------

Define a new alias corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_aliases>`.

.. sdl:synopsis::

    alias <alias-name> := <alias-expr> ;

    alias <alias-name> "{"
        using <alias-expr>;
        [ <annotation-declarations> ]
    "}" ;


Description
-----------

This declaration defines a new alias with the following options:

:eql:synopsis:`<alias-name>`
    The name (optionally module-qualified) of an alias to be created.

:eql:synopsis:`<alias-expr>`
    The aliased expression.  Must be a :ref:`Stable <ref_reference_volatility>`
    EdgeQL expression.

The valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set alias :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Aliases <ref_datamodel_aliases>`
  * - :ref:`DDL > Aliases <ref_eql_ddl_aliases>`
  * - :ref:`Cheatsheets > Aliases <ref_cheatsheet_aliases>`
