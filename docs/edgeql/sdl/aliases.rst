.. _ref_eql_sdl_aliases:

==================
Expression Aliases
==================

This section describes the SDL declarations pertaining to
:ref:`expression aliases <ref_datamodel_aliases>`.


Example
-------

Declare a "UserAlias" that provides additional information for a "User"
via a :ref:`computable link <ref_datamodel_computables>` "friend_of":

.. code-block:: sdl

    alias UserAlias := User {
        # declare a computable link
        friend_of := User.<friends[IS User]
    };


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

The core of the declaration is identical to :eql:stmt:`CREATE ALIAS`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set alias :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.
