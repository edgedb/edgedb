.. _ref_eql_sdl_views:

=====
Views
=====

This section describes the SDL declarations pertaining to
:ref:`views <ref_datamodel_views>`.


Example
-------

Declare a "UserView" that provides additional information for a "User"
via a :ref:`computable link <ref_datamodel_computables>` "friend_of":

.. code-block:: sdl

    view UserView := User {
        # declare a computable link
        friend_of := User.<friends[IS User]
    };


Syntax
------

Define a new view corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_views>`.

.. sdl:synopsis::

    view <view-name> := <view-expr> ;

    view <view-name> "{"
        using <view-expr>;
        [ <annotation-declarations> ]
    "}" ;


Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE VIEW`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set view :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.
