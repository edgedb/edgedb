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
        expr := <view-expr>;
        [ <attribute-declarations> ]
    "}" ;


Description
-----------

:sdl:synopsis:`<view-name>`
    The name (optionally module-qualified) of a view to be created.

:sdl:synopsis:`<view-expr>`
    An expression that defines the *shape* and the contents of the view.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_eql_sdl_schema_attributes>` declarations.
