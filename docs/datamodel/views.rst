.. _ref_datamodel_views:

=====
Views
=====

A *view* is a named query.  Once a view is defined, it can be referred to
like an regular schema type.  Views over queries that return objects
essentially define a *view subtype* of the original object type, which may
have different properties and links as may be specified by a *shape* in
the view expression.


Definition
==========

A view may be defined in EdgeDB Schema using the ``view`` declaration:

.. eschema:synopsis::

    view <view-name>:
        expr := <view-expr>
        [ <attribute-declarations> ]


Parameters
----------

:eschema:synopsis:`<view-name>`
    Specifies the name of the view.

:eschema:synopsis:`<view-expr>`
    An expression defining the *shape* and the contents of the view.

:eschema:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


DDL
===

Views can also be defined using the :eql:stmt:`CREATE VIEW` EdgeQL command.
