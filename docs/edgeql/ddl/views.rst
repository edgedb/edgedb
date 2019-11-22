.. _ref_eql_ddl_views:

=====
Views
=====

This section describes the DDL commands pertaining to
:ref:`views <ref_datamodel_views>`.


CREATE VIEW
===========

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_views>` a new view.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE VIEW <view-name> := <view-expr> ;

    [ WITH <with-item> [, ...] ]
    CREATE VIEW <view-name> "{"
        USING <view-expr>;
        [ SET ANNOTATION <attr-name> := <attr-value>; ... ]
    "}" ;

    # where <with-item> is:

    [ <module-alias> := ] MODULE <module-name>


Description
-----------

``CREATE VIEW`` defines a new view.  The view is not materialized and its
expression is run every time the view is referenced from a query.

If *name* is qualified with a module name, then the view is created
in that module, otherwise it is created in the current module.
The view name must be distinct from that of any existing schema item
in the module.


Parameters
----------

:eql:synopsis:`<view-name>`
    The name (optionally module-qualified) of a view to be created.

:eql:synopsis:`<view-expr>`
    An expression that defines the *shape* and the contents of the view.

:eql:synopsis:`SET ANNOTATION <annotation-name> := <value>;`
    An optional list of annotation values for the view.
    See :eql:stmt:`SET ANNOTATION` for details.

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    view definition.


Example
-------

Create a new view:

.. code-block:: edgeql

    CREATE VIEW Superusers := (
        SELECT User FILTER User.groups.name = 'Superusers'
    );


DROP VIEW
=========

:eql-statement:
:eql-haswith:


Remove a view.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP VIEW <view-name> ;


Description
-----------

``DROP VIEW`` removes a view.


Parameters
----------

*view-name*
    The name (optionally qualified with a module name) of an existing
    view.


Example
-------

Remove a view:

.. code-block:: edgeql

    DROP VIEW SuperUsers;
