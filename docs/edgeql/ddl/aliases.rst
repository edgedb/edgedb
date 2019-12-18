.. _ref_eql_ddl_aliases:

=======
Aliases
=======

This section describes the DDL commands pertaining to
:ref:`expression aliases <ref_datamodel_aliases>`.


CREATE ALIAS
============

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_aliases>` a new expression alias in the schema.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE ALIAS <alias-name> := <alias-expr> ;

    [ WITH <with-item> [, ...] ]
    CREATE ALIAS <alias-name> "{"
        USING <alias-expr>;
        [ SET ANNOTATION <attr-name> := <attr-value>; ... ]
    "}" ;

    # where <with-item> is:

    [ <module-alias> := ] MODULE <module-name>


Description
-----------

``CREATE ALIAS`` defines a new expression alias in the schema.
The schema-level expression aliases are functionally equivalent
to expression aliases defined in a statement :ref:`WITH block <ref_eql_with>`,
but are available to all queries using the schema and can be
introspected.

If *name* is qualified with a module name, then the alias is created
in that module, otherwise it is created in the current module.
The alias name must be distinct from that of any existing schema item
in the module.


Parameters
----------

:eql:synopsis:`<alias-name>`
    The name (optionally module-qualified) of an alias to be created.

:eql:synopsis:`<alias-expr>`
    The aliased expression.  Can be any valid EdgeQL expression.

:eql:synopsis:`SET ANNOTATION <annotation-name> := <value>;`
    An optional list of annotation values for the alias.
    See :eql:stmt:`SET ANNOTATION` for details.

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    alias definition.


Example
-------

Create a new alias:

.. code-block:: edgeql

    CREATE ALIAS Superusers := (
        SELECT User FILTER User.groups.name = 'Superusers'
    );


DROP ALIAS
==========

:eql-statement:
:eql-haswith:


Remove an expression alias from the schema.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP ALIAS <alias-name> ;


Description
-----------

``DROP ALIAS`` removes an expression alias from the schema.


Parameters
----------

*alias-name*
    The name (optionally qualified with a module name) of an existing
    expression alias.


Example
-------

Remove an alias:

.. code-block:: edgeql

    DROP ALIAS SuperUsers;
