.. _ref_eql_ddl_indexes:

=======
Indexes
=======

This section describes the DDL commands pertaining to
:ref:`indexes <ref_datamodel_indexes>`.


CREATE INDEX
============

:eql-statement:


:ref:`Define <ref_eql_sdl_indexes>` an new index for a given object
type or link.

.. eql:synopsis::

    CREATE INDEX ON ( <index-expr> )
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      CREATE ANNOTATION <annotation-name> := <value>


Description
-----------

``CREATE INDEX`` constructs a new index for a given object type or
link using *index-expr*.


Parameters
----------

:sdl:synopsis:`ON ( <index-expr> )`
    The specific expression for which the index is made.  Note also
    that ``<index-expr>`` itself has to be parenthesized.

The only subcommand that is allowed in the ``CREATE INDEX`` block:

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>`
    Set object type :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`CREATE ANNOTATION` for details.


Example
-------

Create an object type ``User`` with an indexed ``name`` property:

.. code-block:: edgeql

    CREATE TYPE User {
        CREATE PROPERTY name -> str {
            SET default := '';
        };

        CREATE INDEX ON (.name);
    };


ALTER INDEX
===========

:eql-statement:


Alter the definition of an :ref:`index <ref_eql_sdl_indexes>`.

.. eql:synopsis::

    ALTER INDEX ON ( <index-expr> )
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      CREATE ANNOTATION <annotation-name> := <value>
      ALTER ANNOTATION <annotation-name> := <value>
      DROP ANNOTATION <annotation-name>


Description
-----------

``ALTER INDEX`` is used to change the :ref:`annotations
<ref_datamodel_annotations>` of an index. The *index-expr* is used to
identify the index to be altered.


Parameters
----------

:sdl:synopsis:`ON ( <index-expr> )`
    The specific expression for which the index is made.  Note also
    that ``<index-expr>`` itself has to be parenthesized.

The following subcommands are allowed in the ``ALTER INDEX`` block:

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>`
    Set index :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.
    See :eql:stmt:`CREATE ANNOTATION` for details.

:eql:synopsis:`ALTER ANNOTATION <annotation-name>;`
    Alter index :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`ALTER ANNOTATION <ALTER ANNOTATION>` for details.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.


Example
-------

Add an annotation to the index on the ``name`` property of object type
``User``:

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER INDEX ON (.name) {
            CREATE ANNOTATION title := "User name index";
        };
    };


DROP INDEX
==========

:eql-statement:

Remove an index from a given schema item.

.. eql:synopsis::

    DROP INDEX ON ( <index-expr> );

Description
-----------

``DROP INDEX`` removes an index from a schema item.

:sdl:synopsis:`ON ( <index-expr> )`
    The specific expression for which the index was made.

This statement can only be used as a subdefinition in another
DDL statement.


Example
-------

Drop the ``name`` index from the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        DROP INDEX ON (.name);
    };
