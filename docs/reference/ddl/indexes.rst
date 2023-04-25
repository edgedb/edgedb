.. _ref_eql_ddl_indexes:

=======
Indexes
=======

This section describes the DDL commands pertaining to
:ref:`indexes <ref_datamodel_indexes>`.


Create index
============

:eql-statement:


:ref:`Define <ref_eql_sdl_indexes>` an new index for a given object
type or link.

.. eql:synopsis::

    create index on ( <index-expr> )
    [ except ( <except-expr> ) ]
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      create annotation <annotation-name> := <value>


Description
-----------

The command ``create index`` constructs a new index for a given object type or
link using *index-expr*.


Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL index declaration <ref_eql_sdl_indexes_syntax>`. There's
only one subcommand that is allowed in the ``create index`` block:

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set object type :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.


Example
-------

Create an object type ``User`` with an indexed ``name`` property:

.. code-block:: edgeql

    create type User {
        create property name -> str {
            set default := '';
        };

        create index on (.name);
    };


Alter index
===========

:eql-statement:


Alter the definition of an :ref:`index <ref_eql_sdl_indexes>`.

.. eql:synopsis::

    alter index on ( <index-expr> ) [ except ( <except-expr> ) ]
    [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>


Description
-----------

The command ``alter index`` is used to change the :ref:`annotations
<ref_datamodel_annotations>` of an index. The *index-expr* is used to
identify the index to be altered.


Parameters
----------

:sdl:synopsis:`on ( <index-expr> )`
    The specific expression for which the index is made.  Note also
    that ``<index-expr>`` itself has to be parenthesized.

The following subcommands are allowed in the ``alter index`` block:

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set index :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.
    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter index :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.


Example
-------

Add an annotation to the index on the ``name`` property of object type
``User``:

.. code-block:: edgeql

    alter type User {
        alter index on (.name) {
            create annotation title := "User name index";
        };
    };


Drop index
==========

:eql-statement:

Remove an index from a given schema item.

.. eql:synopsis::

    drop index on ( <index-expr> ) [ except ( <except-expr> ) ];

Description
-----------

The command ``drop index`` removes an index from a schema item.

:sdl:synopsis:`on ( <index-expr> )`
    The specific expression for which the index was made.

This statement can only be used as a subdefinition in another
DDL statement.


Example
-------

Drop the ``name`` index from the ``User`` object type:

.. code-block:: edgeql

    alter type User {
        drop index on (.name);
    };

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Indexes <ref_datamodel_indexes>`
  * - :ref:`SDL > Indexes <ref_eql_sdl_indexes>`
  * - :ref:`Introspection > Indexes <ref_datamodel_introspection_indexes>`
