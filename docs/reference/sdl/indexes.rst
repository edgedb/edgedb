.. _ref_eql_sdl_indexes:

=======
Indexes
=======

This section describes the SDL declarations pertaining to
:ref:`indexes <ref_datamodel_indexes>`.


Example
-------

Declare an index for a "User" based on the "name" property:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
        required property name -> str;
        property address -> str;

        multi link friends -> User;

        # define an index for User based on name
        index on (.name) {
            annotation title := 'User name index';
        }
    }

.. code-block:: sdl

    type User {
        required name: str;
        address: str;

        multi friends: User;

        # define an index for User based on name
        index on (.name) {
            annotation title := 'User name index';
        }
    }

.. _ref_eql_sdl_indexes_syntax:

Syntax
------

Define a new index corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_indexes>`.

.. sdl:synopsis::

    index on ( <index-expr> )
    [ except ( <except-expr> ) ]
    [ "{" <annotation-declarations> "}" ] ;


Description
-----------

This declaration defines a new index with the following options:

:sdl:synopsis:`on ( <index-expr> )`
    The specific expression for which the index is made.

    The expression must be :ref:`Immutable <ref_reference_volatility>` but may
    refer to the indexed object's properties and links.

    Note also that ``<index-expr>`` itself has to be parenthesized.

:eql:synopsis:`except ( <exception-expr> )`
    An optional expression defining a condition to create exceptions
	to the index. If ``<exception-expr>`` evaluates to ``true``,
	the object is omitted from the index. If it evaluates
	to ``false`` or ``{}``, it appears in the index.

The valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set index :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Indexes <ref_datamodel_indexes>`
  * - :ref:`DDL > Indexes <ref_eql_ddl_indexes>`
  * - :ref:`Introspection > Indexes <ref_datamodel_introspection_indexes>`
