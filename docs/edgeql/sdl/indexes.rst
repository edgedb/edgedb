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

    type User {
        required property name -> str;
        property address -> str;

        multi link friends -> User;

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
    [ "{" <annotation-declarations> "}" ] ;


Description
-----------

This declaration defines a new index with the following options:

:sdl:synopsis:`on ( <index-expr> )`
    The specific expression for which the index is made.  Note also
    that ``<index-expr>`` itself has to be parenthesized.

The valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set index :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.
