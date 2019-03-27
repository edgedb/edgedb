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
        property name -> str;
        property address -> str;

        multi link friends -> User;

        # define an index for User based on name
        index user_name_idx on (__subject__.name);
    }


Syntax
------

Define a new index corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_indexes>`.

.. sdl:synopsis::

    index <index-name> ON ( <index-expr> ) ;


Description
-----------

:sdl:synopsis:`<index-name>`
    The name of the index to be created.  No module name can be specified,
    indexes are always created in the same module as the parent type or
    link.

:sdl:synopsis:`on ( <index-expr> )`
    The specific expression for which the index is made.  Note also
    that ``<index-expr>`` itself has to be parenthesized.
