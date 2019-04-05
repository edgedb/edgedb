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
        index user_name_idx on (__subject__.name);
    }


Syntax
------

Define a new index corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_indexes>`.

.. sdl:synopsis::

    index <index-name> on ( <index-expr> ) ;


Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE INDEX`.
