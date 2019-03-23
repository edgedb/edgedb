.. _ref_eql_sdl_indexes:

=======
Indexes
=======

This section describes the SDL declarations pertaining to
:ref:`indexes <ref_datamodel_indexes>`.

Define a new index corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_indexes>`.

.. sdl:synopsis::

    index <index-name> ON <index-expr> ;


Description
-----------

:sdl:synopsis:`<index-name>`
    The name of the index to be created.  No module name can be specified,
    indexes are always created in the same module as the parent type or
    link.

:sdl:synopsis:`<index-expr>`
    The specific expression for which the index is made.
