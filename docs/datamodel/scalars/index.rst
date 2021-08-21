.. _ref_datamodel_scalar_types:

============
Scalar Types
============

*Scalar types* are primitive individual types. Scalar type instances
hold a single value, called a *scalar value*. Unlike :ref:`objects
<ref_datamodel_object_types>` scalars don't have an ``id`` or any
other property. This means that the database can have as many
duplicates of the same *scalar value* as you like.

Even though, some *scalar types* such as :ref:`date and time scalars
<ref_datamodel_scalars_datetime>` or :eql:type:`json` have internal
structure, they require specialized functions and operators to access
this structure rather than using the ``.`` like objects or tuples.

Each subsection describes the standard EdgeDB scalar types in greater
details.

See also scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
and :ref:`functions and operators <ref_std>`.

.. toctree::
    :maxdepth: 3
    :hidden:

    numeric
    str
    bool
    datetime
    json
    enum
    bytes
    uuid
    casts
