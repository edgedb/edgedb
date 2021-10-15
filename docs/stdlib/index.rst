.. eql:section-intro-page:: stdlib

.. _ref_std:

================
Standard Library
================

.. toctree::
    :maxdepth: 3
    :hidden:

    generic
    string
    numerics
    bool
    datetime
    enum
    json
    uuid
    bytes
    sequence
    abstract
    array
    tuple
    objects
    set
    type
    casts
    math
    sys
    constraints
    deprecated

EdgeDB comes with a rigorously defined type system consisting of **scalar
types**, **collection types** (like arrays and tuples), and **object types**.
There is also a library of built-in functions and operators for working with
each datatype.


.. _ref_datamodel_typesystem:

Scalar Types
------------

.. _ref_datamodel_scalar_types:

*Scalar types* are primitive individual types. Scalar type instances
hold a single value, called a *scalar value*. Unlike :ref:`objects
<ref_datamodel_object_types>` scalars don't have an ``id`` or any
other property.

- :ref:`String <ref_std_string>`
- :ref:`Numbers <ref_std_numeric>`
- :ref:`Boolean <ref_std_logical>`
- :ref:`Dates and times <ref_std_datetime>`
- :ref:`Enums <ref_std_enum>`
- :ref:`JSON <ref_std_json>`
- :ref:`UUID <ref_std_uuid>`
- :ref:`Bytes <ref_std_bytes>`
- :ref:`Sequence <ref_std_sequence>`
- :ref:`Abstract types <ref_std_abstract_types>`: these are the types that
  undergird the scalar hierarchy.

See also :ref:`standard library <ref_std>` scalar types
as well as scalar type :ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
and :ref:`introspection <ref_eql_introspection_scalar_types>`.

.. _ref_datamodel_collection_types:

Collection Types
----------------

*Collection types* are special generic types used to group homogeneous or
heterogeneous data.

- :ref:`Arrays <ref_std_array>`
- :ref:`Tuples <ref_std_tuple>`


Object Types
------------

- :ref:`Object Types <ref_std_object_types>`

Types and Sets
--------------

- :ref:`Sets <ref_std_set>`
- :ref:`Types <ref_std_type>`
- :ref:`Casting <ref_std_casts>`

Utilities
---------

- :ref:`Math <ref_std_math>`
- :ref:`Comparison <ref_std_generic>`
- :ref:`Constraints <ref_std_constraints>`
- :ref:`System <ref_std_sys>`


