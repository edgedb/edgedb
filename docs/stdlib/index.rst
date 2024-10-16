.. eql:section-intro-page:: stdlib
.. versioned-section::

.. _ref_std:

================
Standard Library
================

.. toctree::
    :maxdepth: 3
    :hidden:

    generic
    set
    type
    math
    string
    bool
    numbers
    json
    uuid
    enum
    datetime
    array
    tuple
    range
    bytes
    sequence
    objects
    abstract
    constraints
    net
    fts
    sys
    cfg
    pgcrypto
    pg_trgm
    pg_unaccent
    pgvector
    deprecated

EdgeDB comes with a rigorously defined type system consisting of **scalar
types**, **collection types** (like arrays and tuples), and **object types**.
There is also a library of built-in functions and operators for working with
each datatype.


.. _ref_datamodel_typesystem:

Scalar Types
------------

.. _ref_datamodel_scalar_types:

*Scalar types* store primitive data.

- :ref:`Strings <ref_std_string>`
- :ref:`Numbers <ref_std_numeric>`
- :ref:`Booleans <ref_std_logical>`
- :ref:`Dates and times <ref_std_datetime>`
- :ref:`Enums <ref_std_enum>`
- :ref:`JSON <ref_std_json>`
- :ref:`UUID <ref_std_uuid>`
- :ref:`Bytes <ref_std_bytes>`
- :ref:`Sequences <ref_std_sequence>`
- :ref:`Abstract types <ref_std_abstract_types>`: these are the types that
  undergird the scalar hierarchy.

.. _ref_datamodel_collection_types:

Collection Types
----------------

*Collection types* are special generic types used to group homogeneous or
heterogeneous data.

- :ref:`Arrays <ref_std_array>`
- :ref:`Tuples <ref_std_tuple>`

Range Types
-----------

- :ref:`Range <ref_std_range>`
- :ref:`Multirange <ref_std_multirange>`

Object Types
------------

- :ref:`Object Types <ref_std_object_types>`

Types and Sets
--------------

- :ref:`Sets <ref_std_set>`
- :ref:`Types <ref_std_type>`
- :ref:`Casting <ref_eql_casts>`

Utilities
---------

- :ref:`Math <ref_std_math>`
- :ref:`Comparison <ref_std_generic>`
- :ref:`Constraints <ref_std_constraints>`
- :ref:`Full-text Search <ref_std_fts>`
- :ref:`System <ref_std_sys>`

Extensions
----------

- :ref:`ext::pgvector <ref_ext_pgvector>`
