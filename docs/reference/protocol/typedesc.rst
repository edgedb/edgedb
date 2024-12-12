.. _ref_proto_typedesc:

================
Type descriptors
================

This section describes how type information for query input and results
is encoded.  Specifically, this is needed to decode the server response to
the :ref:`ref_protocol_msg_command_data_description` message.

The type descriptor is essentially a list of type information *blocks*:

* each *block* encodes one type;

* *blocks* can reference other *blocks*.

While parsing the *blocks*, a database driver can assemble an
*encoder* or a *decoder* of the EdgeDB binary data.

An *encoder* is used to encode objects, native to the driver's runtime,
to binary data that EdgeDB can decode and work with.

A *decoder* is used to decode data from EdgeDB native format to
data types native to the driver.

.. versionchanged:: _default

    There is one special type with *type id* of zero:
    ``00000000-0000-0000-0000-000000000000``. The describe result of this type
    contains zero *blocks*. It's used when a statement returns no meaningful
    results, e.g. the ``CREATE DATABASE example`` statement.  It is also used
    to represent the input descriptor when a query does not receive any
    arguments, or the state descriptor for an empty/default state.

.. versionchanged:: 5.0

    There is one special type with *type id* of zero:
    ``00000000-0000-0000-0000-000000000000``. The describe result of this type
    contains zero *blocks*. It's used when a statement returns no meaningful
    results, e.g. the ``CREATE BRANCH example`` statement.  It is also used to
    represent the input descriptor when a query does not receive any arguments,
    or the state descriptor for an empty/default state.

.. versionadded:: 6.0

   Added ``SQLRecordDescriptor``.


Descriptor and type IDs
=======================

The descriptor and type IDs in structures below are intended to be semi-stable
unique identifiers of a type.  Fundamental types have globally stable known
IDs, and type IDs for schema-defined types (i.e. with
``schema_defined = true``) persist.  Ephemeral type ids are derived from
type structure and are not guaranteed to be stable, but are still useful
as cache keys.


Set Descriptor
==============

.. code-block:: c

    struct SetDescriptor {
        // Indicates that this is a Set value descriptor.
        uint8   tag = 0;

        // Descriptor ID.
        uuid    id;

        // Set element type descriptor index.
        uint16  type;
    };

Set values are encoded on the wire as
:ref:`single-dimensional arrays <ref_protocol_fmt_array>`.


Scalar Type Descriptor
======================

.. code-block:: c

    struct ScalarTypeDescriptor {
        // Indicates that this is a
        // Scalar Type descriptor.
        uint8   tag = 3;

        // Schema type ID.
        uuid    id;

        // Schema type name.
        string  name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool    schema_defined;

        // Number of ancestor scalar types.
        uint16  ancestors_count;

        // Indexes of ancestor scalar type descriptors
        // in ancestor resolution order (C3).
        uint16  ancestors[ancestors_count];
    };

The descriptor IDs for fundamental scalar types are constant.
The following table lists all EdgeDB fundamental type descriptor IDs:

.. list-table::
   :header-rows: 1

   * - ID
     - Type

   * - ``00000000-0000-0000-0000-000000000100``
     - :ref:`std::uuid <ref_protocol_fmt_uuid>`

   * - ``00000000-0000-0000-0000-000000000101``
     - :ref:`std::str <ref_protocol_fmt_str>`

   * - ``00000000-0000-0000-0000-000000000102``
     - :ref:`std::bytes <ref_protocol_fmt_bytes>`

   * - ``00000000-0000-0000-0000-000000000103``
     - :ref:`std::int16 <ref_protocol_fmt_int16>`

   * - ``00000000-0000-0000-0000-000000000104``
     - :ref:`std::int32 <ref_protocol_fmt_int32>`

   * - ``00000000-0000-0000-0000-000000000105``
     - :ref:`std::int64 <ref_protocol_fmt_int64>`

   * - ``00000000-0000-0000-0000-000000000106``
     - :ref:`std::float32 <ref_protocol_fmt_float32>`

   * - ``00000000-0000-0000-0000-000000000107``
     - :ref:`std::float64 <ref_protocol_fmt_float64>`

   * - ``00000000-0000-0000-0000-000000000108``
     - :ref:`std::decimal <ref_protocol_fmt_decimal>`

   * - ``00000000-0000-0000-0000-000000000109``
     - :ref:`std::bool <ref_protocol_fmt_bool>`

   * - ``00000000-0000-0000-0000-00000000010A``
     - :ref:`std::datetime <ref_protocol_fmt_datetime>`

   * - ``00000000-0000-0000-0000-00000000010E``
     - :ref:`std::duration <ref_protocol_fmt_duration>`

   * - ``00000000-0000-0000-0000-00000000010F``
     - :ref:`std::json <ref_protocol_fmt_json>`

   * - ``00000000-0000-0000-0000-00000000010B``
     - :ref:`cal::local_datetime <ref_protocol_fmt_local_datetime>`

   * - ``00000000-0000-0000-0000-00000000010C``
     - :ref:`cal::local_date <ref_protocol_fmt_local_date>`

   * - ``00000000-0000-0000-0000-00000000010D``
     - :ref:`cal::local_time <ref_protocol_fmt_local_time>`

   * - ``00000000-0000-0000-0000-000000000110``
     - :ref:`std::bigint <ref_protocol_fmt_bigint>`

   * - ``00000000-0000-0000-0000-000000000111``
     - :ref:`cal::relative_duration <ref_protocol_fmt_relative_duration>`

   * - ``00000000-0000-0000-0000-000000000112``
     - :ref:`cal::date_duration <ref_protocol_fmt_date_duration>`

   * - ``00000000-0000-0000-0000-000000000130``
     - :ref:`cfg::memory <ref_protocol_fmt_memory>`


Tuple Type Descriptor
=====================

.. code-block:: c

    struct TupleTypeDescriptor {
        // Indicates that this is a
        // Tuple Type descriptor.
        uint8     tag = 4;

        // Schema type ID.
        uuid      id;

        // Schema type name.
        string    name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool      schema_defined;

        // Number of ancestor scalar types.
        uint16    ancestors_count;

        // Indexes of ancestor scalar type descriptors
        // in ancestor resolution order (C3).
        uint16    ancestors[ancestors_count];

        // The number of elements in tuple.
        uint16    element_count;

        // Indexes of element type descriptors.
        uint16    element_types[element_count];
    };

An empty tuple type descriptor has an ID of
``00000000-0000-0000-0000-0000000000FF``.


Named Tuple Type Descriptor
===========================

.. code-block:: c

    struct NamedTupleTypeDescriptor {
        // Indicates that this is a
        // Named Tuple Type descriptor.
        uint8         tag = 5;

        // Schema type ID.
        uuid          id;

        // Schema type name.
        string        name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool          schema_defined;

        // Number of ancestor scalar types.
        uint16        ancestors_count;

        // Indexes of ancestor scalar type descriptors
        // in ancestor resolution order (C3).
        uint16        ancestors[ancestors_count];

        // The number of elements in tuple.
        uint16        element_count;

        // Indexes of element descriptors.
        TupleElement  elements[element_count];
    };

    struct TupleElement {
        // Field name.
        string  name;

        // Field type descriptor index.
        int16   type;
    };


Array Type Descriptor
=====================

.. code-block:: c

    struct ArrayTypeDescriptor {
        // Indicates that this is an
        // Array Type descriptor.
        uint8   tag = 6;

        // Schema type ID.
        uuid    id;

        // Schema type name.
        string  name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool    schema_defined;

        // Number of ancestor scalar types.
        uint16  ancestors_count;

        // Indexes of ancestor scalar type descriptors
        // in ancestor resolution order (C3).
        uint16  ancestors[ancestors_count];

        // Array element type.
        uint16  type;

        // The number of array dimensions, at least 1.
        uint16  dimension_count;

        // Sizes of array dimensions, -1 indicates
        // unbound dimension.
        int32   dimensions[dimension_count];
    };


Enumeration Type Descriptor
===========================

.. code-block:: c

    struct EnumerationTypeDescriptor {
        // Indicates that this is an
        // Enumeration Type descriptor.
        uint8   tag = 7;

        // Schema type ID.
        uuid    id;

        // Schema type name.
        string  name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool    schema_defined;

        // Number of ancestor scalar types.
        uint16  ancestors_count;

        // Indexes of ancestor scalar type descriptors
        // in ancestor resolution order (C3).
        uint16  ancestors[ancestors_count];

        // The number of enumeration members.
        uint16  member_count;

        // Names of enumeration members.
        string  members[member_count];
    };


Range Type Descriptor
=====================

.. code-block:: c

    struct RangeTypeDescriptor {
        // Indicates that this is a
        // Range Type descriptor.
        uint8   tag = 9;

        // Schema type ID.
        uuid    id;

        // Schema type name.
        string  name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool    schema_defined;

        // Number of ancestor scalar types.
        uint16  ancestors_count;

        // Indexes of ancestor scalar type descriptors
        // in ancestor resolution order (C3).
        uint16  ancestors[ancestors_count];

        // Range type descriptor index.
        uint16  type;
    };

Ranges are encoded on the wire as :ref:`ranges <ref_protocol_fmt_range>`.


Object Type Descriptor
======================

.. code-block:: c

    struct ObjectTypeDescriptor {
        // Indicates that this is an
        // object type descriptor.
        uint8   tag = 10;

        // Schema type ID.
        uuid    id;

        // Schema type name (can be empty for ephemeral free object types).
        string  name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool    schema_defined;
    };


Compound Type Descriptor
========================

.. code-block:: c

    struct CompoundTypeDescriptor {
        // Indicates that this is a
        // compound type descriptor.
        uint8                 tag = 11;

        // Schema type ID.
        uuid                  id;

        // Schema type name.
        string                name;

        // Whether the type is defined in the schema
        // or is ephemeral.
        bool                  schema_defined;

        // Compound type operation, see TypeOperation below.
        uint8<TypeOperation>  op;

        // Number of compound type components.
        uint16                component_count;

        // Compound type component type descriptor indexes.
        uint16                components[component_count];
    };

    enum TypeOperation {
        // Foo | Bar
        UNION         = 1;

        // Foo & Bar
        INTERSECTION  = 2;
    };


Object Output Shape Descriptor
==============================

.. code-block:: c

    struct ObjectShapeDescriptor {
        // Indicates that this is an
        // Object Shape descriptor.
        uint8         tag = 1;

        // Descriptor ID.
        uuid          id;

        // Whether is is an ephemeral free shape,
        // if true, then `type` would always be 0
        // and should not be interpreted.
        bool          ephemeral_free_shape;

        // Object type descriptor index.
        uint16        type;

        // Number of elements in shape.
        uint16        element_count;

        // Array of shape elements.
        ShapeElement  elements[element_count];
    };

    struct ShapeElement {
        // Field flags:
        //   1 << 0: the field is implicit
        //   1 << 1: the field is a link property
        //   1 << 2: the field is a link
        uint32              flags;

        // The cardinality of the shape element.
        uint8<Cardinality>  cardinality;

        // Element name.
        string              name;

        // Element type descriptor index.
        uint16              type;

        // Source schema type descriptor index
        // (useful for polymorphic queries).
        uint16              source_type;
    };

.. eql:struct:: edb.protocol.enums.Cardinality

Objects are encoded on the wire as :ref:`tuples <ref_protocol_fmt_tuple>`.


Input Shape Descriptor
======================

.. code-block:: c

    struct InputShapeDescriptor {
        // Indicates that this is an
        // Object Shape descriptor.
        uint8              tag = 8;

        // Descriptor ID.
        uuid               id;

        // Number of elements in shape.
        uint16             element_count;

        // Shape elements.
        InputShapeElement  elements[element_count];
    };

    struct InputShapeElement {
        // Field flags, currently always zero.
        uint32              flags;

        // The cardinality of the shape element.
        uint8<Cardinality>  cardinality;

        // Element name.
        string              name;

        // Element type descriptor index.
        uint16              type;
    };

Input objects are encoded on the wire as
:ref:`sparse objects <ref_protocol_fmt_sparse_obj>`.


Type Annotation Text Descriptor
===============================

.. code-block:: c

    struct TypeAnnotationDescriptor {
        // Indicates that this is an
        // Type Annotation descriptor.
        uint8   tag = 127;

        // Index of the descriptor the
        // annotation is for.
        uint16  descriptor;

        // Annotation key.
        string  key;

        // Annotation value.
        string  value;
    };


SQL Record Descriptor
=====================

.. code-block:: c

    struct SQLRecordDescriptor {
        // Indicates that this is a
        // SQL Record descriptor.
        uint8         tag = 13;

        // Descriptor ID.
        uuid          id;

        // Number of elements in record.
        uint16        element_count;

        // Array of shape elements.
        SQLRecordElement  elements[element_count];
    };

    struct SQLRecordElement {
        // Element name.
        string              name;

        // Element type descriptor index.
        uint16              type;
    };
