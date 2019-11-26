.. _ref_proto_typedesc:

================
Type Descriptors
================

This section describes how type information for query input and results
is encoded.  Specifically, this is needed to decode the server response to
the :ref:`ref_protocol_msg_describe_statement` message.

The type descriptor is essentially a list of type information *blocks*:

* each *block* encodes one type;

* *blocks* can reference other *blocks*.

While parsing the *blocks*, a database driver can assemble an
*encoder* or a *decoder* of the EdgeDB binary data.

An *encoder* is used to encode objects, native to the driver's runtime,
to binary data that EdegDB can decode and work with.

A *decoder* is used to decode data from EdgeDB native format to
data types native to the driver.


Set Descriptor
==============

.. code-block:: c

    struct SetDescriptor {
        // Indicates that this is a Set value descriptor.
        int8   type = 0;

        // Descriptor ID.
        uuid   id;

        // Set element type descriptor index.
        int16  type_pos;
    };

Set values are encoded on the wire as
:ref:`single-dimensional arrays <ref_protocol_fmt_array>`.


Object Shape Descriptor
=======================

.. code-block:: c

    struct ObjectShapeDescriptor {
        // Indicates that this is an
        // Object Shape descriptor.
        int8            type = 1;

        // Descriptor ID.
        uuid            id;

        // Number of elements in shape.
        int16           element_count;

        ShapeElement    elements[element_count];
    };

    struct ShapeElement {
        // Field flags:
        //   1 << 0: the field is implicit
        //   1 << 1: the field is a link property
        //   1 << 2: the field is a link
        int8            flags;

        // Field name.
        string          name;

        // Field type descriptor index.
        int16           type_pos;
    };

Objects are encoded on the wire as :ref:`tuples <ref_protocol_fmt_tuple>`.


Base Scalar Type Descriptor
===========================

.. code-block:: c

    struct BaseScalarTypeDescriptor {
        // Indicates that this is an
        // Base Scalar Type descriptor.
        int8            type = 2;

        // Descriptor ID.
        uuid            id;
    };


The descriptor IDs for base scalar types are constant.
The following table lists all EdgeDB base types descriptor IDs:

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

   * - ``00000000-0000-0000-0000-00000000010B``
     - :ref:`std::local_datetime <ref_protocol_fmt_local_datetime>`

   * - ``00000000-0000-0000-0000-00000000010C``
     - :ref:`std::local_date <ref_protocol_fmt_local_date>`

   * - ``00000000-0000-0000-0000-00000000010D``
     - :ref:`std::local_time <ref_protocol_fmt_local_time>`

   * - ``00000000-0000-0000-0000-00000000010E``
     - :ref:`std::duration <ref_protocol_fmt_duration>`

   * - ``00000000-0000-0000-0000-00000000010F``
     - :ref:`std::json <ref_protocol_fmt_json>`


Scalar Type Descriptor
======================

.. code-block:: c

    struct ScalarTypeDescriptor {
        // Indicates that this is a
        // Scalar Type descriptor.
        int8            type = 3;

        // Descriptor ID.
        uuid            id;

        // Parent type descriptor index.
        int16           base_type_pos;
    };


Tuple Type Descriptor
=====================

.. code-block:: c

    struct TupleTypeDescriptor {
        // Indicates that this is a
        // Tuple Type descriptor.
        int8      type = 4;

        // Descriptor ID.
        uuid      id;

        // The number of elements in tuple.
        int16     element_count;

        // Indexes of element type descriptors.
        int16     element_types[element_count];
    };

An empty tuple type descriptor has an ID of
``00000000-0000-0000-0000-0000000000FF``.


Named Tuple Type Descriptor
===========================

.. code-block:: c

    struct ScalarTypeDescriptor {
        // Indicates that this is a
        // Named Tuple Type descriptor.
        int8         type = 5;

        // Descriptor ID.
        uuid         id;

        // The number of elements in tuple.
        int16        element_count;

        // Indexes of element type descriptors.
        TupleElement elements[element_count];
    };

    struct TupleElement {
        // Field name.
        string  name;

        // Field type descriptor index.
        int16   type_pos;
    };


Array Type Descriptor
=====================

.. code-block:: c

    struct ArrayTypeDescriptor {
        // Indicates that this is an
        // Array Type descriptor.
        int8         type = 6;

        // Descriptor ID.
        uuid         id;

        // Element type descriptor index.
        int16        type_pos;

        // The number of array dimensions, at least 1.
        int16        dimension_count;

        // Sizes of array dimensions, -1 indicates
        // unbound dimension.
        int32        dimensions[dimension_count];
    };


Enumeration Type Descriptor
===========================

.. code-block:: c

    struct EnumerationTypeDescriptor {
        // Indicates that this is an
        // Enumeration Type descriptor.
        int8         type = 7;

        // Descriptor ID.
        uuid         id;

        // The number of enumeration members.
        int16        member_count;

        // Names of enumeration members.
        string       members[member_count];
    };


Type Annotation Descriptor
==========================


.. code-block:: c

    struct TypeAnnotationDescriptor {
        // Indicates that this is an
        // Type Annotation descriptor.
        int8         type = 0xf0..0xff;

        // ID of the descriptor the
        // annotation is for.
        uuid         id;

        // Annotation text.
        string       annotation;
    };
