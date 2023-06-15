.. _ref_proto_dataformats:

=================
Data wire formats
=================

This section describes the data wire format of standard EdgeDB types.


.. _ref_protocol_fmt_array:

Sets and array<>
================

The set and array values are represented as the following structure:

.. code-block:: c

    struct SetOrArrayValue {
        // Number of dimensions, currently must
        // always be 0 or 1. 0 indicates an empty set or array.
        int32       ndims;

        // Reserved.
        int32       reserved0;

        // Reserved.
        int32       reserved1;

        // Dimension data.
        Dimension   dimensions[ndims];

        // Element data, the number of elements
        // in this array is the sum of dimension sizes:
        // sum((d.upper - d.lower + 1) for d in dimensions)
        Element     elements[];
    };

    struct Dimension {
        // Upper dimension bound, inclusive,
        // number of elements in the dimension
        // relative to the lower bound.
        int32       upper;

        // Lower dimension bound, always 1.
        int32       lower;
    };

    struct Element {
        // Encoded element data length in bytes.
        int32       length;

        // Element data.
        uint8       data[length];
    };


Note: zero-length arrays (and sets) are represented as a 12-byte value where
``dims`` equal to zero regardless of the shape in type descriptor.


Sets of arrays are a special case. Every array within a set is wrapped in an
Envelope. The full structure follows:

.. code-block:: c

    struct SetOfArrayValue {
        // Number of dimensions, currently must
        // always be 0 or 1. 0 indicates an empty set.
        int32 ndims;

        // Reserved.
        int32 reserved0;

        // Reserved.
        int32 reserved1;

        // Dimension data. Same layout as above.
        Dimension dimensions[ndims];

        // Envelope data, the number of elements
        // in this array is the sum of dimension sizes:
        // sum((d.upper - d.lower + 1) for d in dimensions)
        Envelope elements[];
    };

    struct Envelope {
        // Encoded envelope element length in bytes.
        int32 length;

        // Number of elements, currently must
        // always be 1.
        int32 nelems;

        // Reserved.
        int32 reserved

        // Element data. Same layout as above.
        Element element[nelems];
    };


.. _ref_protocol_fmt_tuple:

tuple<>,  namedtuple<>, and object<>
====================================

The values are represented as the following structure:

.. code-block:: c

    struct TupleOrNamedTupleOrObjectValue {
        // Number of elements
        int32       nelems;

        // Element data.
        Element     elements[nelems];
    };

    struct Element {
        // Reserved.
        int32       reserved;

        // Encoded element data length in bytes.
        int32       length;

        // Element data.
        uint8       data[length];
    };


Note that for objects, ``Element.length`` can be set to ``-1``, which
means an empty set.


.. _ref_protocol_fmt_sparse_obj:

Sparse Objects
==============

The values are represented as the following structure:

.. code-block:: c

    struct SparseObjectValue {
        // Number of elements
        int32       nelems;

        // Element data.
        Element     elements[nelems];
    };

    struct Element {
        // Index of the element in the input shape.
        int32       index;

        // Encoded element data length in bytes.
        int32       length;

        // Element data.
        uint8       data[length];
    };


.. _ref_protocol_fmt_range:

Ranges
====================================

The ranges are represented as the following structure:

.. code-block:: c

    struct Range {
        // A bit mask of range definition.
        uint8<RangeFlag> flags;

        // Lower boundary data.
        Boundary         lower;

        // Upper boundary data.
        Boundary         upper;
    };

    struct Boundary {
        // Encoded boundary data length in bytes.
        int32       length;

        // Boundary data.
        uint8       data[length];
    };

    enum RangeFlag {
        // Empty range.
        EMPTY   = 0x0001;

        // Included lower boundary.
        LB_INC  = 0x0002;

        // Included upper boundary.
        UB_INC  = 0x0004;

        // Inifinity (excluded) lower boundary.
        LB_INF  = 0x0008;

        // Infinity (excluded) upper boundary.
        UB_INF  = 0x0010;
    };


.. _ref_protocol_fmt_uuid:

std::uuid
=========

The :eql:type:`std::uuid` values are represented as a sequence of 16 unsigned
byte values.

For example, the UUID value ``b9545c35-1fe7-485f-a6ea-f8ead251abd3`` is
represented as:

.. code-block:: c

    0xb9 0x54 0x5c 0x35 0x1f 0xe7 0x48 0x5f
    0xa6 0xea 0xf8 0xea 0xd2 0x51 0xab 0xd3


.. _ref_protocol_fmt_str:

std::str
========

The :eql:type:`std::str` values are represented as a UTF-8 encoded byte string.
For example, the ``str`` value ``'Hello! 🙂'`` is encoded as:

.. code-block:: c

    0x48 0x65 0x6c 0x6c 0x6f 0x21 0x20 0xf0 0x9f 0x99 0x82


.. _ref_protocol_fmt_bytes:

std::bytes
==========

The :eql:type:`std::bytes` values are represented as-is.


.. _ref_protocol_fmt_int16:

std::int16
==========

The :eql:type:`std::int16` values are represented as two bytes, most
significant byte first.

For example, the ``int16`` value ``6556`` is represented as:

.. code-block:: c

    0x19 0x9c


.. _ref_protocol_fmt_int32:

std::int32
==========

The :eql:type:`std::int32` values are represented as four bytes, most
significant byte first.

For example, the ``int32`` value ``655665`` is represented as:

.. code-block:: c

    0x00 0x0a 0x01 0x31


.. _ref_protocol_fmt_int64:

std::int64
==========

The :eql:type:`std::int64` values are represented as eight bytes, most
significant byte first.

For example, the ``int64`` value ``123456789987654321`` is represented as:

.. code-block:: c

    0x01 0xb6 0x9b 0x4b 0xe0 0x52 0xfa 0xb1


.. _ref_protocol_fmt_float32:

std::float32
============

The :eql:type:`std::float32` values are represented as a IEEE 754-2008 binary
32-bit value, most significant byte first.

For example, the ``float32`` value ``-15.625`` is represented as:

.. code-block:: c

    0xc1 0x7a 0x00 0x00


.. _ref_protocol_fmt_float64:

std::float64
============

The :eql:type:`std::float32` values are represented as a IEEE 754-2008 binary
64-bit value, most significant byte first.

For example, the ``float64`` value ``-15.625`` is represented as:

.. code-block:: c

    0xc0 0x2f 0x40 0x00 0x00 0x00 0x00 0x00


.. _ref_protocol_fmt_decimal:

std::decimal
============

The :eql:type:`std::decimal` values are represented as the following structure:

.. code-block:: c

    struct Decimal {
        // Number of digits in digits[], can be 0.
        uint16               ndigits;

        // Weight of first digit.
        int16                weight;

        // Sign of the value
        uint16<DecimalSign>  sign;

        // Value display scale.
        uint16               dscale;

        // base-10000 digits.
        uint16                digits[ndigits];
    };

    enum DecimalSign {
        // Positive value.
        POS     = 0x0000;

        // Negative value.
        NEG     = 0x4000;
    };

The decimal values are represented as a sequence of base-10000 *digits*.  The
first digit is assumed to be multiplied by *weight* * 10000, i.e. there might
be up to weight + 1 digits before the decimal point. Trailing zeros can be
absent. It is possible to have negative weight.

*dscale*, or display scale, is the nominal precision expressed as number of
base-10 digits after the decimal point.  It is always non-negative.  dscale may
be more than the number of physically present fractional digits, implying
significant trailing zeroes.  The actual number of digits physically present in
the *digits* array contains trailing zeros to the next 4-byte increment
(meaning that integer and fractional part are always distinc base-10000
digits).

For example, the decimal value ``-15000.6250000`` is represented as:

.. code-block:: c

    // ndigits
    0x00 0x04

    // weight
    0x00 0x01

    // sign
    0x40 0x00

    // dscale
    0x00 0x07

    // digits
    0x00 0x01 0x13 0x88 0x18 0x6a 0x00 0x00


.. _ref_protocol_fmt_bool:

std::bool
=========

The :eql:type:`std::bool` values are represented as an int8 with
only two valid values: ``0x01`` for ``true`` and ``0x00`` for ``false``.


.. _ref_protocol_fmt_datetime:

std::datetime
=============

The :eql:type:`std::datetime` values are represented as a 64-bit integer,
most sigificant byte first.  The value is the number of *microseconds*
between the encoded datetime and January 1st 2000, 00:00 UTC.  A Unix
timestamp can be converted into an EdgeDB ``datetime`` value using this
formula:

.. code-block:: c

    edb_datetime = (unix_ts + 946684800) * 1000000

For example, the ``datetime`` value ``'2019-05-06T12:00+00:00'`` is
encoded as:

.. code-block:: c

    0x00 0x02 0x2b 0x35 0x9b 0xc4 0x10 0x00

See :ref:`client libraries <ref_bindings_datetime>` section for more info
about how to handle different precision when encoding data.


.. _ref_protocol_fmt_local_datetime:

cal::local_datetime
===================

The :eql:type:`cal::local_datetime` values are represented as a 64-bit integer,
most sigificant byte first.  The value is the number of *microseconds*
between the encoded datetime and January 1st 2000, 00:00.

For example, the ``local_datetime`` value ``'2019-05-06T12:00'`` is
encoded as:

.. code-block:: c

    0x00 0x02 0x2b 0x35 0x9b 0xc4 0x10 0x00

See :ref:`client libraries <ref_bindings_datetime>` section for more info
about how to handle different precision when encoding data.


.. _ref_protocol_fmt_local_date:

cal::local_date
===============

The :eql:type:`cal::local_date` values are represented as a 32-bit integer,
most sigificant byte first.  The value is the number of *days*
between the encoded date and January 1st 2000.

For example, the ``local_date`` value ``'2019-05-06'`` is
encoded as:

.. code-block:: c

    0x00 0x00 0x1b 0x99


.. _ref_protocol_fmt_local_time:

cal::local_time
===============

The :eql:type:`cal::local_time` values are represented as a 64-bit integer,
most sigificant byte first.  The value is the number of *microseconds*
since midnight.

For example, the ``local_time`` value ``'12:10'`` is
encoded as:

.. code-block:: c

    0x00 0x00 0x00 0x0a 0x32 0xae 0xf6 0x00

See :ref:`client libraries <ref_bindings_datetime>` section for more info
about how to handle different precision when encoding data.


.. _ref_protocol_fmt_duration:

std::duration
=============

The :eql:type:`std::duration` values are represented as the following
structure:

.. code-block:: c

    struct Duration {
        int64   microseconds;

        // deprecated, is always 0
        int32   days;

        // deprecated, is always 0
        int32   months;
    };

For example, the ``duration`` value ``'48 hours 45 minutes 7.6 seconds'`` is
encoded as:

.. code-block:: c

    // microseconds
    0x00 0x00 0x00 0x28 0xdd 0x11 0x72 0x80

    // days
    0x00 0x00 0x00 0x00

    // months
    0x00 0x00 0x00 0x00

See :ref:`client libraries <ref_bindings_datetime>` section for more info
about how to handle different precision when encoding data.


.. _ref_protocol_fmt_relative_duration:

cal::relative_duration
======================

The :eql:type:`cal::relative_duration` values are represented as the following
structure:

.. code-block:: c

    struct Duration {
        int64   microseconds;
        int32   days;
        int32   months;
    };

For example, the ``cal::relative_duration`` value
``'2 years 7 months 16 days 48 hours 45 minutes 7.6 seconds'`` is encoded as:

.. code-block:: c

    // microseconds
    0x00 0x00 0x00 0x28 0xdd 0x11 0x72 0x80

    // days
    0x00 0x00 0x00 0x10

    // months
    0x00 0x00 0x00 0x1f

See :ref:`client libraries <ref_bindings_datetime>` section for more info
about how to handle different precision when encoding data.


.. _ref_protocol_fmt_date_duration:

cal::date_duration
======================

The :eql:type:`cal::date_duration` values are represented as the following
structure:

.. code-block:: c

    struct DateDuration {
        int64   reserved;
        int32   days;
        int32   months;
    };

For example, the ``cal::date_duration`` value ``'1 years 2 days'`` is encoded
as:

.. code-block:: c

    // reserved
    0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00

    // days
    0x00 0x00 0x00 0x02

    // months
    0x00 0x00 0x00 0x0c


.. _ref_protocol_fmt_json:

std::json
=========

The :eql:type:`std::json` values are represented as the following structure:

.. code-block:: c

    struct JSON {
        uint8   format;
        uint8   jsondata[];
    };

*format* is currently always ``1``, and *jsondata* is a UTF-8 encoded JSON
string.


.. _ref_protocol_fmt_bigint:

std::bigint
============

The :eql:type:`std::bigint` values are represented as the following structure:

.. code-block:: c

    struct BigInt {
        // Number of digits in digits[], can be 0.
        uint16               ndigits;

        // Weight of first digit.
        int16                weight;

        // Sign of the value
        uint16<DecimalSign>  sign;

        // Reserved value, must be zero
        uint16               reserved;

        // base-10000 digits.
        uint16                digits[ndigits];
    };

    enum BigIntSign {
        // Positive value.
        POS     = 0x0000;

        // Negative value.
        NEG     = 0x4000;
    };

The decimal values are represented as a sequence of base-10000 *digits*.
The first digit is assumed to be multiplied by *weight* * 10000, i.e. there
might be up to weight + 1 digits.  Trailing zeros can be absent.

For example, the bigint value ``-15000`` is represented as:

.. code-block:: c

    // ndigits
    0x00 0x02

    // weight
    0x00 0x01

    // sign
    0x40 0x00

    // reserved
    0x00 0x00

    // digits
    0x00 0x01 0x13 0x88


.. _ref_protocol_fmt_memory:

cfg::memory
===========

The :eql:type:`cfg::memory` values are represented as a number of *bytes*
encoded as a 64-bit integer, most sigificant byte first.

For example, the ``cfg::memory`` value ``123MiB`` is represented as:

.. code-block:: c

    0x00 0x00 0x00 0x00 0x07 0xb0 0x00 0x00
