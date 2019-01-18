.. _ref_datamodel_scalars_numeric:

Number Types
============

.. eql:type:: std::decimal

    :index: numeric float

    Any number of arbitrary precision.

    The EdgeDB philosophy is that using decimal type should be very
    explicitly opt-in, but once used the values should not be
    accidentally cast into a numeric type with less precision. In
    accordance with this :ref:`the mathematical functions
    <ref_eql_functions_math>` are designed to keep the separation
    between decimal values and the rest of the numeric types.

    All of the following types can be explicitly cast into decimal:
    :eql:type:`int16`, :eql:type:`int32`, :eql:type:`int64`,
    :eql:type:`float32`, and :eql:type:`float64`.

.. eql:type:: std::int16

    :index: int

    A 16-bit signed integer.

.. eql:type:: std::int32

    :index: int

    A 32-bit signed integer.

.. eql:type:: std::int64

    :index: int

    A 64-bit signed integer.

.. eql:type:: std::float32

    :index: float

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 6 decimal digits.

.. eql:type:: std::float64

    :index: float

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 15 decimal digits.


Abstract Number Types
=====================

.. eql:type:: std::anyint

    :index: anytype int

    Abstract base scalar type for
    :eql:type:`int16`, :eql:type:`int32`, and :eql:type:`int64`.

.. eql:type:: std::anyfloat

    :index: anytype float

    Abstract base scalar type for
    :eql:type:`float32` and :eql:type:`float64`.

.. eql:type:: std::anyreal

    :index: anytype

    Abstract base scalar type for
    :eql:type:`anyint`, :eql:type:`anyfloat`, and :eql:type:`decimal`.
