.. _ref_datamodel_scalars_numeric:

Numeric Types
=============

.. eql:type:: std::int16

    :index: int integer

    A 16-bit signed integer.

.. eql:type:: std::int32

    :index: int integer

    A 32-bit signed integer.

.. eql:type:: std::int64

    :index: int integer

    A 64-bit signed integer.

.. eql:type:: std::float32

    :index: float

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 6 decimal digits.

.. eql:type:: std::float64

    :index: float double

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 15 decimal digits.

.. eql:type:: std::decimal

    :index: numeric float

    Any number of arbitrary precision.

    The EdgeDB philosophy is that using decimal type should be an
    explicit opt-in, but once used, the values should not be
    accidentally cast into a numeric type with less precision.

    In accordance with this :ref:`the mathematical functions
    <ref_eql_functions_math>` are designed to keep the separation
    between decimal values and the rest of the numeric types.

    All of the following types can be explicitly cast into decimal:
    :eql:type:`str`, :eql:type:`int16`, :eql:type:`int32`,
    :eql:type:`int64`, :eql:type:`float32`, and :eql:type:`float64`.
