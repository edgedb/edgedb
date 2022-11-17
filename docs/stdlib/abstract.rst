.. _ref_std_abstract_types:

==============
Abstract Types
==============

Abstract types are used to describe polymorphic functions, otherwise known as
"generic functions," which can be called on a broad range of value types.


----------


.. eql:type:: anytype

    :index: any anytype

    A generic type.

    This type is used as a placeholder in cases where there aren't specific
    type requirements such as when defining polymorphic parameters in functions
    and operators.


----------


.. eql:type:: std::anyscalar

    :index: any anytype scalar

    An abstract base scalar type.

    All scalar types are derived from this type.


----------


.. eql:type:: std::anyenum

    :index: any anytype enum

    An abstract base enumerator type.

    All :eql:type:`enum` types are derived from this type.


----------


.. eql:type:: anytuple

    :index: any anytype anytuple

    A generic tuple.

    Similar to :eql:type:`anytype`, this type is used to denote a generic
    tuple without detailing its constituents. This is useful when defining
    polymorphic parameters in functions and operators.


Abstract Numeric Types
======================

These abstract numeric types extend :eql:type:`anyscalar`.

.. eql:type:: std::anyint

    :index: any anytype int

    An abstract base scalar type for
    :eql:type:`int16`, :eql:type:`int32`, and :eql:type:`int64` types.


----------


.. eql:type:: std::anyfloat

    :index: any anytype float

    An abstract base scalar type for
    :eql:type:`float32` and :eql:type:`float64` types.


----------


.. eql:type:: std::anyreal

    :index: any anytype

    An abstract base scalar type for
    :eql:type:`anyint`, :eql:type:`anyfloat`, and :eql:type:`decimal` types.


Abstract Range Types
====================

These types serve as the base types for all :ref:`ranges <ref_std_range>`.

.. eql:type:: std::anypoint

    :index: any anypoint anyrange

    An abstract base type for all valid ranges.

    This is also an abstract base scalar type for
    :eql:type:`int32`, :eql:type:`int64`,
    :eql:type:`float32`, :eql:type:`float64`, :eql:type:`decimal`,
    :eql:type:`datetime`, :eql:type:`cal::local_datetime` and
    :eql:type:`cal::local_date` types.


----------


.. eql:type:: std::anydiscrete

    :index: any anydiscrete anyrange

    An abstract base type for all valid *discrete* ranges.

    This is also an abstract base scalar type for :eql:type:`int32`,
    :eql:type:`int64`, and :eql:type:`cal::local_date` types.


----------


.. eql:type:: std::anycontiguous

    :index: any anycontiguous anyrange

    An abstract base type for all valid *contiguous* ranges.

    This is also an abstract base scalar type for :eql:type:`float32`,
    :eql:type:`float64`, :eql:type:`decimal`, :eql:type:`datetime` and
    :eql:type:`cal::local_datetime` types.
