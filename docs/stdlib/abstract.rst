.. _ref_std_abstract_types:

==============
Abstract Types
==============

These types are used in definitions of polymorphic (generic) functions
that can be applied to a broad range of types.


----------


.. eql:type:: anytype

    :index: any anytype

    Generic type.

    It is a placeholder used in cases where no specific type
    requirements are needed, such as defining polymorphic parameters
    in functions and operators.


----------


.. eql:type:: std::anyscalar

    :index: any anytype scalar

    Abstract base scalar type.

    All scalar types are derived from this type.


----------


.. eql:type:: std::anyenum

    :index: any anytype enum

    Abstract base enumerated type.

    All :eql:type:`enum` types are derived from this type.


----------


.. eql:type:: anytuple

    :index: any anytype anytuple

    Generic tuple.

    Similarly to ``anytype`` it denotes a generic tuple without going
    into details of what the components are.  Just as with
    ``anytype``, this is useful when defining polymorphic parameters
    in functions and operators.


Abstract Numeric Types
======================

There are a number of abstract numeric types extending ``anyscalar``:

.. eql:type:: std::anyint

    :index: any anytype int

    Abstract base scalar type for
    :eql:type:`int16`, :eql:type:`int32`, and :eql:type:`int64`.


----------


.. eql:type:: std::anyfloat

    :index: any anytype float

    Abstract base scalar type for
    :eql:type:`float32` and :eql:type:`float64`.


----------


.. eql:type:: std::anyreal

    :index: any anytype

    Abstract base scalar type for
    :eql:type:`anyint`, :eql:type:`anyfloat`, and :eql:type:`decimal`.


Abstract Range Types
====================

There are some types that can be used to construct :ref:`ranges
<ref_std_range>`. These scalar types are distinguished by the following
abstract types:

.. eql:type:: std::anypoint

    :index: any anypoint anyrange

    Abstract base type for all valid ranges.

    Abstract base scalar type for :eql:type:`int32`, :eql:type:`int64`,
    :eql:type:`float32`, :eql:type:`float64`, :eql:type:`decimal`,
    :eql:type:`datetime`, :eql:type:`cal::local_datetime`, and
    :eql:type:`cal::local_date`.


----------


.. eql:type:: std::anydiscrete

    :index: any anydiscrete anyrange

    Abstract base type for all valid *discrete* ranges.

    Abstract base scalar type for :eql:type:`int32`, :eql:type:`int64`,
    and :eql:type:`cal::local_date`.


----------


.. eql:type:: std::anycontiguous

    :index: any anycontiguous anyrange

    Abstract base type for all valid *contiguous* ranges.

    Abstract base scalar type for :eql:type:`float32`, :eql:type:`float64`,
    :eql:type:`decimal`, :eql:type:`datetime`, and
    :eql:type:`cal::local_datetime`.
