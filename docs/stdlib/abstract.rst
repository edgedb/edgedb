.. _ref_std_abstract_types:

==============
Abstract Types
==============

An abstract type is used when describing a polymorphic (generic) function
applicable to a broad range of types.


----------


.. eql:type:: anytype

    :index: any anytype

    Represents a generic type.

    This type is a placeholder used in cases where no specific type
    requirements are necessary, such as defining polymorphic parameters
    in functions and operators.


----------


.. eql:type:: std::anyscalar

    :index: any anytype scalar

    Represents an abstract base scalar type.

    All scalar types are derived from this type.


----------


.. eql:type:: std::anyenum

    :index: any anytype enum

    Represents an abstract base enumerator type.

    All :eql:type:`enum` types are derived from this type.


----------


.. eql:type:: anytuple

    :index: any anytype anytuple

    Represents a generic tuple.

    Similarly to ``anytype``, this type denotes a generic tuple without
    going into the details of what the components are. This is useful
    when defining polymorphic parameters in functions and operators.


Abstract Numeric Types
======================

There are a number of abstract numeric types extending ``anyscalar``:

.. eql:type:: std::anyint

    :index: any anytype int

    Represents an abstract base scalar type for
    :eql:type:`int16`, :eql:type:`int32` and :eql:type:`int64`.


----------


.. eql:type:: std::anyfloat

    :index: any anytype float

    Represents an abstract base scalar type for
    :eql:type:`float32` and :eql:type:`float64`.


----------


.. eql:type:: std::anyreal

    :index: any anytype

    Represents an abstract base scalar type for
    :eql:type:`anyint`, :eql:type:`anyfloat` and :eql:type:`decimal`.


Abstract Range Types
====================

Like abstract types, you may also specify :ref:`ranges <ref_std_range>`.
These scalar types are distinguished by the following:

.. eql:type:: std::anypoint

    :index: any anypoint anyrange

    Represents an abstract base type for all valid ranges.

    This is also an abstract base scalar type for
    :eql:type:`int32`, :eql:type:`int64`,
    :eql:type:`float32`, :eql:type:`float64`, :eql:type:`decimal`,
    :eql:type:`datetime`, :eql:type:`cal::local_datetime` and
    :eql:type:`cal::local_date`.


----------


.. eql:type:: std::anydiscrete

    :index: any anydiscrete anyrange

    Represents an abstract base type for all valid *discrete* ranges.

    This is also an abstract base scalar type for :eql:type:`int32`,
    :eql:type:`int64` and :eql:type:`cal::local_date`.


----------


.. eql:type:: std::anycontiguous

    :index: any anycontiguous anyrange

    Represents an abstract base type for all valid *contiguous* ranges.

    This is also an abstract base scalar type for :eql:type:`float32`,
    :eql:type:`float64`, :eql:type:`decimal`, :eql:type:`datetime` and
    :eql:type:`cal::local_datetime`.
