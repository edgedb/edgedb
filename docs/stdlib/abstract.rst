.. _ref_std_abstract_types:

==============
Abstract Types
==============

Abstract types are used to describe a polymorphic, otherwise known as
generic function to be applicable over a broad range of types.


----------


.. eql:type:: anytype

    :index: any anytype

    Represents a generic type.

    This type is used as a placeholder in cases where there isn't a specific
    set of type requirements, or are not necessary, such as defining
    polymorphic parameters in functions and operators.


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

    Similar to :eql:type:`anytype`, this type is used to denote a generic
    tuple without going into detail of which components there are. This is
    useful when defining polymorphic parameters in functions and operators.


Abstract Numeric Types
======================

Abstract numeric types are used when wanting to extend off of
:eql:type:`anyscalar`.

.. eql:type:: std::anyint

    :index: any anytype int

    Represents an abstract base scalar type for
    :eql:type:`int16`, :eql:type:`int32` and :eql:type:`int64` types.


----------


.. eql:type:: std::anyfloat

    :index: any anytype float

    Represents an abstract base scalar type for
    :eql:type:`float32` and :eql:type:`float64` types.


----------


.. eql:type:: std::anyreal

    :index: any anytype

    Represents an abstract base scalar type for
    :eql:type:`anyint`, :eql:type:`anyfloat` and :eql:type:`decimal` types.


Abstract Range Types
====================

Like abstract numeric types, :ref:`ranges <ref_std_range>` can also be
specified.

.. eql:type:: std::anypoint

    :index: any anypoint anyrange point

    Represents an abstract base type for all valid ranges.

    This is also an abstract base scalar type for
    :eql:type:`int32`, :eql:type:`int64`,
    :eql:type:`float32`, :eql:type:`float64`, :eql:type:`decimal`,
    :eql:type:`datetime`, :eql:type:`cal::local_datetime` and
    :eql:type:`cal::local_date` types.


----------


.. eql:type:: std::anydiscrete

    :index: any anydiscrete anyrange discrete

    Represents an abstract base type for all valid *discrete* ranges.

    This is also an abstract base scalar type for :eql:type:`int32`,
    :eql:type:`int64` and :eql:type:`cal::local_date` types.


----------


.. eql:type:: std::anycontiguous

    :index: any anycontiguous anyrange

    Represents an abstract base type for all valid *contiguous* ranges.

    This is also an abstract base scalar type for :eql:type:`float32`,
    :eql:type:`float64`, :eql:type:`decimal`, :eql:type:`datetime` and
    :eql:type:`cal::local_datetime` types.
