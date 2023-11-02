.. _ref_std_abstract_types:

==============
Abstract Types
==============

Abstract types are used to describe polymorphic functions, otherwise known as
"generic functions," which can be called on a broad range of types.


----------


.. eql:type:: anytype

    :index: any anytype

    A generic type.

    It is a placeholder used in cases where no specific type
    requirements are needed, such as defining polymorphic parameters
    in functions and operators.


----------


.. eql:type:: std::anyobject

    :index: any anytype object

    A generic object.

    Similarly to :eql:type:`anytype`, this type is used to denote a generic
    object. This is useful when defining polymorphic parameters in functions
    and operators as it conforms to whatever type is actually passed. This is
    different friom :eql:type:`BaseObject` which although is the parent type
    of any object also only has an ``id`` property, making access to other
    properties and links harder.


----------


.. eql:type:: std::anyscalar

    :index: any anytype scalar

    An abstract base scalar type.

    All scalar types are derived from this type.


----------


.. eql:type:: std::anyenum

    :index: any anytype enum

    An abstract base enumerated type.

    All :eql:type:`enum` types are derived from this type.


----------


.. eql:type:: anytuple

    :index: any anytype anytuple

    A generic tuple.

    Similarly to :eql:type:`anytype`, this type is used to denote a generic
    tuple without detailing its component types. This is useful when defining
    polymorphic parameters in functions and operators.


Abstract Numeric Types
======================

These abstract numeric types extend :eql:type:`anyscalar`.

.. eql:type:: std::anyint

    :index: any anytype int

    An abstract base scalar type for
    :eql:type:`int16`, :eql:type:`int32`, and :eql:type:`int64`.


----------


.. eql:type:: std::anyfloat

    :index: any anytype float

    An abstract base scalar type for
    :eql:type:`float32` and :eql:type:`float64`.


----------


.. eql:type:: std::anyreal

    :index: any anytype

    An abstract base scalar type for
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

    :index: any anydiscrete anyrange discrete

    An abstract base type for all valid *discrete* ranges.

    This is an abstract base scalar type for :eql:type:`int32`,
    :eql:type:`int64`, and :eql:type:`cal::local_date`.


----------


.. eql:type:: std::anycontiguous

    :index: any anycontiguous anyrange

    An abstract base type for all valid *contiguous* ranges.

    This is an abstract base scalar type for :eql:type:`float32`,
    :eql:type:`float64`, :eql:type:`decimal`, :eql:type:`datetime`, and
    :eql:type:`cal::local_datetime`.
