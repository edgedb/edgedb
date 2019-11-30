.. _ref_datamodel_scalars_numeric:

Numerics
========

:edb-alt-title: Numeric Types

It's possible to explicitly :eql:op:`cast <CAST>`
between all numeric types. All numeric types can also be cast to and
from :eql:type:`str` and :eql:type:`json`.

----------


.. eql:type:: std::int16

    :index: int integer

    A 16-bit signed integer.

    An integer value in range from ``-32768`` to ``+32767`` (inclusive).


----------


.. eql:type:: std::int32

    :index: int integer

    A 32-bit signed integer.

    An integer value in range from ``-2147483648`` to ``+2147483647``
    (inclusive).


----------


.. eql:type:: std::int64

    :index: int integer

    A 64-bit signed integer.

    An integer value in range from ``-9223372036854775808`` to
    ``+9223372036854775807`` (inclusive).


----------


.. eql:type:: std::float32

    :index: float

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 6 decimal digits. The
    approximate range of a ``float32`` is ``-3.4e+38`` to
    ``+3.4e+38``.


----------


.. eql:type:: std::float64

    :index: float double

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 15 decimal digits. The
    approximate range of a ``float32`` is ``-1.7e+308`` to ``+1.7e+308``.


----------


.. eql:type:: std::bigint

    :index: numeric bigint

    Arbitrary precision integer.

    The EdgeDB philosophy is that using bigint type should be an
    explicit opt-in, but once used, the values should not be
    accidentally cast into a numeric type with less precision.

    In accordance with this :ref:`the mathematical functions
    <ref_eql_functions_math>` are designed to keep the separation
    between bigint values and the rest of the numeric types.

    All of the following types can be explicitly cast into bigint:
    :eql:type:`str`, :eql:type:`json`, :eql:type:`int16`,
    :eql:type:`int32`, :eql:type:`int64`, :eql:type:`float32`,
    :eql:type:`float64`, and :eql:type:`decimal`.

    A bigint literal is an integer literal followed by 'n':

    .. code-block:: edgeql-repl

        db> SELECT 42n IS bigint;
        {true}

    Note that is a float literal followed by 'n' produces a
    :eql:type:`decimal`:

    .. code-block:: edgeql-repl

        db> SELECT 1.23n IS decimal;
        {true}

        db> SELECT 1e+100n IS decimal;
        {true}


----------


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
    :eql:type:`str`, :eql:type:`json`, :eql:type:`int16`,
    :eql:type:`int32`, :eql:type:`int64`, :eql:type:`float32`,
    :eql:type:`float64`, and :eql:type:`bigint`.

    A decimal literal is a float literal followed by 'n':

    .. code-block:: edgeql-repl

        db> SELECT 1.23n IS decimal;
        {true}

        db> SELECT 1e+100n IS decimal;
        {true}

    Note that an integer literal (without a dot or exponent) followed by 'n'
    produces a :eql:type:`bigint`:

    .. code-block:: edgeql-repl

        db> SELECT 42n IS bigint;
        {true}


----------


.. eql:type:: std::sequence

    Auto-incrementing sequence of :eql:type:`int64`.


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
:ref:`arithmetic operators and numeric converter functions
<ref_eql_funcops_numeric>`,
:ref:`mathematical functions <ref_eql_functions_math>`,
:eql:func:`max`,
:eql:func:`min`,
:eql:func:`random`,
:eql:func:`round`,
:eql:func:`sum`.
