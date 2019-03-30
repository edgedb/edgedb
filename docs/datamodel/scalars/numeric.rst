.. _ref_datamodel_scalars_numeric:

Numeric Types
=============

It's possbile to explicitly :ref:`cast <ref_eql_expr_typecast>`
between any numeric types. All numeric types can also be cast to and
from :eql:type:`str` and :eql:type:`json`.

See also the list of standard
:ref:`mathematical functions <ref_eql_functions_math>` and
generic functions such as :eql:func:`min`.


.. eql:type:: std::int16

    :index: int integer

    A 16-bit signed integer.

    An integer value in range from ``-32768`` to ``+32767`` (inclusive).

.. eql:type:: std::int32

    :index: int integer

    A 32-bit signed integer.

    An integer value in range from ``-2147483648`` to ``+2147483647``
    (inclusive).

.. eql:type:: std::int64

    :index: int integer

    A 64-bit signed integer.

    An integer value in range from ``-9223372036854775808`` to
    ``+9223372036854775807`` (inclusive).

.. eql:type:: std::float32

    :index: float

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 6 decimal digits. The
    approximate range of a ``float32`` is ``-3.4e+38`` to
    ``+3.4e+38``.

.. eql:type:: std::float64

    :index: float double

    A variable precision, inexact number.

    Minimal guaranteed precision is at least 15 decimal digits. The
    approximate range of a ``float32`` is ``-1.7e+308`` to ``+1.7e+308``.

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

    A decimal type has it's own literal:

    .. code-block:: edgeql-repl

        db> SELECT 42n IS decimal;
        {true}
        db> SELECT 1.23n IS decimal;
        {true}
