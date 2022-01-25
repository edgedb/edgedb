.. _ref_std_numeric:

=======
Numbers
=======

:edb-alt-title: Numerical Types, Functions, and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`int16`
      - 16-bit integer

    * - :eql:type:`int32`
      - 32-bit integer

    * - :eql:type:`int64`
      - 64-bit integer

    * - :eql:type:`float32`
      - 32-bit floating point number

    * - :eql:type:`float64`
      - 64-bit floating point number

    * - :eql:type:`bigint`
      - Arbitrary precision integer.

    * - :eql:type:`decimal`
      - Arbitrary precision number.

    * - :eql:op:`anyreal + anyreal <plus>`
      - :eql:op-desc:`plus`

    * - :eql:op:`anyreal - anyreal <minus>`
      - :eql:op-desc:`minus`

    * - :eql:op:`-anyreal <uminus>`
      - :eql:op-desc:`uminus`

    * - :eql:op:`anyreal * anyreal <mult>`
      - :eql:op-desc:`mult`

    * - :eql:op:`anyreal / anyreal <div>`
      - :eql:op-desc:`div`

    * - :eql:op:`anyreal // anyreal <floordiv>`
      - :eql:op-desc:`floordiv`

    * - :eql:op:`anyreal % anyreal <mod>`
      - :eql:op-desc:`mod`

    * - :eql:op:`anyreal ^ anyreal <pow>`
      - :eql:op-desc:`pow`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`sum`
      - :eql:func-desc:`sum`

    * - :eql:func:`min`
      - :eql:func-desc:`min`

    * - :eql:func:`max`
      - :eql:func-desc:`max`

    * - :eql:func:`round`
      - :eql:func-desc:`round`

    * - :eql:func:`random`
      - :eql:func-desc:`random`

Mathematical functions
----------------------

.. include:: math_funcops_table.rst

String parsing
--------------

.. list-table::
  :class: funcoptable

  * - :eql:func:`to_bigint`
    - :eql:func-desc:`to_bigint`

  * - :eql:func:`to_decimal`
    - :eql:func-desc:`to_decimal`

  * - :eql:func:`to_int16`
    - :eql:func-desc:`to_int16`

  * - :eql:func:`to_int32`
    - :eql:func-desc:`to_int32`

  * - :eql:func:`to_int64`
    - :eql:func-desc:`to_int64`

  * - :eql:func:`to_float32`
    - :eql:func-desc:`to_float32`

  * - :eql:func:`to_float64`
    - :eql:func-desc:`to_float64`

It's possible to explicitly :eql:op:`cast <cast>`
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
    approximate range of a ``float64`` is ``-1.7e+308`` to ``+1.7e+308``.


----------


.. eql:type:: std::bigint

    :index: numeric bigint

    Arbitrary precision integer.

    The EdgeDB philosophy is that using bigint type should be an
    explicit opt-in, but once used, the values should not be
    accidentally cast into a numeric type with less precision.

    In accordance with this :ref:`the mathematical functions
    <ref_std_math>` are designed to keep the separation
    between bigint values and the rest of the numeric types.

    All of the following types can be explicitly cast into bigint:
    :eql:type:`str`, :eql:type:`json`, :eql:type:`int16`,
    :eql:type:`int32`, :eql:type:`int64`, :eql:type:`float32`,
    :eql:type:`float64`, and :eql:type:`decimal`.

    A bigint literal is an integer literal followed by 'n':

    .. code-block:: edgeql-repl

        db> select 42n is bigint;
        {true}

    To represent really big integers it is possible to use the
    exponent notation (e.g. ``1e20n`` instead of ``100000000000000000000n``)
    as long as the exponent is positive and there is no dot anywhere.

    .. code-block:: edgeql-repl

        db> select 1e+100n is bigint;
        {true}

    When a float literal is followed by ‘n’ it produces a
    :eql:type:`decimal` instead:

    .. code-block:: edgeql-repl

        db> select 1.23n is decimal;
        {true}

        db> select 1.0e+100n is decimal;
        {true}

    .. note::

        Caution is advised when casting ``bigint`` values into
        ``json``. The JSON specification does not have a limit on
        significant digits, so a ``bigint`` number can be losslessly
        represented in JSON. However, JSON decoders in many languages
        will read all such numbers as some kind of 32- or 64-bit
        number type, which may result in errors or precision loss. If
        such loss is unacceptable, then consider casting the value
        into ``str`` and decoding it on the client side into a more
        appropriate type.


----------


.. eql:type:: std::decimal

    :index: numeric float

    Any number of arbitrary precision.

    The EdgeDB philosophy is that using a decimal type should be an
    explicit opt-in, but once used, the values should not be
    accidentally cast into a numeric type with less precision.

    In accordance with this :ref:`the mathematical functions
    <ref_std_math>` are designed to keep the separation
    between decimal values and the rest of the numeric types.

    All of the following types can be explicitly cast into decimal:
    :eql:type:`str`, :eql:type:`json`, :eql:type:`int16`,
    :eql:type:`int32`, :eql:type:`int64`, :eql:type:`float32`,
    :eql:type:`float64`, and :eql:type:`bigint`.

    A decimal literal is a float literal followed by 'n':

    .. code-block:: edgeql-repl

        db> select 1.23n is decimal;
        {true}

        db> select 1.0e+100n is decimal;
        {true}

    Note that an integer literal (without a dot or exponent) followed
    by ‘n’ produces a :eql:type:`bigint`. A literal without a dot
    and with a positive exponent makes a :eql:type:`bigint`, too:

    .. code-block:: edgeql-repl

        db> select 42n is bigint;
        {true}

        db> select 12e+34n is bigint;
        {true}

    .. note::

        Caution is advised when casting ``decimal`` values into
        ``json``. The JSON specification does not have a limit on
        significant digits, so a ``decimal`` number can be losslessly
        represented in JSON. However, JSON decoders in many languages
        will read all such numbers as some kind of floating point
        values, which may result in precision loss. If such loss is
        unacceptable, then consider casting the value into ``str`` and
        decoding it on the client side into a more appropriate type.


----------


.. eql:operator:: plus: anyreal + anyreal -> anyreal

    :index: plus add

    Arithmetic addition.

    .. code-block:: edgeql-repl

        db> select 2 + 2;
        {4}


----------


.. eql:operator:: minus: anyreal - anyreal -> anyreal

    :index: minus subtract

    Arithmetic subtraction.

    .. code-block:: edgeql-repl

        db> select 3 - 2;
        {1}


----------


.. eql:operator:: uminus: - anyreal -> anyreal

    :index: unary minus subtract

    Arithmetic negation.

    .. code-block:: edgeql-repl

        db> select -5;
        {-5}


----------


.. eql:operator:: mult: anyreal * anyreal -> anyreal

    :index: multiply multiplication

    Arithmetic multiplication.

    .. code-block:: edgeql-repl

        db> select 2 * 10;
        {20}


----------


.. eql:operator:: div: anyreal / anyreal -> anyreal

    :index: divide division

    Arithmetic division.

    .. code-block:: edgeql-repl

        db> select 10 / 4;
        {2.5}

    Division by zero results in an error:

    .. code-block:: edgeql-repl

        db> select 10 / 0;
        DivisionByZeroError: division by zero


----------


.. eql:operator:: floordiv: anyreal // anyreal -> anyreal

    :index: floor divide division

    Floor division.

    The result is rounded down to the nearest integer. It is
    equivalent to using regular division and the applying
    :eql:func:`math::floor` to the result.

    .. code-block:: edgeql-repl

        db> select 10 // 4;
        {2}
        db> select math::floor(10 / 4);
        {2}
        db> select -10 // 4;
        {-3}

    It also works on :eql:type:`float <anyfloat>`, :eql:type:`bigint`, and
    :eql:type:`decimal` types. The type of the result corresponds to
    the type of the operands:

    .. code-block:: edgeql-repl

        db> select 3.7 // 1.1;
        {3.0}
        db> select 3.7n // 1.1n;
        {3.0n}
        db> select 37 // 11;
        {3}

    Regular division, floor division, and :eql:op:`%<mod>` are
    related in the following way: ``A // B  =  (A - (A % B)) / B``.


----------


.. eql:operator:: mod: anyreal % anyreal -> anyreal

    :index: modulo mod division

    Remainder from division (modulo).

    This is the remainder from floor division. Just as is
    the case with :eql:op:`//<floordiv>` the result type of the
    remainder operator corresponds to the operand type:

    .. code-block:: edgeql-repl

        db> select 10 % 4;
        {2}
        db> select 10n % 4;
        {2n}
        db> select -10 % 4;
        {2}
        db> # floating arithmetic is inexact, so
        ... # we get 0.3999999999999999 instead of 0.4
        ... select 3.7 % 1.1;
        {0.3999999999999999}
        db> select 3.7n % 1.1n;
        {0.4n}
        db> select 37 % 11;
        {4}

    Regular division, :eql:op:`//<floordiv>` and :eql:op:`%<mod>` are
    related in the following way: ``A // B  =  (A - (A % B)) / B``.

    Modulo division by zero results in an error:

    .. code-block:: edgeql-repl

        db> select 10 % 0;
        DivisionByZeroError: division by zero


-----------


.. eql:operator:: pow: anyreal ^ anyreal -> anyreal

    :index: power pow

    Power operation.

    .. code-block:: edgeql-repl

        db> select 2 ^ 4;
        {16}


----------


.. eql:function:: std::round(value: int64) -> float64
                  std::round(value: float64) -> float64
                  std::round(value: bigint) -> bigint
                  std::round(value: decimal) -> decimal
                  std::round(value: decimal, d: int64) -> decimal

    Round to the nearest value.

    There's a difference in how ties (which way ``0.5`` is rounded)
    are handled depending on the type of the input *value*.

    :eql:type:`float64` tie is rounded to the nearest even number:

    .. code-block:: edgeql-repl

        db> select round(1.2);
        {1}

        db> select round(1.5);
        {2}

        db> select round(2.5);
        {2}

    :eql:type:`decimal` tie is rounded away from 0:

    .. code-block:: edgeql-repl

        db> select round(1.2n);
        {1n}

        db> select round(1.5n);
        {2n}

        db> select round(2.5n);
        {3n}

    Additionally, when rounding a :eql:type:`decimal` *value* an
    optional argument *d* can be provided to specify to what decimal
    point the *value* must to be rounded.

    .. code-block:: edgeql-repl

        db> select round(163.278n, 2);
        {163.28n}

        db> select round(163.278n, 1);
        {163.3n}

        db> select round(163.278n, 0);
        {163n}

        db> select round(163.278n, -1);
        {160n}

        db> select round(163.278n, -2);
        {200n}


----------


.. eql:function:: std::random() -> float64

    Return a pseudo-random number in the range ``0.0 <= x < 1.0``.

    .. code-block:: edgeql-repl

        db> select random();
        {0.62649393780157}


------------


.. eql:function:: std::to_bigint(s: str, fmt: optional str={}) -> bigint

    :index: parse bigint

    Create a :eql:type:`bigint` value.

    Parse a :eql:type:`bigint` from the input *s* and optional format
    specification *fmt*.

    .. code-block:: edgeql-repl

        db> select to_bigint('-000,012,345', 'S099,999,999,999');
        {-12345n}
        db> select to_bigint('31st', '999th');
        {31n}

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------



.. eql:function:: std::to_decimal(s: str, fmt: optional str={}) -> decimal

    :index: parse decimal

    Create a :eql:type:`decimal` value.

    Parse a :eql:type:`decimal` from the input *s* and optional format
    specification *fmt*.

    .. code-block:: edgeql-repl

        db> select to_decimal('-000,012,345', 'S099,999,999,999');
        {-12345.0n}
        db> select to_decimal('-012.345');
        {-12.345n}
        db> select to_decimal('31st', '999th');
        {31.0n}

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_int16(s: str, fmt: optional str={}) -> int16

    :index: parse int16

    Create an :eql:type:`int16` value.

    Parse an :eql:type:`int16` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_int32(s: str, fmt: optional str={}) -> int32

    :index: parse int32

    Create an :eql:type:`int32` value.

    Parse an :eql:type:`int32` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_int64(s: str, fmt: optional str={}) -> int64

    :index: parse int64

    Create an :eql:type:`int64` value.

    Parse an :eql:type:`int64` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_float32(s: str, fmt: optional str={}) -> float32

    :index: parse float32

    Create a :eql:type:`float32` value.

    Parse a :eql:type:`float32` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_float64(s: str, fmt: optional str={}) -> float64

    :index: parse float64

    Create a :eql:type:`float64` value.

    Parse a :eql:type:`float64` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.
