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

Bitwise functions
-----------------

.. list-table::
  :class: funcoptable

  * - :eql:func:`bit_and`
    - :eql:func-desc:`bit_and`

  * - :eql:func:`bit_or`
    - :eql:func-desc:`bit_or`

  * - :eql:func:`bit_xor`
    - :eql:func-desc:`bit_xor`

  * - :eql:func:`bit_not`
    - :eql:func-desc:`bit_not`

  * - :eql:func:`bit_lshift`
    - :eql:func-desc:`bit_lshift`

  * - :eql:func:`bit_rshift`
    - :eql:func-desc:`bit_rshift`

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

    Represents a 16-bit signed integer.

    The value of an :eql:type:`int16` ranges from ``-32768`` to ``+32767``
    (inclusive).


----------


.. eql:type:: std::int32

    :index: int integer

    Represents a 32-bit signed integer.

    The value of an :eql:type:`int32` ranges from ``-2147483648`` to
    ``+2147483647`` (inclusive).


----------


.. eql:type:: std::int64

    :index: int integer

    Represents a 64-bit signed integer.

    An integer value in range from ``-9223372036854775808`` to
    ``+9223372036854775807`` (inclusive).


----------


.. eql:type:: std::float32

    :index: float

    Represents a variable precision, inexact number.

    The minimal guaranteed precision is at least 6 decimal digits. The
    approximate range of a :eql:type:`float32` ranges from ``-3.4e+38`` to
    ``+3.4e+38``.


----------


.. eql:type:: std::float64

    :index: float double

    Represents a variable precision, inexact number. Also see
    :eql:type:`float32`.

    The minimal guaranteed precision is at least 15 decimal digits. The
    approximate range of a :eql:type:`float64` ranges from ``-1.7e+308`` to
    ``+1.7e+308``.


----------


.. eql:type:: std::bigint

    :index: numeric bigint

    Represents an arbitrary precision integer.

    In EdgeDB, the philosophy of :eql:type:`bigint` is that using this type
    should be an explicit opt-in and not implicit. Once used, these values
    should not be accidentally casted to a different numerical type that gives
    less precision.

    In addition with this, :ref:`our mathematical functions <ref_std_math>`
    are designed to keep separation between big integer values and the
    rest of our numerical types.

    All of the following types can be explicitly cast into a
    :eql:type:`bigint` type:

    - :eql:type:`str`
    - :eql:type:`json`
    - :eql:type:`int16`
    - :eql:type:`int32`
    - :eql:type:`int64`
    - :eql:type:`float32`
    - :eql:type:`float64`
    - :eql:type:`decimal`

    A :eql:type:`bigint` literal is also an integer literal, followed by
    ``n``:

    .. code-block:: edgeql-repl

        db> select 42n is bigint;
        {true}

    To represent really big integers, it is possible to use the
    exponent notation (e.g. ``1e20n`` instead of ``100000000000000000000n``)
    so as long as the exponent is positive and there is no dot anywhere:

    .. code-block:: edgeql-repl

        db> select 1e+100n is bigint;
        {true}

    When a float literal is followed by ``n``, it will produce a
    :eql:type:`decimal` value instead:

    .. code-block:: edgeql-repl

        db> select 1.23n is decimal;
        {true}

        db> select 1.0e+100n is decimal;
        {true}

    .. note::

        Caution is advised when casting :eql:type:`bigint` values into
        :eql:type:`json`. The JSON specification does not have a limit on
        significant digits, so a big integer number can be losslessly
        represented in JSON. However, JSON decoders in many languages
        will read all such numbers as some kind of 32 or 64-bit
        number type, which may result in errors or precision loss. If
        such loss is unacceptable, then consider casting the value
        into a :eql:type:`str` and decoding it on the client side into a more
        appropriate type.


----------


.. eql:type:: std::decimal

    :index: numeric float

    Represents any number of arbitrary precision.

    In EdgeDB, the philosophy of :eql:type:`bigint` is that using this type
    should be an explicit opt-in and not implicit. Once used, these values
    should not be accidentally casted to a different numerical type that gives
    less precision.

    In addition to this, :ref:`our mathematical functions <ref_std_math>`
    are designed to keep separation between decimal values and the rest of
    our numerical types.

    All of the following types can be explicitly cast into decimal:
    - :eql:type:`str`
    - :eql:type:`json`
    - :eql:type:`int16`
    - :eql:type:`int32`
    - :eql:type:`int64`
    - :eql:type:`float32`
    - :eql:type:`float64`
    - :eql:type:`bigint`

    A decimal literal is a float literal, followed by ``n``:

    .. code-block:: edgeql-repl

        db> select 1.23n is decimal;
        {true}

        db> select 1.0e+100n is decimal;
        {true}

    Note that an integer literal (without a dot or exponent) followed
    by ``n`` produces a :eql:type:`bigint` value. A literal without a dot
    and with a positive exponent makes a :eql:type:`bigint` too:

    .. code-block:: edgeql-repl

        db> select 42n is bigint;
        {true}

        db> select 12e+34n is bigint;
        {true}

    .. note::

        Caution is advised when casting :eql:type:`decimal` values into
        :eql:type:`json`. The JSON specification does not have a limit on
        significant digits, so a decimal number can be losslessly
        represented in JSON. However, JSON decoders in many languages
        will read all such numbers as some kind of floating point
        values, which may result in precision loss. If such loss is
        unacceptable, then consider casting the value into a :eql:type:`str`
        and decoding it on the client side into a more appropriate type.


----------


.. eql:operator:: plus: anyreal + anyreal -> anyreal

    :index: plus add

    Performs arithmetic addition between two arbitrary numbers:

    .. code-block:: edgeql-repl

        db> select 2 + 2;
        {4}


----------


.. eql:operator:: minus: anyreal - anyreal -> anyreal

    :index: minus subtract

    Performs arithmetic subtraction between two arbitrary numbers:

    .. code-block:: edgeql-repl

        db> select 3 - 2;
        {1}


----------


.. eql:operator:: uminus: - anyreal -> anyreal

    :index: unary minus subtract

    Performs a logical negation of an arthimetic, arbitrarily numerical value:

    .. code-block:: edgeql-repl

        db> select -5;
        {-5}


----------


.. eql:operator:: mult: anyreal * anyreal -> anyreal

    :index: multiply multiplication

    Performs arithmetic multiplication between two arbitrary numbers:

    .. code-block:: edgeql-repl

        db> select 2 * 10;
        {20}


----------


.. eql:operator:: div: anyreal / anyreal -> anyreal

    :index: divide division

    Performs arithmetic division between two arbitrary numbers:

    .. code-block:: edgeql-repl

        db> select 10 / 4;
        {2.5}

    Dividing a value by zero will result in an exception:

    .. code-block:: edgeql-repl

        db> select 10 / 0;
        DivisionByZeroError: division by zero


----------


.. eql:operator:: floordiv: anyreal // anyreal -> anyreal

    :index: floor divide division

    Performs a floor based divison between two arbitrary numbers.

    The result of this operation is rounded down to its nearest integer.
    It is the equivalent to using regular division and then applying
    :eql:func:`math::floor` to the result:

    .. code-block:: edgeql-repl

        db> select 10 // 4;
        {2}
        db> select math::floor(10 / 4);
        {2}
        db> select -10 // 4;
        {-3}

    This also works on :eql:type:`float <anyfloat>`, :eql:type:`bigint` and
    :eql:type:`decimal` types. The type of the result corresponds to
    the type of its operands:

    .. code-block:: edgeql-repl

        db> select 3.7 // 1.1;
        {3.0}
        db> select 3.7n // 1.1n;
        {3.0n}
        db> select 37 // 11;
        {3}

    Regular division, floor division and :eql:op:`%<mod>` operations are
    related in the following way: ``A // B  =  (A - (A % B)) / B``.


----------


.. eql:operator:: mod: anyreal % anyreal -> anyreal

    :index: modulo mod division

    Retrieves the remainder from a division operation. (modulo)

    This is also the remainder from floor division. Just as is
    the case with the :eql:op:`//<floordiv>` operator, the resulting type of
    the remainder operator corresponds to the left operand's type:

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

    In regular division, the :eql:op:`//<floordiv>` and :eql:op:`%<mod>`
    operators are related in the following way:
    ``A // B  =  (A - (A % B)) / B``.

    Modulo division by zero will result in an exception:

    .. code-block:: edgeql-repl

        db> select 10 % 0;
        DivisionByZeroError: division by zero


-----------


.. eql:operator:: pow: anyreal ^ anyreal -> anyreal

    :index: power pow

    Performs a power (exponential) operation by two arbitrary numbers:

    .. code-block:: edgeql-repl

        db> select 2 ^ 4;
        {16}


----------


.. eql:function:: std::round(value: int64) -> float64
                  std::round(value: float64) -> float64
                  std::round(value: bigint) -> bigint
                  std::round(value: decimal) -> decimal
                  std::round(value: decimal, d: int64) -> decimal

    Returns a number from ``value`` rounded by its nearest integer.

    There's a difference in how ties (which way ``0.5`` is rounded)
    are handled depending on the type of the inputted ``value``.

    The :eql:type:`float64` tie is rounded to the nearest even number:

    .. code-block:: edgeql-repl

        db> select round(1.2);
        {1}

        db> select round(1.5);
        {2}

        db> select round(2.5);
        {2}

    But the :eql:type:`decimal` tie is rounded away from zero:

    .. code-block:: edgeql-repl

        db> select round(1.2n);
        {1n}

        db> select round(1.5n);
        {2n}

        db> select round(2.5n);
        {3n}

    Additionally, when rounding a :eql:type:`decimal` value, an
    optional argument ``d`` may be provided to further specify what decimal
    point the value must to be rounded to:

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

    Returns a pseudo-random number in the range of ``0.0 <= x < 1.0``:

    .. code-block:: edgeql-repl

        db> select random();
        {0.62649393780157}


----------


.. eql:function:: std::bit_and(l: int16, r: int16) -> int16
                  std::bit_and(l: int32, r: int32) -> int32
                  std::bit_and(l: int64, r: int64) -> int64

    Returns the result of a bitwise AND operator for two integers:

    .. code-block:: edgeql-repl

        db> select bit_and(17, 3);
        {1}


----------


.. eql:function:: std::bit_or(l: int16, r: int16) -> int16
                  std::bit_or(l: int32, r: int32) -> int32
                  std::bit_or(l: int64, r: int64) -> int64

    Returns the result of a bitwise OR operator for two integers.

    .. code-block:: edgeql-repl

        db> select bit_or(17, 3);
        {19}


----------


.. eql:function:: std::bit_xor(l: int16, r: int16) -> int16
                  std::bit_xor(l: int32, r: int32) -> int32
                  std::bit_xor(l: int64, r: int64) -> int64

    Returns the result of an exclusive, bitwise OR operator for two integers:

    .. code-block:: edgeql-repl

        db> select bit_xor(17, 3);
        {18}


----------


.. eql:function:: std::bit_not(r: int16) -> int16
                  std::bit_not(r: int32) -> int32
                  std::bit_not(r: int64) -> int64

    Returns the result of a bitwise negation operator for two integers.

    Bitwise negation for integers ends up similar to mathematical negation
    because typically the signed integers use "two's complement"
    representation. In this represenation mathematical negation is achieved by
    aplying bitwise negation and adding ``1``.

    .. code-block:: edgeql-repl

        db> select bit_not(17);
        {-18}
        db> select -17 = bit_not(17) + 1;
        {true}


----------


.. eql:function:: std::bit_lshift(val: int16, n: int64) -> int16
                  std::bit_lshift(val: int32, n: int64) -> int32
                  std::bit_lshift(val: int64, n: int64) -> int64

    Returns the result of a bitwise left-shift operator for two integers.

    The integer of ``val`` is shifted by ``n`` bits to the left. The rightmost
    added bits are all ``0``. Shifting an integer by a number of bits larger
    than the bit size of the integer results in ``0``:

    .. code-block:: edgeql-repl

        db> select bit_lshift(123, 2);
        {492}
        db> select bit_lshift(123, 65);
        {0}

    It is possible to affect the sign bit by left-shifting an integer:

    .. code-block:: edgeql-repl

        db> select bit_lshift(123, 60);
        {-5764607523034234880}

    In general, left-shifting an integer in small increments produces the same
    result as shifting it in one step:

    .. code-block:: edgeql-repl

        db> select bit_lshift(bit_lshift(123, 1), 3);
        {1968}
        db> select bit_lshift(123, 4);
        {1968}

    An exception will be raised when attempting to shift by a negative number
    of bits:

    .. code-block:: edgeql-repl

        db> select bit_lshift(123, -2);
        edgedb error: InvalidValueError: bit_lshift(): cannot shift by
        negative amount


----------


.. eql:function:: std::bit_rshift(val: int16, n: int64) -> int16
                  std::bit_rshift(val: int32, n: int64) -> int32
                  std::bit_rshift(val: int64, n: int64) -> int64

    Returns the result of a bitwise right-shift operator for
    two integers.

    The integer of ``val`` is shifted by ``n`` bits to the right. In the
    arithmetic, right-shifting the sign is preserved. This means that the
    leftmost added bits are ``1`` or ``0`` depending on the sign bit.
    Shifting an integer by a number of bits larger than the bit size of the
    integer results in ``0`` for positive numbers and ``-1`` for negative
    numbers:

    .. code-block:: edgeql-repl

        db> select bit_rshift(123, 2);
        {30}
        db> select bit_rshift(123, 65);
        {0}
        db> select bit_rshift(-123, 2);
        {-31}
        db> select bit_rshift(-123, 65);
        {-1}

    In general, right-shifting an integer in small increments produces the same
    result as shifting it in one step:

    .. code-block:: edgeql-repl

        db> select bit_rshift(bit_rshift(123, 1), 3);
        {7}
        db> select bit_rshift(123, 4);
        {7}
        db> select bit_rshift(bit_rshift(-123, 1), 3);
        {-8}
        db> select bit_rshift(-123, 4);
        {-8}

    An exception will be raised when attempting to shift by a negative number
    of bits:

    .. code-block:: edgeql-repl

        db> select bit_rshift(123, -2);
        edgedb error: InvalidValueError: bit_rshift(): cannot shift by
        negative amount


------------


.. eql:function:: std::to_bigint(s: str, fmt: optional str={}) -> bigint

    :index: parse bigint

    Returns a :eql:type:`bigint` value from ``s`` with a possible format of
    ``fmt``:

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

    Returns a :eql:type:`decimal` value from ``s`` with possible format of
    ``fmt``:

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

    Returns an :eql:type:`int16` value from ``s`` with possible format of
    ``fmt``:

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_int32(s: str, fmt: optional str={}) -> int32

    :index: parse int32

    Returns an :eql:type:`int32` value from ``s`` with possible format of
    ``fmt``:

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_int64(s: str, fmt: optional str={}) -> int64

    :index: parse int64

    Returns an :eql:type:`int64` value from ``s`` with possible format of
    ``fmt``:

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_float32(s: str, fmt: optional str={}) -> float32

    :index: parse float32

    Returns a :eql:type:`float32` value from ``s`` with possible format of
    ``fmt``:

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.


------------


.. eql:function:: std::to_float64(s: str, fmt: optional str={}) -> float64

    :index: parse float64

    Returns a :eql:type:`float64` value from ``s`` with possible format of
    ``fmt``:

    For more details on formatting see :ref:`here
    <ref_std_converters_number_fmt>`.
