.. _ref_eql_funcops_numeric:

========
Numerics
========

:edb-alt-title: Arithmetic Operators and Numeric Converter Functions


.. list-table::
    :class: funcoptable

    * - :eql:op:`anyreal + anyreal <PLUS>`
      - :eql:op-desc:`PLUS`

    * - :eql:op:`anyreal - anyreal <MINUS>`
      - :eql:op-desc:`MINUS`

    * - :eql:op:`-anyreal <UMINUS>`
      - :eql:op-desc:`UMINUS`

    * - :eql:op:`anyreal * anyreal <MULT>`
      - :eql:op-desc:`MULT`

    * - :eql:op:`anyreal / anyreal <DIV>`
      - :eql:op-desc:`DIV`

    * - :eql:op:`anyreal // anyreal <FLOORDIV>`
      - :eql:op-desc:`FLOORDIV`

    * - :eql:op:`anyreal % anyreal <MOD>`
      - :eql:op-desc:`MOD`

    * - :eql:op:`anyreal ^ anyreal <POW>`
      - :eql:op-desc:`POW`

    * - :eql:op:`anyreal = anyreal <EQ>`,
        :eql:op:`anyreal \< anyreal <LT>`, ...
      - Comparison operators.

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


----------


.. eql:operator:: PLUS: anyreal + anyreal -> anyreal

    :index: plus add

    Arithmetic addition.

    .. code-block:: edgeql-repl

        db> SELECT 2 + 2;
        {4}


----------


.. eql:operator:: MINUS: anyreal - anyreal -> anyreal

    :index: minus subtract

    Arithmetic subtraction.

    .. code-block:: edgeql-repl

        db> SELECT 3 - 2;
        {1}


----------


.. eql:operator:: UMINUS: - anyreal -> anyreal

    :index: unary minus subtract

    Arithmetic negation.

    .. code-block:: edgeql-repl

        db> SELECT -5;
        {-5}


----------


.. eql:operator:: MULT: anyreal * anyreal -> anyreal

    :index: multiply multiplication

    Arithmetic multiplication.

    .. code-block:: edgeql-repl

        db> SELECT 2 * 10;
        {20}


----------


.. eql:operator:: DIV: anyreal / anyreal -> anyreal

    :index: divide division

    Arithmetic division.

    .. code-block:: edgeql-repl

        db> SELECT 10 / 4;
        {2.5}

    Division by zero results in an error:

    .. code-block:: edgeql-repl

        db> SELECT 10 / 0;
        DivisionByZeroError: division by zero


----------


.. eql:operator:: FLOORDIV: anyreal // anyreal -> anyreal

    :index: floor divide division

    Floor division.

    The result is rounded down to the nearest integer. It is
    equivalent to using regular division and the applying
    :eql:func:`math::floor` to the result.

    .. code-block:: edgeql-repl

        db> SELECT 10 // 4;
        {2}
        db> SELECT math::floor(10 / 4);
        {2}
        db> SELECT -10 // 4;
        {-3}

    It also works on :eql:type:`float <anyfloat>`, :eql:type:`bigint`, and
    :eql:type:`decimal` types. The type of the result corresponds to
    the type of the operands:

    .. code-block:: edgeql-repl

        db> SELECT 3.7 // 1.1;
        {3.0}
        db> SELECT 3.7n // 1.1n;
        {3.0n}
        db> SELECT 37 // 11;
        {3}

    Regular division, floor division, and :eql:op:`%<MOD>` are
    related in the following way: ``A // B  =  (A - (A % B)) / B``.


----------


.. eql:operator:: MOD: anyreal % anyreal -> anyreal

    :index: modulo mod division

    Remainder from division (modulo).

    This is the remainder from floor division. Just as is
    the case with :eql:op:`//<FLOORDIV>` the result type of the
    remainder operator corresponds to the operand type:

    .. code-block:: edgeql-repl

        db> SELECT 10 % 4;
        {2}
        db> SELECT 10n % 4;
        {2n}
        db> SELECT -10 % 4;
        {2}
        db> # floating arithmetic is inexact, so
        ... # we get 0.3999999999999999 instead of 0.4
        ... SELECT 3.7 % 1.1;
        {0.3999999999999999}
        db> SELECT 3.7n % 1.1n;
        {0.4n}
        db> SELECT 37 % 11;
        {4}

    Regular division, :eql:op:`//<FLOORDIV>` and :eql:op:`%<MOD>` are
    related in the following way: ``A // B  =  (A - (A % B)) / B``.

    Modulo division by zero results in an error:

    .. code-block:: edgeql-repl

        db> SELECT 10 % 0;
        DivisionByZeroError: division by zero


-----------


.. eql:operator:: POW: anyreal ^ anyreal -> anyreal

    :index: power pow

    Power operation.

    .. code-block:: edgeql-repl

        db> SELECT 2 ^ 4;
        {16}


------------


.. eql:function:: std::to_bigint(s: str, fmt: OPTIONAL str={}) -> bigint

    :index: parse bigint

    Create a :eql:type:`bigint` value.

    Parse a :eql:type:`bigint` from the input *s* and optional format
    specification *fmt*.

    .. code-block:: edgeql-repl

        db> SELECT to_bigint('-000,012,345', 'S099,999,999,999');
        {-12345n}
        db> SELECT to_bigint('31st', '999th');
        {31n}

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------



.. eql:function:: std::to_decimal(s: str, fmt: OPTIONAL str={}) -> decimal

    :index: parse decimal

    Create a :eql:type:`decimal` value.

    Parse a :eql:type:`decimal` from the input *s* and optional format
    specification *fmt*.

    .. code-block:: edgeql-repl

        db> SELECT to_decimal('-000,012,345', 'S099,999,999,999');
        {-12345.0n}
        db> SELECT to_decimal('-012.345');
        {-12.345n}
        db> SELECT to_decimal('31st', '999th');
        {31.0n}

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_int16(s: str, fmt: OPTIONAL str={}) -> int16

    :index: parse int16

    Create a :eql:type:`int16` value.

    Parse a :eql:type:`int16` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_int32(s: str, fmt: OPTIONAL str={}) -> int32

    :index: parse int32

    Create a :eql:type:`int32` value.

    Parse a :eql:type:`int32` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_int64(s: str, fmt: OPTIONAL str={}) -> int64

    :index: parse int64

    Create a :eql:type:`int64` value.

    Parse a :eql:type:`int64` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_float32(s: str, fmt: OPTIONAL str={}) -> float32

    :index: parse float32

    Create a :eql:type:`float32` value.

    Parse a :eql:type:`float32` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_float64(s: str, fmt: OPTIONAL str={}) -> float64

    :index: parse float64

    Create a :eql:type:`float64` value.

    Parse a :eql:type:`float64` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.
