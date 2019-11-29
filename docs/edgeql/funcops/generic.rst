.. _ref_eql_functions_generic:

=======
Generic
=======

:edb-alt-title: Generic Functions and Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`anytype = anytype <EQ>`
      - :eql:op-desc:`EQ`

    * - :eql:op:`anytype != anytype <NEQ>`
      - :eql:op-desc:`NEQ`

    * - :eql:op:`anytype ?= anytype <COALEQ>`
      - :eql:op-desc:`COALEQ`

    * - :eql:op:`anytype ?!= anytype <COALNEQ>`
      - :eql:op-desc:`COALNEQ`

    * - :eql:op:`anytype \< anytype <LT>`
      - :eql:op-desc:`LT`

    * - :eql:op:`anytype \> anytype <GT>`
      - :eql:op-desc:`GT`

    * - :eql:op:`anytype \<= anytype <LTEQ>`
      - :eql:op-desc:`LTEQ`

    * - :eql:op:`anytype \>= anytype <GTEQ>`
      - :eql:op-desc:`GTEQ`

    * - :eql:op:`anytype IF bool ELSE anytype <IF..ELSE>`
      - :eql:op-desc:`IF..ELSE`

    * - :eql:func:`len`
      - :eql:func-desc:`len`

    * - :eql:func:`contains`
      - :eql:func-desc:`contains`

    * - :eql:func:`find`
      - :eql:func-desc:`find`

    * - :eql:func:`round`
      - :eql:func-desc:`round`

    * - :eql:func:`random`
      - :eql:func-desc:`random`


-----------


.. eql:operator:: IF..ELSE: anytype IF bool ELSE anytype -> anytype

    :index: if else ifelse elif ternary

    Conditionally provide one or the other result.

    .. eql:synopsis::

        <left_expr> IF <condition> ELSE <right_expr>

    If :eql:synopsis:`<condition>` is ``true``, then the value of the
    ``IF..ELSE`` expression is the value of :eql:synopsis:`<left_expr>`,
    if :eql:synopsis:`<condition>` is ``false``, the result is the value of
    :eql:synopsis:`<right_expr>`.

    .. code-block:: edgeql-repl

        db> SELECT 'hello' IF 2 * 2 = 4 ELSE 'bye';
        {'hello'}

    ``IF..ELSE`` expressions can be chained when checking multiple conditions
    is necessary:

    .. code-block:: edgeql-repl

        db> WITH color := 'yellow'
        ... SELECT 'Apple' IF color = 'red' ELSE
        ...        'Banana' IF color = 'yellow' ELSE
        ...        'Orange' IF color = 'orange' ELSE
        ...        'Other';
        {'Banana'}

-----------


.. eql:operator:: EQ: anytype = anytype -> bool

    Compare two values for equality.

    .. code-block:: edgeql-repl

        db> SELECT 3 = 3.0;
        {true}
        db> SELECT [1, 2] = [1, 2];
        {true}
        db> SELECT (x := 1, y := 2) = (x := 1, y := 2);
        {true}
        db> SELECT 'hello' = 'hello';
        {true}


----------


.. eql:operator:: NEQ: anytype != anytype -> bool

    Compare two values for inequality.

    .. code-block:: edgeql-repl

        db> SELECT 3 != 3.14;
        {true}


----------


.. eql:operator:: COALEQ: OPTIONAL anytype ?= OPTIONAL anytype -> bool

    Compare two (potentially empty) values for equality.

    Works the same as regular :eql:op:`=<EQ>`, but also allows
    comparing ``{}``.  Two ``{}`` are considered equal.

    .. code-block:: edgeql-repl

        db> SELECT {1} ?= {1.0};
        {true}
        db> SELECT {1} ?= <int64>{};
        {false}
        db> SELECT <int64>{} ?= <int64>{};
        {true}


----------


.. eql:operator:: COALNEQ: OPTIONAL anytype ?!= OPTIONAL anytype -> bool

    Compare two (potentially empty) values for inequality.

    Works the same as regular |neq|_, but also allows
    comparing ``{}``.  Two ``{}`` are considered equal.

    .. code-block:: edgeql-repl

        db> SELECT {2} ?!= {2};
        {false}

    .. code-block:: edgeql-repl

        db> SELECT {1} ?!= <int64>{};
        {true}

    .. code-block:: edgeql-repl

        db> SELECT <bool>{} ?!= <bool>{};
        {false}


----------


.. eql:operator:: LT: anytype < anytype -> bool

    Less than operator.

    Return ``true`` if the value of the left expression is less
    than the value of the right expression.

    .. code-block:: edgeql-repl

        db> SELECT 1 < 2;
        {true}
        db> SELECT 2 < 2;
        {false}

----------


.. eql:operator:: GT: anytype > anytype -> bool

    Greater than operator.

    Return ``true`` if the value of the left expression is greater
    than the value of the right expression.

    .. code-block:: edgeql-repl

        db> SELECT 1 > 2;
        {false}
        db> SELECT 3 > 2;
        {true}


----------


.. eql:operator:: LTEQ: anytype <= anytype -> bool

    Less or equal operator.

    Return ``true`` if the value of the left expression is less
    than or equal to the value of the right expression.

    .. code-block:: edgeql-repl

        db> SELECT 1 <= 2;
        {true}
        db> SELECT 'aaa' <= 'bbb';
        {true}


----------


.. eql:operator:: GTEQ: anytype >= anytype -> bool

    Greater or equal operator.

    Return ``true`` if the value of the left expression is greater
    than or equal to the value of the right expression.

    .. code-block:: edgeql-repl

        db> SELECT 1 >= 2;
        {false}


----------


.. eql:function:: std::len(value: str) -> int64
                  std::len(value: bytes) -> int64
                  std::len(value: array<anytype>) -> int64

    :index: length count array

    A polymorphic function to calculate a "length" of its first
    argument.

    Return the number of characters in a :eql:type:`str`, or the
    number of bytes in :eql:type:`bytes`, or the number of elements in
    an :eql:type:`array`.

    .. code-block:: edgeql-repl

        db> SELECT len('foo');
        {3}

        db> SELECT len(b'bar');
        {3}

        db> SELECT len([2, 5, 7]);
        {3}


----------


.. eql:function:: std::contains(haystack: str, needle: str) -> bool
                  std::contains(haystack: bytes, needle: bytes) -> bool
                  std::contains(haystack: array<anytype>, needle: anytype) \
                  -> bool

    :index: find strpos strstr position array

    A polymorphic function to test if a sequence contains a certain element.

    When the *haystack* is :eql:type:`str` or :eql:type:`bytes`,
    return ``true`` if *needle* is contained as a subsequence in it
    and ``false`` otherwise.

    When the *haystack* is an :eql:type:`array`, return ``true`` if
    the array contains the specified element and ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> SELECT contains('qwerty', 'we');
        {true}

        db> SELECT contains(b'qwerty', b'42');
        {false}

        db> SELECT contains([2, 5, 7, 2, 100], 2);
        {true}


----------


.. eql:function:: std::find(haystack: str, needle: str) -> int64
                  std::find(haystack: bytes, needle: bytes) -> int64
                  std::find(haystack: array<anytype>, needle: anytype, \
                            from_pos: int64=0) -> int64

    :index: find strpos strstr position array

    A polymorphic function to find index of an element in a sequence.

    When the *haystack* is :eql:type:`str` or :eql:type:`bytes`,
    return the index of the first occurrence of *needle* in it.

    When the *haystack* is an :eql:type:`array`, return the index of
    the first occurrence of the specific *needle* element. For
    :eql:type:`array` inputs it is also possible to provide an
    optional *from_pos* argument to specify the position from
    which to start the search.

    If the *needle* is not found, return ``-1``.

    .. code-block:: edgeql-repl

        db> SELECT find('qwerty', 'we');
        {1}

        db> SELECT find(b'qwerty', b'42');
        {-1}

        db> SELECT find([2, 5, 7, 2, 100], 2);
        {0}

        db> SELECT find([2, 5, 7, 2, 100], 2, 1);
        {3}


----------


.. eql:function:: std::round(value: int64) -> float64
                  std::round(value: float64) -> float64
                  std::round(value: decimal) -> decimal
                  std::round(value: decimal, d: int64) -> decimal

    Round to the nearest value.

    There's a difference in how ties (which way ``0.5`` is rounded)
    are handled depending on the type of the input *value*.

    :eql:type:`float64` tie is rounded to the nearest even number:

    .. code-block:: edgeql-repl

        db> SELECT round(1.2);
        {1}

        db> SELECT round(1.5);
        {2}

        db> SELECT round(2.5);
        {2}

    :eql:type:`decimal` tie is rounded away from 0:

    .. code-block:: edgeql-repl

        db> SELECT round(1.2n);
        {1n}

        db> SELECT round(1.5n);
        {2n}

        db> SELECT round(2.5n);
        {3n}

    Additionally, when rounding a :eql:type:`decimal` *value* an
    optional argument *d* can be provided to specify to what decimal
    point the *value* must to be rounded.

    .. code-block:: edgeql-repl

        db> SELECT round(163.278n, 2);
        {163.28n}

        db> SELECT round(163.278n, 1);
        {163.3n}

        db> SELECT round(163.278n, 0);
        {163n}

        db> SELECT round(163.278n, -1);
        {160n}

        db> SELECT round(163.278n, -2);
        {200n}


----------


.. eql:function:: std::random() -> float64

    Return a pseudo-random number in the range ``0.0 <= x < 1.0``.

    .. code-block:: edgeql-repl

        db> SELECT random();
        {0.62649393780157}

.. |neq| replace:: !=
.. _neq: #operator::NEQ
