.. _ref_std_generic:

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

    * - :eql:func:`len`
      - :eql:func-desc:`len`

    * - :eql:func:`contains`
      - :eql:func-desc:`contains`

    * - :eql:func:`find`
      - :eql:func-desc:`find`

-----------


.. eql:operator:: EQ: anytype = anytype -> bool

    Compare two values for equality.

    .. code-block:: edgeql-repl

        db> SELECT 3 = 3.0;
        {true}
        db> SELECT 3 = 3.14;
        {false}
        db> SELECT [1, 2] = [1, 2];
        {true}
        db> SELECT (1, 2) = (x := 1, y := 2);
        {true}
        db> SELECT (x := 1, y := 2) = (a := 1, b := 2);
        {true}
        db> SELECT 'hello' = 'world';
        {false}


----------


.. eql:operator:: NEQ: anytype != anytype -> bool

    Compare two values for inequality.

    .. code-block:: edgeql-repl


        db> SELECT 3 != 3.0;
        {false}
        db> SELECT 3 != 3.14;
        {true}
        db> SELECT [1, 2] != [2, 1];
        {false}
        db> SELECT (1, 2) != (x := 1, y := 2);
        {false}
        db> SELECT (x := 1, y := 2) != (a := 1, b := 2);
        {false}
        db> SELECT 'hello' != 'world';
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

    Works the same as regular :eql:op:`\!= <NEQ>`, but also allows
    comparing ``{}``.  Two ``{}`` are considered equal.

    .. code-block:: edgeql-repl

        db> SELECT {2} ?!= {2};
        {false}
        db> SELECT {1} ?!= <int64>{};
        {true}
        db> SELECT <bool>{} ?!= <bool>{};
        {false}


----------


.. eql:operator:: LT: anytype < anytype -> bool

    Less than operator.

    Return ``true`` if the value of the left expression is less than
    the value of the right expression. In EdgeQL any values can be
    compared to each other as long as they are of the same type:

    .. code-block:: edgeql-repl

        db> select 1 < 2;
        {true}
        db> select 2 < 2;
        {false}
        db> select 'hello' < 'world';
        {true}
        db> select (1, 'hello') < (1, 'world');
        {true}

----------


.. eql:operator:: GT: anytype > anytype -> bool

    Greater than operator.

    Return ``true`` if the value of the left expression is greater
    than the value of the right expression. In EdgeQL any values can be
    compared to each other as long as they are of the same type:

    .. code-block:: edgeql-repl

        db> SELECT 1 > 2;
        {false}
        db> SELECT 3 > 2;
        {true}
        db> select 'hello' > 'world';
        {false}
        db> select (1, 'hello') > (1, 'world');
        {false}


----------


.. eql:operator:: LTEQ: anytype <= anytype -> bool

    Less or equal operator.

    Return ``true`` if the value of the left expression is less than
    or equal to the value of the right expression. In EdgeQL any
    values can be compared to each other as long as they are of the
    same type:

    .. code-block:: edgeql-repl

        db> SELECT 1 <= 2;
        {true}
        db> select 2 <= 2;
        {true}
        db> select 3 <= 2;
        {false}
        db> select 'hello' <= 'world';
        {true}
        db> select (1, 'hello') <= (1, 'world');
        {true}


----------


.. eql:operator:: GTEQ: anytype >= anytype -> bool

    Greater or equal operator.

    Return ``true`` if the value of the left expression is greater
    than or equal to the value of the right expression. In EdgeQL any
    values can be compared to each other as long as they are of the
    same type:

    .. code-block:: edgeql-repl

        db> SELECT 1 >= 2;
        {false}
        db> SELECT 2 >= 2;
        {true}
        db> SELECT 3 >= 2;
        {true}
        db> select 'hello' >= 'world';
        {false}
        db> select (1, 'hello') >= (1, 'world');
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


