.. _ref_std_generic:

=======
Generic
=======

:edb-alt-title: Generic Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:op:`anytype = anytype <eq>`
      - :eql:op-desc:`eq`

    * - :eql:op:`anytype != anytype <neq>`
      - :eql:op-desc:`neq`

    * - :eql:op:`anytype ?= anytype <coaleq>`
      - :eql:op-desc:`coaleq`

    * - :eql:op:`anytype ?!= anytype <coalneq>`
      - :eql:op-desc:`coalneq`

    * - :eql:op:`anytype \< anytype <lt>`
      - :eql:op-desc:`lt`

    * - :eql:op:`anytype \> anytype <gt>`
      - :eql:op-desc:`gt`

    * - :eql:op:`anytype \<= anytype <lteq>`
      - :eql:op-desc:`lteq`

    * - :eql:op:`anytype \>= anytype <gteq>`
      - :eql:op-desc:`gteq`

    * - :eql:func:`len`
      - :eql:func-desc:`len`

    * - :eql:func:`contains`
      - :eql:func-desc:`contains`

    * - :eql:func:`find`
      - :eql:func-desc:`find`

-----------

.. note::

    In EdgeQL, any value can be compared to another as long as they are
    both the same type.

.. eql:operator:: eq: anytype = anytype -> bool

    Compares two values of :eql:type:`anytype` for equality.

    This reuslts in a :eql:type:`bool`:

    .. code-block:: edgeql-repl

        db> select 3 = 3.0;
        {true}
        db> select 3 = 3.14;
        {false}
        db> select [1, 2] = [1, 2];
        {true}
        db> select (1, 2) = (x := 1, y := 2);
        {true}
        db> select (x := 1, y := 2) = (a := 1, b := 2);
        {true}
        db> select 'hello' = 'world';
        {false}


----------


.. eql:operator:: neq: anytype != anytype -> bool

    Compares two values of :eql:type:`anytype` for inequality.

    This results in a :eql:type:`bool`:

    .. code-block:: edgeql-repl

        db> select 3 != 3.0;
        {false}
        db> select 3 != 3.14;
        {true}
        db> select [1, 2] != [2, 1];
        {false}
        db> select (1, 2) != (x := 1, y := 2);
        {false}
        db> select (x := 1, y := 2) != (a := 1, b := 2);
        {false}
        db> select 'hello' != 'world';
        {true}


----------


.. eql:operator:: coaleq: optional anytype ?= optional anytype -> bool

    Compares two potentially empty values of :eql:type:`anytype` for equality.

    This works the same as a regular :eql:op:`=<eq>` operator, but also allows
    comparing an empty ``{}`` set.  Two empty sets are considered equal.

    This results in a :eql:type:`bool`:

    .. code-block:: edgeql-repl

        db> select {1} ?= {1.0};
        {true}
        db> select {1} ?= <int64>{};
        {false}
        db> select <int64>{} ?= <int64>{};
        {true}


----------


.. eql:operator:: coalneq: optional anytype ?!= optional anytype -> bool

    Compares two potentially empty values for inequality.

    This works the same as a negated :eql:op:`\!= <neq>` operator, but also
    allows comparing an empty ``{}`` set. Two empty sets are considered equal.

    This results in a :eql:type:`bool`:

    .. code-block:: edgeql-repl

        db> select {2} ?!= {2};
        {false}
        db> select {1} ?!= <int64>{};
        {true}
        db> select <bool>{} ?!= <bool>{};
        {false}


----------


.. eql:operator:: lt: anytype < anytype -> bool

    Performs a less than value comparison with operands of
    :eql:type:`anytype`.

    This results in a :eql:type:`bool` of ``true`` if the left operand
    is less than the the right operand:

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


.. eql:operator:: gt: anytype > anytype -> bool

    Performs a greater than value comparison with operands of
    :eql:type:`anytype`.

    This results in a :eql:type:`bool` of ``true`` if the left operand
    is greater than the the right operand:

    .. code-block:: edgeql-repl

        db> select 1 > 2;
        {false}
        db> select 3 > 2;
        {true}
        db> select 'hello' > 'world';
        {false}
        db> select (1, 'hello') > (1, 'world');
        {false}


----------


.. eql:operator:: lteq: anytype <= anytype -> bool

    Performs a less than/equals comparison with operands of
    :eql:type:`anytype`.

    This results in a :eql:type:`bool` of ``true`` if the left operand
    is less than or the same value as the right operand:

    .. code-block:: edgeql-repl

        db> select 1 <= 2;
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


.. eql:operator:: gteq: anytype >= anytype -> bool

    Performs a greater than/equals comparison with operands of
    :eql:type:`anytype`.

    This results in a :eql:type:`bool` of ``true`` if the left operand
    is greater than or the same value as the right operand:

    .. code-block:: edgeql-repl

        db> select 1 >= 2;
        {false}
        db> select 2 >= 2;
        {true}
        db> select 3 >= 2;
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

    Returns the number of values within a type as its "length."

    This is a polymorphic function to determine the number of characters
    within a :eql:type:`str`, number of :eql:type:`bytes` or number of
    elements in an :eql:type:`array`:

    .. code-block:: edgeql-repl

        db> select len('foo');
        {3}

        db> select len(b'bar');
        {3}

        db> select len([2, 5, 7]);
        {3}


----------


.. eql:function:: std::contains(haystack: str, needle: str) -> bool
                  std::contains(haystack: bytes, needle: bytes) -> bool
                  std::contains(haystack: array<anytype>, needle: anytype) \
                  -> bool
                  std::contains(haystack: range<anypoint>, \
                                needle: range<anypoint>) \
                  -> std::bool
                  std::contains(haystack: range<anypoint>, \
                                needle: anypoint) \
                  -> std::bool

    :index: find strpos strstr position array

    A polymorphic function to test if the *haystack* contains the *needle*.

    When the *haystack* is :eql:type:`str` or :eql:type:`bytes`,
    return ``true`` if *needle* is contained as a subsequence in it
    and ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select contains('qwerty', 'we');
        {true}

        db> select contains(b'qwerty', b'42');
        {false}

    When the *haystack* is an :eql:type:`array`, return ``true`` if
    the array contains the specified element and ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select contains([2, 5, 7, 2, 100], 2);
        {true}

    When the *haystack* is a :ref:`range <ref_std_range>`, return ``true`` if
    it contains either the specified sub-range or element and ``false``
    otherwise.

    .. code-block:: edgeql-repl

        db> select contains(range(1, 10), range(2,5));
        {true}

        db> select contains(range(1, 10), range(2,15));
        {false}

        db> select contains(range(1, 10), 2);
        {true}

        db> select contains(range(1, 10), 10);
        {false}


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

        db> select find('qwerty', 'we');
        {1}

        db> select find(b'qwerty', b'42');
        {-1}

        db> select find([2, 5, 7, 2, 100], 2);
        {0}

        db> select find([2, 5, 7, 2, 100], 2, 1);
        {3}


