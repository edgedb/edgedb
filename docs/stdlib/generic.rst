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

.. note::

    In EdgeQL, any value can be compared to another as long as their types
    are compatible.


-----------


.. eql:operator:: eq: anytype = anytype -> bool

    Compares two values for equality.

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

    .. warning::

        When either operand in an equality comparison is an empty set, the
        result will not be a ``bool`` but instead an empty set.

        .. code-block:: edgeql-repl

            db> select true = <bool>{};
            {}

        If one of the operands in an equality comparison could be an empty set,
        you may want to use the :eql:op:`coalescing equality <coaleq>` operator
        (``?=``) instead.

----------


.. eql:operator:: neq: anytype != anytype -> bool

    Compares two values for inequality.

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

    .. warning::

        When either operand in an inequality comparison is an empty set, the
        result will not be a ``bool`` but instead an empty set.

        .. code-block:: edgeql-repl

            db> select true != <bool>{};
            {}

        If one of the operands in an inequality comparison could be an empty
        set, you may want to use the :eql:op:`coalescing inequality <coaleq>`
        operator (``?!=``) instead.


----------


.. eql:operator:: coaleq: optional anytype ?= optional anytype -> bool

    Compares two (potentially empty) values for equality.

    This works the same as a regular :eql:op:`=<eq>` operator, but also allows
    comparing an empty ``{}`` set.  Two empty sets are considered equal.

    .. code-block:: edgeql-repl

        db> select {1} ?= {1.0};
        {true}
        db> select {1} ?= <int64>{};
        {false}
        db> select <int64>{} ?= <int64>{};
        {true}


----------


.. eql:operator:: coalneq: optional anytype ?!= optional anytype -> bool

    Compares two (potentially empty) values for inequality.

    This works the same as a regular :eql:op:`=<eq>` operator, but also allows
    comparing an empty ``{}`` set.  Two empty sets are considered equal.

    .. code-block:: edgeql-repl

        db> select {2} ?!= {2};
        {false}
        db> select {1} ?!= <int64>{};
        {true}
        db> select <bool>{} ?!= <bool>{};
        {false}


----------


.. eql:operator:: lt: anytype < anytype -> bool

    Less than operator.

    The operator returns ``true`` if the value of the left expression is less
    than the value of the right expression:

    .. code-block:: edgeql-repl

        db> select 1 < 2;
        {true}
        db> select 2 < 2;
        {false}
        db> select 'hello' < 'world';
        {true}
        db> select (1, 'hello') < (1, 'world');
        {true}

    .. warning::

        When either operand in a comparison is an empty set, the result will
        not be a ``bool`` but instead an empty set.

        .. code-block:: edgeql-repl

            db> select 1 < <int16>{};
            {}

        If one of the operands in a comparison could be an empty set, you may
        want to coalesce the result of the comparison with ``false`` to ensure
        your result is boolean.

        .. code-block:: edgeql-repl

            db> select (1 < <int16>{}) ?? false;
            {false}


----------


.. eql:operator:: gt: anytype > anytype -> bool

    Greater than operator.

    The operator returns ``true`` if the value of the left expression is
    greater than the value of the right expression:

    .. code-block:: edgeql-repl

        db> select 1 > 2;
        {false}
        db> select 3 > 2;
        {true}
        db> select 'hello' > 'world';
        {false}
        db> select (1, 'hello') > (1, 'world');
        {false}

    .. warning::

        When either operand in a comparison is an empty set, the result will
        not be a ``bool`` but instead an empty set.

        .. code-block:: edgeql-repl

            db> select 1 > <int16>{};
            {}

        If one of the operands in a comparison could be an empty set, you may
        want to coalesce the result of the comparison with ``false`` to ensure
        your result is boolean.

        .. code-block:: edgeql-repl

            db> select (1 > <int16>{}) ?? false;
            {false}


----------


.. eql:operator:: lteq: anytype <= anytype -> bool

    Less or equal operator.

    The operator returns ``true`` if the value of the left expression is less
    than or equal to the value of the right expression:

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

    .. warning::

        When either operand in a comparison is an empty set, the result will
        not be a ``bool`` but instead an empty set.

        .. code-block:: edgeql-repl

            db> select 1 <= <int16>{};
            {}

        If one of the operands in a comparison could be an empty set, you may
        want to coalesce the result of the comparison with ``false`` to ensure
        your result is boolean.

        .. code-block:: edgeql-repl

            db> select (1 <= <int16>{}) ?? false;
            {false}


----------


.. eql:operator:: gteq: anytype >= anytype -> bool

    Greater or equal operator.

    The operator returns ``true`` if the value of the left expression is
    greater than or equal to the value of the right expression:

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

    .. warning::

        When either operand in a comparison is an empty set, the result will
        not be a ``bool`` but instead an empty set.

        .. code-block:: edgeql-repl

            db> select 1 >= <int16>{};
            {}

        If one of the operands in a comparison could be an empty set, you may
        want to coalesce the result of the comparison with ``false`` to ensure
        your result is boolean.

        .. code-block:: edgeql-repl

            db> select (1 >= <int16>{}) ?? false;
            {false}


----------


.. eql:function:: std::len(value: str) -> int64
                  std::len(value: bytes) -> int64
                  std::len(value: array<anytype>) -> int64

    :index: length count array

    Returns the number of elements of a given value.

    This function works with the :eql:type:`str`, :eql:type:`bytes` and
    :eql:type:`array` types:

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
                  std::contains(haystack: multirange<anypoint>, \
                                needle: multirange<anypoint>) \
                  -> std::bool
                  std::contains(haystack: multirange<anypoint>, \
                                needle: range<anypoint>) \
                  -> std::bool
                  std::contains(haystack: multirange<anypoint>, \
                                needle: anypoint) \
                  -> std::bool

    :index: find strpos strstr position array

    Returns true if the given sub-value exists within the given value.

    When *haystack* is a :eql:type:`str` or a :eql:type:`bytes` value,
    this function will return ``true`` if it contains *needle* as a
    subsequence within it or ``false`` otherwise:

    .. code-block:: edgeql-repl

        db> select contains('qwerty', 'we');
        {true}

        db> select contains(b'qwerty', b'42');
        {false}

    When *haystack* is an :eql:type:`array`, the function will return
    ``true`` if the array contains the element specified as *needle* or
    ``false`` otherwise:

    .. code-block:: edgeql-repl

        db> select contains([2, 5, 7, 2, 100], 2);
        {true}

    When *haystack* is a :ref:`range <ref_std_range>`, the function will
    return ``true`` if it contains either the specified sub-range or element.
    The function will return ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select contains(range(1, 10), range(2, 5));
        {true}

        db> select contains(range(1, 10), range(2, 15));
        {false}

        db> select contains(range(1, 10), 2);
        {true}

        db> select contains(range(1, 10), 10);
        {false}

    When *haystack* is a :ref:`multirange <ref_std_multirange>`, the function
    will return ``true`` if it contains either the specified multirange,
    sub-range or element. The function will return ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select contains(
        ...   multirange([
        ...     range(1, 4), range(7),
        ...   ]),
        ...   multirange([
        ...     range(1, 2), range(8, 10),
        ...   ]),
        ... );
        {true}

        db> select contains(
        ...   multirange([
        ...     range(1, 4), range(8, 10),
        ...   ]),
        ...   range(8),
        ... );
        {false}

        db> select contains(
        ...   multirange([
        ...     range(1, 4), range(8, 10),
        ...   ]),
        ...   3,
        ... );
        {true}

    When *haystack* is :ref:`JSON <ref_std_json>`, the function will return
    ``true`` if the json data contains the element specified as *needle* or
    ``false`` otherwise:

    .. code-block:: edgeql-repl

        db> with haystack := to_json('{
        ...   "city": "Baerlon",
        ...   "city": "Caemlyn"
        ... }'),
        ... needle := to_json('{
        ...   "city": "Caemlyn"
        ... }'),
        ... select contains(haystack, needle);
        {true}


----------


.. eql:function:: std::find(haystack: str, needle: str) -> int64
                  std::find(haystack: bytes, needle: bytes) -> int64
                  std::find(haystack: array<anytype>, needle: anytype, \
                            from_pos: int64=0) -> int64

    :index: find strpos strstr position array

    Returns the index of a given sub-value in a given value.

    When *haystack* is a :eql:type:`str` or a :eql:type:`bytes` value, the
    function will return the index of the first occurrence of *needle* in it.

    When *haystack* is an :eql:type:`array`, this will return the index of the
    the first occurrence of the element passed as *needle*. For
    :eql:type:`array` inputs it is also possible to provide an optional
    *from_pos* argument to specify the position from which to start the
    search.

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
