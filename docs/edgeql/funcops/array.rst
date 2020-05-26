.. _ref_eql_funcops_array:


=====
Array
=====

:edb-alt-title: Array Functions and Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`array[i] <ARRAYIDX>`
      - :eql:op-desc:`ARRAYIDX`

    * - :eql:op:`array[from:to] <ARRAYSLICE>`
      - :eql:op-desc:`ARRAYSLICE`

    * - :eql:op:`array ++ array <ARRAYPLUS>`
      - :eql:op-desc:`ARRAYPLUS`

    * - :eql:op:`array = array <EQ>`, :eql:op:`array \< array <LT>`, ...
      - Comparison operators.

    * - :eql:func:`len`
      - Return number of elements in the array.

    * - :eql:func:`contains`
      - Check if an element is in the array.

    * - :eql:func:`find`
      - Find the index of an element in the array.

    * - :eql:func:`array_join`
      - Render an array to a string.

    * - :eql:func:`array_agg`
      - :eql:func-desc:`array_agg`

    * - :eql:func:`array_get`
      - :eql:func-desc:`array_get`

    * - :eql:func:`array_unpack`
      - :eql:func-desc:`array_unpack`


----------


.. eql:operator:: ARRAYIDX: array<anytype> [ int64 ] -> anytype

    Array indexing.

    Example:

    .. code-block:: edgeql-repl

        db> SELECT [1, 2, 3][0];
        {1}
        db> SELECT [(x := 1, y := 1), (x := 2, y := 3.3)][1];
        {(x := 2, y := 3.3)}

    Negative indexing is supported:

    .. code-block:: edgeql-repl

        db> SELECT [1, 2, 3][-1];
        {3}

    Referencing a non-existent array element will result in an error:

    .. code-block:: edgeql-repl

        db> SELECT [1, 2, 3][4];
        InvalidValueError: array index 4 is out of bounds


----------


.. eql:operator:: ARRAYSLICE: array<anytype> [ int64 : int64 ] -> anytype

    Array slicing.

    An omitted lower bound defaults to zero, and an omitted upper
    bound defaults to the size of the array.

    The upper bound is non-inclusive.

    Examples:

    .. code-block:: edgeql-repl

        db> SELECT [1, 2, 3][0:2];
        {[1, 2]}
        db> SELECT [1, 2, 3][2:];
        {[3]}
        db> SELECT [1, 2, 3][:1];
        {[1]}
        db> SELECT [1, 2, 3][:-2];
        {[1]}

    Referencing an array slice beyond the array boundaries will result in
    an empty array (unlike the direct reference to a specific index):

    .. code-block:: edgeql-repl

        db> SELECT [1, 2, 3][1:20];
        {[2, 3]}
        db> SELECT [1, 2, 3][10:20];
        {[]}


---------


.. eql:operator:: ARRAYPLUS: array<anytype> ++ array<anytype> -> array<anytype>

    Array concatenation.

    .. code-block:: edgeql-repl

        db> SELECT [1, 2, 3] ++ [99, 98];
        {[1, 2, 3, 99, 98]}


----------


.. eql:function:: std::array_agg(s: SET OF anytype) -> array<anytype>

    :index: aggregate array set

    Return the array made from all of the input set elements.

    The ordering of the input set will be preserved if specified.

    .. code-block:: edgeql-repl

        db> SELECT array_agg({2, 3, 5});
        {[2, 3, 5]}

        db> SELECT array_agg(User.name ORDER BY User.name);
        {['Alice', 'Bob', 'Joe', 'Sam']}


----------


.. eql:function:: std::array_get(array: array<anytype>, \
                                 index: int64, \
                                 NAMED ONLY default: anytype = {} \
                                 ) -> OPTIONAL anytype

    :index: array access get

    Return the element of *array* at the specified *index*.

    If *index* is out of array bounds, the *default* or ``{}`` (empty set)
    is returned.

    This works the same as :eql:op:`array indexing operator <ARRAYIDX>`
    except that if the index is outside array boundaries an empty set
    of the array element type is returned instead of raising an exception.

    .. code-block:: edgeql-repl

        db> SELECT array_get([2, 3, 5], 1);
        {3}
        db> SELECT array_get([2, 3, 5], 100);
        {}
        db> SELECT array_get([2, 3, 5], 100, default := 42);
        {42}


----------


.. eql:function:: std::array_unpack(array: array<anytype>) -> SET OF anytype

    :index: set array unpack

    Return array elements as a set.

    .. note::

        The ordering of the returned set is not guaranteed.

    .. code-block:: edgeql-repl

        db> SELECT array_unpack([2, 3, 5]);
        {3, 2, 5}


----------


.. eql:function:: std::array_join(array: array<str>, delimiter: str) -> str

    :index: join array_to_string implode

    Render an array to a string.

    Join a string array into a single string using a specified *delimiter*:

    .. code-block:: edgeql-repl

        db> SELECT to_str(['one', 'two', 'three'], ', ');
        {'one, two, three'}
