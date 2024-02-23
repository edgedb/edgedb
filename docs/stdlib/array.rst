.. _ref_std_array:

======
Arrays
======

:edb-alt-title: Array Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:op:`array[i] <arrayidx>`
      - :eql:op-desc:`arrayidx`

    * - :eql:op:`array[from:to] <arrayslice>`
      - :eql:op-desc:`arrayslice`

    * - :eql:op:`array ++ array <arrayplus>`
      - :eql:op-desc:`arrayplus`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`len`
      - Returns the number of elements in the array.

    * - :eql:func:`contains`
      - Checks if an element is in the array.

    * - :eql:func:`find`
      - Finds the index of an element in the array.

    * - :eql:func:`array_join`
      - Renders an array to a string or byte-string.

    * - :eql:func:`array_fill`
      - :eql:func-desc:`array_fill`

    * - :eql:func:`array_replace`
      - :eql:func-desc:`array_replace`

    * - :eql:func:`array_agg`
      - :eql:func-desc:`array_agg`

    * - :eql:func:`array_get`
      - :eql:func-desc:`array_get`

    * - :eql:func:`array_unpack`
      - :eql:func-desc:`array_unpack`

Arrays store expressions of the *same type* in an ordered list.

.. _ref_std_array_constructor:

Constructing arrays
^^^^^^^^^^^^^^^^^^^

An array constructor is an expression that consists of a sequence of
comma-separated expressions *of the same type* enclosed in square brackets.
It produces an array value:

.. eql:synopsis::

    "[" <expr> [, ...] "]"

For example:

.. code-block:: edgeql-repl

    db> select [1, 2, 3];
    {[1, 2, 3]}
    db> select [('a', 1), ('b', 2), ('c', 3)];
    {[('a', 1), ('b', 2), ('c', 3)]}

Empty arrays
^^^^^^^^^^^^

You can also create an empty array, but it must be done by providing the type
information using type casting. EdgeDB cannot infer the type of an empty array
created otherwise. For example:

.. code-block:: edgeql-repl

    db> select [];
    QueryError: expression returns value of indeterminate type
    Hint: Consider using an explicit type cast.
    ### select [];
    ###        ^

    db> select <array<int64>>[];
    {[]}



Reference
^^^^^^^^^

.. eql:type:: std::array

    :index: array

    An ordered list of values of the same type.

    Array indexing starts at zero.

    An array can contain any type except another array. In EdgeDB, arrays are
    always one-dimensional.

    An array type is created implicitly when an :ref:`array
    constructor <ref_std_array_constructor>` is used:

    .. code-block:: edgeql-repl

        db> select [1, 2];
        {[1, 2]}

    The array types themselves are denoted by ``array`` followed by their
    sub-type in angle brackets. These may appear in cast operations:

    .. code-block:: edgeql-repl

        db> select <array<str>>[1, 4, 7];
        {['1', '4', '7']}
        db> select <array<bigint>>[1, 4, 7];
        {[1n, 4n, 7n]}

    Array types may also appear in schema declarations:

    .. code-block:: sdl
        :version-lt: 3.0

        type Person {
            property str_array -> array<str>;
            property json_array -> array<json>;
        }

    .. code-block:: sdl

        type Person {
            str_array: array<str>;
            json_array: array<json>;
        }

    See also the list of standard :ref:`array functions <ref_std_array>`, as
    well as :ref:`generic functions <ref_std_generic>` such as
    :eql:func:`len`.


----------


.. eql:operator:: arrayidx: array<anytype> [ int64 ] -> anytype

    Accesses the array element at a given index.

    Example:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0];
        {1}
        db> select [(x := 1, y := 1), (x := 2, y := 3.3)][1];
        {(x := 2, y := 3.3)}

    This operator also allows accessing elements from the end of the array
    using a negative index:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][-1];
        {3}

    Referencing a non-existent array element will result in an error:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][4];
        InvalidValueError: array index 4 is out of bounds


----------


.. eql:operator:: arrayslice: array<anytype> [ int64 : int64 ] -> anytype

    Produces a sub-array from an existing array.

    Omitting the lower bound of an array slice will default to a lower bound
    of zero.

    Omitting the upper bound will default the upper bound to the length of the
    array.

    The lower bound of an array slice is inclusive while the upper bound is
    not.

    Examples:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0:2];
        {[1, 2]}
        db> select [1, 2, 3][2:];
        {[3]}
        db> select [1, 2, 3][:1];
        {[1]}
        db> select [1, 2, 3][:-2];
        {[1]}

    Referencing an array slice beyond the array boundaries will result in an
    empty array (unlike a direct reference to a specific index). Slicing with
    a lower bound less than the minimum index or a upper bound greater than
    the maximum index are functionally equivalent to not specifying those
    bounds for your slice:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][1:20];
        {[2, 3]}
        db> select [1, 2, 3][10:20];
        {[]}


---------


.. eql:operator:: arrayplus: array<anytype> ++ array<anytype> -> array<anytype>

    Concatenates two arrays of the same type into one.

    .. code-block:: edgeql-repl

        db> select [1, 2, 3] ++ [99, 98];
        {[1, 2, 3, 99, 98]}


----------


.. eql:function:: std::array_agg(s: set of anytype) -> array<anytype>

    :index: aggregate array set

    Returns an array made from all of the input set elements.

    The ordering of the input set will be preserved if specified:

    .. code-block:: edgeql-repl

        db> select array_agg({2, 3, 5});
        {[2, 3, 5]}

        db> select array_agg(User.name order by User.name);
        {['Alice', 'Bob', 'Joe', 'Sam']}


----------


.. eql:function:: std::array_get(array: array<anytype>, \
                                 index: int64, \
                                 named only default: anytype = {} \
                              ) -> optional anytype

    :index: array access get

    Returns the element of a given *array* at the specified *index*.

    If the index is out of the array's bounds, the *default* argument or
    ``{}`` (empty set) will be returned.

    This works the same as the :eql:op:`array indexing operator <arrayidx>`,
    except that if the index is out of bounds, an empty set
    of the array element's type is returned instead of raising an exception:

    .. code-block:: edgeql-repl

        db> select array_get([2, 3, 5], 1);
        {3}
        db> select array_get([2, 3, 5], 100);
        {}
        db> select array_get([2, 3, 5], 100, default := 42);
        {42}


----------


.. eql:function:: std::array_unpack(array: array<anytype>) -> set of anytype

    :index: set array unpack

    Returns the elements of an array as a set.

    .. note::

        The ordering of the returned set is not guaranteed.
        However, if it is wrapped in a call to :eql:func:`enumerate`,
        the assigned indexes are guaranteed to match the array.

    .. code-block:: edgeql-repl

        db> select array_unpack([2, 3, 5]);
        {3, 2, 5}

        db> select enumerate(array_unpack([2, 3, 5]));
        {(1, 3), (0, 2), (2, 5)}


----------


.. eql:function:: std::array_join(array: array<str>, delimiter: str) -> str
                  std::array_join(array: array<bytes>, \
                                  delimiter: bytes) -> bytes

    :index: join array_to_string implode

    Renders an array to a string or byte-string.

    Join a string array into a single string using a specified *delimiter*:

    .. code-block:: edgeql-repl

        db> select array_join(['one', 'two', 'three'], ', ');
        {'one, two, three'}

    Similarly, an array of :eql:type:`bytes` can be joined as a single value
    using a specified *delimiter*:

    .. code-block:: edgeql-repl

        db> select array_join([b'\x01', b'\x02', b'\x03'], b'\xff');
        {b'\x01\xff\x02\xff\x03'}


----------


.. eql:function:: std::array_fill(val: anytype, n: int64) -> array<anytype>

    :index: fill

    Returns an array of the specified size, filled with the provided value.

    Create an array of size *n* where every element has the value *val*.

    .. code-block:: edgeql-repl

        db> select array_fill(0, 5);
        {[0, 0, 0, 0, 0]}
        db> select array_fill('n/a', 3);
        {['n/a', 'n/a', 'n/a']}


----------


.. eql:function:: std::array_replace(array: array<anytype>, \
                                     old: anytype, \
                                     new: anytype) \
                  -> array<anytype>

    Returns an array with all occurrences of one value replaced by another.

    Return an array where every *old* value is replaced with *new*.

    .. code-block:: edgeql-repl

        db> select array_replace([1, 1, 2, 3, 5], 1, 99);
        {[99, 99, 2, 3, 5]}
        db> select array_replace(['h', 'e', 'l', 'l', 'o'], 'l', 'L');
        {['h', 'e', 'L', 'L', 'o']}
