.. _ref_std_array:

=====
Array
=====

:edb-alt-title: Array Functions and Operators

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

An empty array can also be created, but it must be used together with
a type cast, since EdgeDB cannot infer the type of an array that contains no
elements.

.. code-block:: edgeql-repl

    db> select [];
    QueryError: expression returns value of indeterminate type
    Hint: Consider using an explicit type cast.
    ### select [];
    ###        ^

    db> select <array<int64>>[];
    {[]}

Functions and operators
^^^^^^^^^^^^^^^^^^^^^^^

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



Reference
^^^^^^^^^

.. eql:type:: std::array

    :index: array

    Arrays represent a one-dimensional homogeneous ordered list.

    Array indexing starts at zero.

    With the exception of other array types, any type can be used as an
    array element type.

    An array type is created implicitly when an :ref:`array
    constructor <ref_std_array_constructor>` is used:

    .. code-block:: edgeql-repl

        db> select [1, 2];
        {[1, 2]}

    The syntax of an array type declaration can be found in :ref:`this
    section <ref_datamodel_arrays>`.

    See also the list of standard
    :ref:`array functions <ref_std_array>` and
    generic functions such as :eql:func:`len`.



----------


.. eql:operator:: arrayidx: array<anytype> [ int64 ] -> anytype

    Array indexing.

    Example:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0];
        {1}
        db> select [(x := 1, y := 1), (x := 2, y := 3.3)][1];
        {(x := 2, y := 3.3)}

    Negative indexing is supported:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][-1];
        {3}

    Referencing a non-existent array element will result in an error:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][4];
        InvalidValueError: array index 4 is out of bounds


----------


.. eql:operator:: arrayslice: array<anytype> [ int64 : int64 ] -> anytype

    Array slicing.

    An omitted lower bound defaults to zero, and an omitted upper
    bound defaults to the size of the array.

    The upper bound is non-inclusive.

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

    Referencing an array slice beyond the array boundaries will result in
    an empty array (unlike a direct reference to a specific index):

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][1:20];
        {[2, 3]}
        db> select [1, 2, 3][10:20];
        {[]}


---------


.. eql:operator:: arrayplus: array<anytype> ++ array<anytype> -> array<anytype>

    Array concatenation.

    .. code-block:: edgeql-repl

        db> select [1, 2, 3] ++ [99, 98];
        {[1, 2, 3, 99, 98]}


----------


.. eql:function:: std::array_agg(s: set of anytype) -> array<anytype>

    :index: aggregate array set

    Return an array made from all of the input set elements.

    The ordering of the input set will be preserved if specified.

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

    Return the element of *array* at the specified *index*.

    If *index* is out of array bounds, the *default* or ``{}`` (empty set)
    is returned.

    This works the same as :eql:op:`array indexing operator <arrayidx>`
    except that if the index is outside array boundaries an empty set
    of the array element type is returned instead of raising an exception.

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

    Return array elements as a set.

    .. note::

        The ordering of the returned set is not guaranteed.

    .. code-block:: edgeql-repl

        db> select array_unpack([2, 3, 5]);
        {3, 2, 5}


----------


.. eql:function:: std::array_join(array: array<str>, delimiter: str) -> str

    :index: join array_to_string implode

    Render an array to a string.

    Join a string array into a single string using a specified *delimiter*:

    .. code-block:: edgeql-repl

        db> select to_str(['one', 'two', 'three'], ', ');
        {'one, two, three'}
