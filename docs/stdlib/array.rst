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
      - Returns a number of elements in the array.

    * - :eql:func:`contains`
      - Checks if an element is in the array.

    * - :eql:func:`find`
      - Finds the index of an element in the array.

    * - :eql:func:`array_join`
      - Renders an array in string-form.

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

Array constructors are an expression that allow for a sequence of
comma-separated expressions *of the same type* enclosed in square brackets.
This will produce an array of the following:

.. eql:synopsis::

    "[" <expr> [, ...] "]"

You can then use these arrays in EdgeDB to access information.

.. code-block:: edgeql-repl

    db> select [1, 2, 3];
    {[1, 2, 3]}
    db> select [('a', 1), ('b', 2), ('c', 3)];
    {[('a', 1), ('b', 2), ('c', 3)]}

Empty arrays
^^^^^^^^^^^^

An empty array can also be created, but it must be used with
a type cast as EdgeDB cannot infer the type of an array with no elements:

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

    Represents a one-dimensional array of an homogeneous ordered list.

    Array indexing will always start at zero. With the exception of other
    array types, any type may be used as the given element contained within.

    Array types are implicitly created from :ref:`array
    constructors <ref_std_array_constructor>` as seen here:

    .. code-block:: edgeql-repl

        db> select [1, 2];
        {[1, 2]}

    The syntax of an array type's declaration can be found from :ref:`this
    section <ref_datamodel_arrays>`.

    Please see also the list of standard :ref:`array
    functions <ref_std_array>`, as well as generic functions such as
    :eql:func:`len`.



----------


.. eql:operator:: arrayidx: array<anytype> [ int64 ] -> anytype

    Indexes an array of :eql:type:`anytype` containing :eql:type:`int64`.

    This results in a representable reference of the array element specified.

    Below is an example of selecting an array with an index of zero and
    one:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0];
        {1}
        db> select [(x := 1, y := 1), (x := 2, y := 3.3)][1];
        {(x := 2, y := 3.3)}

    This operator may also be used when negatively index elements.

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][-1];
        {3}

    However, referencing a non-existent element of an array will result in
    an error:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][4];
        InvalidValueError: array index 4 is out of bounds


----------


.. eql:operator:: arrayslice: array<anytype> [ int64 : int64 ] -> anytype

    Slices an array of :eql:type:`anytype` containing :eql:type:`int64`.

    This results in a representable reference of the array's elements.

    Omitting the lower bound an array will default the result to zero.
    Doing so to the upper bound will also default to the current size of the
    array.

    The upper bound of an array is non-inclusive.

    Below is an example of selecting indices of an array between slices and
    given ranges:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0:2];
        {[1, 2]}
        db> select [1, 2, 3][2:];
        {[3]}
        db> select [1, 2, 3][:1];
        {[1]}
        db> select [1, 2, 3][:-2];
        {[1]}

    Referencing an array slice beyond the array's boundaries will result in
    an empty array, unlike a direct reference to a specific index:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][1:20];
        {[2, 3]}
        db> select [1, 2, 3][10:20];
        {[]}


---------


.. eql:operator:: arrayplus: array<anytype> ++ array<anytype> -> array<anytype>

    Concatenates given arrays of :eql:type:`anytype` into one.

    This results in a reference of both array's elements conjoined together:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3] ++ [99, 98];
        {[1, 2, 3, 99, 98]}


----------


.. eql:function:: std::array_agg(s: set of anytype) -> array<anytype>

    :index: aggregate array set

    Returns an array made from all of the input set elements.

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

    Returns the element of a given ``array`` at the specified ``index``.

    If the index is out of the array's bounds, the ``default`` argument
    or ``{}`` (empty set) will be returned.

    This works the same as :eql:op:`array indexing operator <arrayidx>`,
    except that if the index is out of boundaries, an empty set of the array
    element's type is returned instead of raising an exception. Below
    exemplifies this:

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

        The returned set is not guaranteed to be ordered.

    .. code-block:: edgeql-repl

        db> select array_unpack([2, 3, 5]);
        {3, 2, 5}


----------


.. eql:function:: std::array_join(array: array<str>, delimiter: str) -> str

    :index: join array_to_string implode

    Returns the elements of an array joined together in string-form.

    This subsequently returns back a string with ``delimiter`` separating
    each element:

    .. code-block:: edgeql-repl

        db> select to_str(['one', 'two', 'three'], ', ');
        {'one, two, three'}


----------


.. eql:function:: std::array_fill(val: anytype, n: int64) -> array<anytype>

    :index: fill

    Returns a new array of size ``n`` with the specified value ``val``:

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

    Returns an array with all occurrences of ``old`` replaced by ``new``:

    .. code-block:: edgeql-repl

        db> select array_replace([1, 1, 2, 3, 5], 1, 99);
        {[99, 99, 2, 3, 5]}
        db> select array_replace(['h', 'e', 'l', 'l', 'o'], 'l', 'L');
        {['h', 'e', 'L', 'L', 'o']}
