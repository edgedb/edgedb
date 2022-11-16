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

Arrays are constructed by placing multiple comma-separated expressions within
square brackets. Here are a few examples:

.. eql:synopsis::

    "[" <expr> [, ...] "]"


.. code-block:: edgeql-repl

    db> select [1, 2, 3];
    {[1, 2, 3]}
    db> select [('a', 1), ('b', 2), ('c', 3)];
    {[('a', 1), ('b', 2), ('c', 3)]}

Empty arrays
^^^^^^^^^^^^

You can also create an empty array, but it must be done by providing the type
information using type casting. EdgeDB cannot infer the type of an empty array
created otherwise. Example:

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

    Represents an ordered list of values of the same type.

    Array indexing will always start at zero.

    An array can contain any type except another array. In EdgeDB, arrays are
    always one-dimensional.

    Array types are implicitly created from :ref:`array
    constructors <ref_std_array_constructor>` as seen here:

    .. code-block:: edgeql-repl

        db> select [1, 2];
        {[1, 2]}

    The declaration of an array type will follow this syntax:

    .. code-block:: edgeql

        type Person {
            property str_array -> array<str>;
            property json_array -> array<json>;
        }

    Please see also the list of standard :ref:`array
    functions <ref_std_array>`, as well as :ref:`generic functions
    <ref_std_generic>` such as :eql:func:`len`.


----------


.. eql:operator:: arrayidx: array<anytype> [ int64 ] -> anytype

    Indexes an array of :eql:type:`anytype`:

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0];
        {1}
        db> select [(x := 1, y := 1), (x := 2, y := 3.3)][1];
        {(x := 2, y := 3.3)}

    This operator also allows accessing elements from the end of the array
    using a negative index.

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

    Produces a sub-array from an existing array.

    This results in a representable reference of the array's elements.

    Omitting the lower bound of an array slice will default to a low bound of
    zero.
    Omitting the upper bound will default the upper bound to the length of the
    array.
    
    The lower bound of an array slice is inclusive while the upper bound is
    not.

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][0:2];
        {[1, 2]}
        db> select [1, 2, 3][2:];
        {[3]}
        db> select [1, 2, 3][:1];
        {[1]}
        db> select [1, 2, 3][:-2];
        {[1]}

    If your array slice boundaries do not include any valid index from the
    array, the slice will produce an empty array. Slicing with a lower bound
    less than the minimum index or a upper bound greater than the
    maximum index are functionally equivalent to not specifying those bounds
    for your slice.

    .. code-block:: edgeql-repl

        db> select [1, 2, 3][1:20];
        {[2, 3]}
        db> select [1, 2, 3][10:20];
        {[]}


---------


.. eql:operator:: arrayplus: array<anytype> ++ array<anytype> -> array<anytype>

    Concatenates two arrays of the same type into one.

    This results in an array containing the elements of both of the
    concatenated arrays:

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

    Returns the element of a given ``array`` at the specified ``index``.

    If the index is out of the array's bounds, the ``default`` argument
    or ``{}`` (empty set) will be returned.

    This works the same as the :eql:op:`array indexing operator <arrayidx>`,
    except that if the index is out of bounds, an empty set of the array
    element's type is returned instead of raising an exception:

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

    The returned set is not guaranteed to be ordered:

    .. code-block:: edgeql-repl

        db> select array_unpack([2, 3, 5]);
        {3, 2, 5}


----------


.. eql:function:: std::array_join(array: array<str>, delimiter: str) -> str

    :index: join array_to_string implode

    Returns the elements of an *array* joined together with a *delimiter*:

    .. code-block:: edgeql-repl

        db> select to_str(['one', 'two', 'three'], ', ');
        {'one, two, three'}


----------


.. eql:function:: std::array_fill(val: anytype, n: int64) -> array<anytype>

    :index: fill

    Returns *array* elements as a string joined by *delimiter*.
    
    The new array will have *n* copies of the value passed for *val*:

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
