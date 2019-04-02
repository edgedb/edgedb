.. _ref_eql_functions_array:


=====
Array
=====

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

    Return the element of the *array* at the specified *index*.

    If the *index* is out of array bounds, the *default* or ``{}``
    is returned.

    This works the same as :ref:`array element referencing
    operator<ref_eql_expr_array_elref>` except that if the index is
    outside array boundaries an empty set of the array element type is
    returned instead of raising an exception.

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
