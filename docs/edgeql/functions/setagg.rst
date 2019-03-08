.. _ref_eql_functions_setagg:

==========
Aggregates
==========

:index: set aggregate

.. eql:function:: std::count(s: SET OF anytype) -> int64

    :index: aggregate

    Return the number of elements in a set.

    .. code-block:: edgeql-repl

        db> SELECT count({2, 3, 5});
        {3}

        db> SELECT count(User);  # number of User objects in db
        {4}

.. eql:function:: std::sum(s: SET OF int32) -> int64
                  std::sum(s: SET OF int64) -> int64
                  std::sum(s: SET OF float32) -> float32
                  std::sum(s: SET OF float64) -> float64
                  std::sum(s: SET OF decimal) -> decimal

    :index: aggregate

    Return the sum of the set of numbers.

    The result type depends on the input set type. The general rule is
    that the type of the input set is preserved (as if a simple
    :eql:op:`+<PLUS>` was used) while trying to reduce the chance of
    an overflow (so all integers produce :eql:type:`int64` sum).

    .. code-block:: edgeql-repl

        db> SELECT sum({2, 3, 5});
        {10}

        db> SELECT sum({0.2, 0.3, 0.5});
        {1.0}

.. eql:function:: std::all(values: SET OF bool) -> bool

    :index: aggregate

    Generalized boolean :eql:op:`AND` applied to the set of *values*.

    The result is ``TRUE`` if all of the *values* are ``TRUE`` or the
    set of *values* is ``{}``. Return ``FALSE`` otherwise.

    .. code-block:: edgeql-repl

        db> SELECT all(<bool>{});
        {true}

        db> SELECT all({1, 2, 3, 4} < 4);
        {false}

.. eql:function:: std::any(values: SET OF bool) -> bool

    :index: aggregate

    Generalized boolean :eql:op:`OR` applied to the set of *values*.

    The result is ``TRUE`` if any of the *values* are ``TRUE``. Return
    ``FALSE`` otherwise.

    .. code-block:: edgeql-repl

        db> SELECT any(<bool>{});
        {false}

        db> SELECT any({1, 2, 3, 4} < 4);
        {true}

.. eql:function:: std::enumerate(values: SET OF anytype) -> \
                  SET OF tuple<int64, anytype>

    :index: enumerate

    Return a set of tuples of the form ``(index, element)``.

    The ``enumerate()`` function takes any set and produces a set of
    tuples containing the zero-based index number and the value for each
    element.

    .. note::

        The ordering of the returned set is not guaranteed, however
        the assigned indexes are guaranteed to be in order of the
        original set.

    .. code-block:: edgeql-repl

        db> SELECT enumerate({2, 3, 5});
        {(1, 3), (0, 2), (2, 5)}

    .. code-block:: edgeql-repl

        db> SELECT enumerate(User.name);
        {(0, 'Alice'), (1, 'Bob'), (2, 'Dave')}

.. eql:function:: std::min(values: SET OF anytype) -> OPTIONAL anytype

    :index: aggregate

    Return the smallest value of the input set.


.. eql:function:: std::max(values: SET OF anytype) -> OPTIONAL anytype

    :index: aggregate

    Return the greatest value of the input set.


Here's a list of aggregate functions covered in other sections:

* :eql:func:`array_agg`
* :eql:func:`math::mean`
* :eql:func:`math::stddev`
* :eql:func:`math::stddev_pop`
* :eql:func:`math::var`
* :eql:func:`math::var_pop`
