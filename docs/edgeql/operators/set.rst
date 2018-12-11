.. _ref_eql_operators_set:

===
Set
===

:index: set

This section describes EdgeQL operators that work on whole sets.


.. eql:operator:: DISTINCT: DISTINCT A

    :optype A: SET OF anytype
    :resulttype: anytype

    Return a set without repeating any elements.

    ``DISTINCT`` is a set operator that returns a new set where
    no member is equal to any other member.

    .. code-block:: edgeql-repl

        db> SELECT DISTINCT {1, 2, 2, 3};
        {1, 2, 3}


.. eql:operator:: IN: A IN B or A NOT IN B

    :optype A: anytype
    :optype B: SET OF anytype
    :resulttype: bool

    :index: in

    Test the membership of an element in a set.

    Set membership operators :eql:op:`IN` and :eql:op:`NOT IN<IN>`
    that test for each element of ``A`` whether it is present in ``B``.

    .. code-block:: edgeql-repl

        db> SELECT 1 IN {1, 3, 5};
        {True}

        db> SELECT 'Alice' IN User.name;
        {True}

        db> SELECT {1, 2} IN {1, 3, 5};
        {True, False}


.. eql:operator:: UNION: A UNION B

    :optype A: SET OF anytype
    :optype B: SET OF anytype
    :resulttype: SET OF anytype

    Merge two sets.

    Since EdgeDB sets are formally multisets, ``UNION`` is a *multiset sum*,
    so effectively it merges two multisets keeping all of their members.

    For example, applying ``UNION`` to ``{1, 2, 2}`` and
    ``{2}``, results in ``{1, 2, 2, 2}``.

    If you need a distinct union, wrap it with :eql:op:`DISTINCT`.
