.. _ref_eql_operators_coalesce:


========
Coalesce
========

.. eql:operator:: COALESCE: A ?? B

    :optype A: OPTIONAL anytype
    :optype B: SET OF anytype
    :resulttype: SET OF anytype

    Evaluate to ``A`` for non-empty ``A``, otherwise evaluate to ``B``.

    A typical use case of coalescing operator is to provide default
    values for optional properties.

    .. code-block:: edgeql

        # Get a set of tuples (<issue name>, <priority>)
        # for all issues.
        SELECT (Issue.name, Issue.priority.name ?? 'n/a');

    Without the coalescing operator the above query would skip any
    ``Issue`` without priority.
