.. _ref_eql_operators_comparison:

==========
Comparison
==========

EdgeDB supports the following comparison operators:

.. eql:operator:: EQ: A = B

    :optype A: anytype
    :optype B: anytype
    :resulttype: bool

    Compare two values for equality.

    .. code-block:: edgeql-repl

        db> SELECT 3 = 3.0;
        {true}


.. eql:operator:: NEQ: A != B

    :optype A: anytype
    :optype B: anytype
    :resulttype: bool

    Compare two values for inequality.

    .. code-block:: edgeql-repl

        db> SELECT 3 != 3.14;
        {true}


.. eql:operator:: COALEQ: A ?= B

    :optype A: OPTIONAL anytype
    :optype B: OPTIONAL anytype
    :resulttype: bool

    Compare two values for equality.

    Works the same as regular :eql:op:`=<EQ>`, but also allows
    comparing ``{}``.  Two ``{}`` are considered equal.

    .. code-block:: edgeql-repl

        db> SELECT {1} ?= {1.0};
        {true}

    .. code-block:: edgeql-repl

        db> SELECT {1} ?= {};
        {false}

    .. code-block:: edgeql-repl

        db> SELECT <int64>{} ?= {};
        {true}


.. eql:operator:: COALNEQ: A ?!= B

    :optype A: OPTIONAL anytype
    :optype B: OPTIONAL anytype
    :resulttype: bool

    Compare two values for inequality.

    Works the same as regular :eql:op:`\!=<NEQ>`, but also allows
    comparing ``{}``.  Two ``{}`` are considered equal.

    .. code-block:: edgeql-repl

        db> SELECT {2} ?!= {2};
        {false}

    .. code-block:: edgeql-repl

        db> SELECT {1} ?!= {};
        {true}

    .. code-block:: edgeql-repl

        db> SELECT <int64>{} ?!= {};
        {false}


.. eql:operator:: LT: A < B

    :optype A: anytype
    :optype B: anytype
    :resulttype: bool

    ``TRUE`` if ``A`` is less than ``B``.

    .. code-block:: edgeql-repl

        db> SELECT 1 < 2;
        {true}


.. eql:operator:: GT: A > B

    :optype A: anytype
    :optype B: anytype
    :resulttype: bool

    ``TRUE`` if ``A`` is greater than ``B``.

    .. code-block:: edgeql-repl

        db> SELECT 1 > 2;
        {false}


.. eql:operator:: LTEQ: A <= B

    :optype A: anytype
    :optype B: anytype
    :resulttype: bool

    ``TRUE`` if ``A`` is less than or equal to ``B``.

    .. code-block:: edgeql-repl

        db> SELECT 1 <= 2;
        {true}


.. eql:operator:: GTEQ: A >= B

    :optype A: anytype
    :optype B: anytype
    :resulttype: bool

    ``TRUE`` if ``A`` is greater than or equal to ``B``.

    .. code-block:: edgeql-repl

        db> SELECT 1 >= 2;
        {false}


.. eql:operator:: EXISTS: EXISTS A

    :optype A: SET OF anytype
    :resulttype: bool

    Test whether a set is not empty.

    ``EXISTS`` is an aggregate operator that returns a singleton set
    ``{true}`` if the input set is not empty and returns ``{false}``
    otherwise.

    .. code-block:: edgeql-repl

        db> SELECT EXISTS {1, 2};
        {true}
