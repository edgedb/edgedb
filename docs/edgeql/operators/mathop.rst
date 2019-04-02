.. _ref_eql_operators_math:

==========
Arithmetic
==========

This section describes arithmetic operators
provided by EdgeDB.


Numerical
=========

.. eql:operator:: PLUS: A + B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: plus add

    Arithmetic addition.

    .. code-block:: edgeql-repl

        db> SELECT 2 + 2;
        {4}


.. eql:operator:: MINUS: A - B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: minus subtract

    Arithmetic subtraction.

    .. code-block:: edgeql-repl

        db> SELECT 3 - 2;
        {1}


.. eql:operator:: UMINUS: -A

    :optype A: anyreal
    :resulttype: anyreal
    :index: unary minus subtract

    Arithmetic negation.

    .. code-block:: edgeql-repl

        db> SELECT -5;
        {-5}


.. eql:operator:: MULT: A * B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: multiply multiplication

    Arithmetic multiplication.

    .. code-block:: edgeql-repl

        db> SELECT 2 * 10;
        {20}


.. eql:operator:: DIV: A / B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: divide division

    Arithmetic division.

    .. code-block:: edgeql-repl

        db> SELECT 10 / 4;
        {2.5}


.. eql:operator:: FLOORDIV: A // B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: floor divide division

    Integer division.

    The result is rounded down to the nearest integer. It is
    equivalent to using regular division and the applying
    :eql:func:`math::floor` to the result.

    .. code-block:: edgeql-repl

        db> SELECT 10 // 4;
        {2}
        db> SELECT math::floor(10 / 4);
        {2}
        db> SELECT -10 // 4;
        {-3}

    Regular division, integer division and :eql:op:`%<MOD>` are
    related in the following way: ``A / B = (A // B + A % B)``


.. eql:operator:: MOD: A % B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: modulo mod division

    Remainder from division (modulo).

    .. code-block:: edgeql-repl

        db> SELECT 7 % 4;
        {3}


.. eql:operator:: POW: A ^ B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: power pow

    Power operation.

    .. code-block:: edgeql-repl

        db> SELECT 2 ^ 4;
        {16}


.. _ref_eql_operators_datetime:

Date and Time
=============

.. eql:operator:: DTPLUS: A + B

    :optype A: datetime or local_datetime or local_time or \
               local_date or timedelta
    :optype B: datetime or local_datetime or local_time or \
               local_date or timedelta
    :resulttype: datetime or local_datetime or local_time or \
                 local_date or timedelta
    :index: plus add

    Time interval addition.

    .. code-block:: edgeql-repl

        db> select <local_time>'22:00' + <timedelta>'1 hour';
        {<local_time>'23:00:00'}
        db> select  <timedelta>'1 hour' + <local_time>'22:00';
        {<local_time>'23:00:00'}
        db> select  <timedelta>'1 hour' + <timedelta>'2 hours';
        {<timedelta>'3:00:00'}


.. eql:operator:: DTMINUS: A - B

    :optype A: datetime or local_datetime or local_time or \
               local_date or timedelta
    :optype B: datetime or local_datetime or local_time or \
               local_date or timedelta
    :resulttype: datetime or local_datetime or local_time or \
                 local_date or timedelta
    :index: minus subtract

    Time interval and date/time subtraction.

    .. code-block:: edgeql-repl

        db> select <datetime>'January 01 2019 UTC' - <timedelta>'1 day';
        {<datetime>'2018-12-31T00:00:00+00:00'}
        db> select <datetime>'January 01 2019 UTC' -
        ...   <datetime>'January 02 2019 UTC';
        {<timedelta>'-1 day, 0:00:00'}
        db> select  <timedelta>'1 hour' - <timedelta>'2 hours';
        {<timedelta>'-1 day, 23:00:00'}

    It is an error to subtract a date/time object from a time interval:

    .. code-block:: edgeql-repl

        db> select <timedelta>'1 day' - <datetime>'January 01 2019 UTC';
        QueryError: operator '-' cannot be applied to operands ...

    It is also an error to subtract timezone-aware :eql:type:`std::datetime`
    to or from :eql:type:`std::local_datetime`:

    .. code-block:: edgeql-repl

    db> select <datetime>'January 01 2019 UTC' -
    ...   <local_datetime>'January 02 2019';
    QueryError: operator '-' cannot be applied to operands ...
