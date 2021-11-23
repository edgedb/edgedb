.. _ref_eql_statements_select:

SELECT
======

:eql-statement:
:eql-haswith:

:index: order filter select offset limit with then asc desc first last empty

``SELECT``--retrieve or compute a set of values.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]

    SELECT <expr>

    [ FILTER <filter-expr> ]

    [ ORDER BY <order-expr> [direction] [THEN ...] ]

    [ OFFSET <offset-expr> ]

    [ LIMIT  <limit-expr> ] ;

:eql:synopsis:`FILTER <filter-expr>`
    The optional ``FILTER`` clause, where :eql:synopsis:`<filter-expr>`
    is any expression that has a result of type :eql:type:`bool`.
    The condition is evaluated for every element in the set produced by
    the ``SELECT`` clause.  The result of the evaluation of the
    ``FILTER`` clause is a set of boolean values.  If at least one value
    in this set is ``true``, the input element is included, otherwise
    it is eliminated from the output.

.. _ref_reference_select_order:

:eql:synopsis:`ORDER BY <order-expr> [direction] [THEN ...]`
    The optional ``ORDER BY`` clause has this general form:

    .. eql:synopsis::

        ORDER BY
            <order-expr> [ ASC | DESC ] [ EMPTY { FIRST | LAST } ]
            [ THEN ... ]

    The ``ORDER BY`` clause produces a result set sorted according
    to the specified expression or expressions, which are evaluated
    for every element of the input set.

    If two elements are equal according to the leftmost *expression*, they
    are compared according to the next expression and so on.  If two
    elements are equal according to all expressions, the resulting order
    is undefined.

    Each *expression* can be an arbitrary expression that results in a
    value of an *orderable type*.  Primitive types are orderable,
    object types are not.  Additionally, the result of each expression
    must be an empty set or a singleton.  Using an expression that may
    produce more elements is a compile-time error.

    An optional ``ASC`` or ``DESC`` keyword can be added after any
    *expression*.  If not specified ``ASC`` is assumed by default.

    If ``EMPTY LAST`` is specified, then input values that produce
    an empty set when evaluating an *expression* are sorted *after*
    all other values; if ``EMPTY FIRST`` is specified, then they
    are sorted *before* all other values.  If neither is specified,
    ``EMPTY FIRST`` is assumed when ``ASC`` is specified or implied,
    and ``EMPTY LAST`` when ``DESC`` is specified.

:eql:synopsis:`OFFSET <offset-expr>`
    The optional ``OFFSET`` clause, where
    :eql:synopsis:`<offset-expr>`
    is a *singleton expression* of an integer type.
    This expression is evaluated once and its result is used
    to skip the first *element-count* elements of the input set
    while producing the output.  If *element-count* evaluates to
    an empty set, it is equivalent to ``OFFSET 0``, which is equivalent
    to omitting the ``OFFSET`` clause.  If *element-count* evaluates
    to a value that is larger then the cardinality of the input set,
    an empty set is produced as the result.

:eql:synopsis:`LIMIT <limit-expr>`
    The optional ``LIMIT`` clause, where :eql:synopsis:`<limit-expr>`
    is a *singleton expression* of an integer
    type.  This expression is evaluated once and its result is used
    to include only the first *element-count* elements of the input set
    while producing the output.  If *element-count* evaluates to
    an empty set, it is equivalent to specifying no ``LIMIT`` clause.


Description
-----------

``SELECT`` retrieves or computes a set of values.  The data
flow of a ``SELECT`` block can be conceptualized like this:

.. eql:synopsis::

    WITH MODULE example

    # select clause
    SELECT
        <expr>  # compute a set of things

    # optional clause
    FILTER
        <expr>  # filter the computed set

    # optional clause
    ORDER BY
        <expr>  # define ordering of the filtered set

    # optional clause
    OFFSET
        <expr>  # slice the filtered/ordered set

    # optional clause
    LIMIT
        <expr>  # slice the filtered/ordered set

Please note that the ``ORDER BY`` clause defines ordering that can
only be relied upon if the resulting set is not used in any other
operation. ``SELECT``, ``OFFSET`` and ``LIMIT`` clauses are the only
exception to that rule as they preserve the inherent ordering of the
underlying set.

The first clause is ``SELECT``. It indicates that ``FILTER``, ``ORDER
BY``, ``OFFSET``, or ``LIMIT`` clauses may follow an expression, i.e.
it makes an expression into a ``SELECT`` statement. Without any of the
optional clauses a ``(SELECT Expr)`` is completely equivalent to
``Expr`` for any expression ``Expr``.

Consider an example using the ``FILTER`` optional clause:

.. code-block:: edgeql

    WITH MODULE example
    SELECT User {
        name,
        owned := (SELECT
            User.<owner[IS Issue] {
                number,
                body
            }
        )
    }
    FILTER User.name LIKE 'Alice%';



The above example retrieves a single user with a specific name. The
fact that there is only one such user is a detail that can be well-
known and important to the creator of the database, but otherwise non-
obvious. However, forcing the cardinality to be at most 1 by using the
``LIMIT`` clause ensures that a set with a single object or
``{}`` is returned. This way any further code that relies on the
result of this query can safely assume there's only one result
available.

.. code-block:: edgeql

    WITH MODULE example
    SELECT User {
        name,
        owned := (SELECT
            User.<owner[IS Issue] {
                number,
                body
            }
        )
    }
    FILTER User.name LIKE 'Alice%'
    LIMIT 1;

Next example makes use of ``ORDER BY`` and ``LIMIT`` clauses:

.. code-block:: edgeql

    WITH MODULE example
    SELECT Issue {
        number,
        body,
        due_date
    }
    FILTER
        EXISTS Issue.due_date
        AND
        Issue.status.name = 'Open'
    ORDER BY
        Issue.due_date
    LIMIT 3;

The above query retrieves the top 3 open Issues with the closest due
date.


.. _ref_eql_statements_select_filter:

Filter
------

The ``FILTER`` clause cannot affect anything aggregate-like in the
preceding ``SELECT`` clause. This is due to how ``FILTER`` clause
works. It can be conceptualized as a function like ``filter($input,
SET OF $cond)``, where the ``$input`` represents the value of the
preceding clause, while the ``$cond`` represents the filtering
condition expression. Consider the following:

.. code-block:: edgeql

    WITH MODULE example
    SELECT count(User)
    FILTER User.name LIKE 'Alice%';

The above can be conceptualized as:

.. code-block:: edgeql

    WITH MODULE example
    SELECT _filter(
        count(User),
        User.name LIKE 'Alice%'
    );

In this form it is more apparent that ``User`` is a ``SET OF``
argument (of :eql:func:`count`), while ``User.name LIKE 'Alice%'`` is
also a ``SET OF`` argument (of ``filter``). So the symbol ``User`` in
these two expressions exists in 2 parallel scopes. Contrast it with:

.. code-block:: edgeql

    # This will actually only count users whose name starts with
    # 'Alice'.

    WITH MODULE example
    SELECT count(
        (SELECT User
         FILTER User.name LIKE 'Alice%')
    );

    # which can be represented as:
    WITH MODULE example
    SELECT count(
        _filter(User,
               User.name LIKE 'Alice%')
    );

Clause signatures
-----------------

Here is a summary of clauses that can be used with ``SELECT``:

- *A* FILTER ``SET OF`` *B*
- *A* ORDER BY ``SET OF`` *B*
- ``SET OF`` *A* OFFSET ``SET OF`` *B*
- ``SET OF`` *A* LIMIT ``SET OF`` *B*

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Select <ref_eql_select>`
  * - :ref:`Cheatsheets > Selecting data <ref_cheatsheet_select>`
