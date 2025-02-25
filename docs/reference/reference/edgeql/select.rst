.. _ref_eql_statements_select:

Select
======

:eql-statement:
:eql-haswith:

:index: order filter select offset limit with then asc desc first last empty

``select``--retrieve or compute a set of values.

.. eql:synopsis::

    [ with <with-item> [, ...] ]

    select <expr>

    [ filter <filter-expr> ]

    [ order by <order-expr> [direction] [then ...] ]

    [ offset <offset-expr> ]

    [ limit  <limit-expr> ] ;

:eql:synopsis:`filter <filter-expr>`
    The optional ``filter`` clause, where :eql:synopsis:`<filter-expr>`
    is any expression that has a result of type :eql:type:`bool`.
    The condition is evaluated for every element in the set produced by
    the ``select`` clause.  The result of the evaluation of the
    ``filter`` clause is a set of boolean values.  If at least one value
    in this set is ``true``, the input element is included, otherwise
    it is eliminated from the output.

.. _ref_reference_select_order:

:eql:synopsis:`order by <order-expr> [direction] [then ...]`
    The optional ``order by`` clause has this general form:

    .. eql:synopsis::

        order by
            <order-expr> [ asc | desc ] [ empty { first | last } ]
            [ then ... ]

    The ``order by`` clause produces a result set sorted according
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

    An optional ``asc`` or ``desc`` keyword can be added after any
    *expression*.  If not specified ``asc`` is assumed by default.

    If ``empty last`` is specified, then input values that produce
    an empty set when evaluating an *expression* are sorted *after*
    all other values; if ``empty first`` is specified, then they
    are sorted *before* all other values.  If neither is specified,
    ``empty first`` is assumed when ``asc`` is specified or implied,
    and ``empty last`` when ``desc`` is specified.

:eql:synopsis:`offset <offset-expr>`
    The optional ``offset`` clause, where
    :eql:synopsis:`<offset-expr>`
    is a *singleton expression* of an integer type.
    This expression is evaluated once and its result is used
    to skip the first *element-count* elements of the input set
    while producing the output.  If *element-count* evaluates to
    an empty set, it is equivalent to ``offset 0``, which is equivalent
    to omitting the ``offset`` clause.  If *element-count* evaluates
    to a value that is larger then the cardinality of the input set,
    an empty set is produced as the result.

:eql:synopsis:`limit <limit-expr>`
    The optional ``limit`` clause, where :eql:synopsis:`<limit-expr>`
    is a *singleton expression* of an integer
    type.  This expression is evaluated once and its result is used
    to include only the first *element-count* elements of the input set
    while producing the output.  If *element-count* evaluates to
    an empty set, it is equivalent to specifying no ``limit`` clause.


Description
-----------

``select`` retrieves or computes a set of values.  The data
flow of a ``select`` block can be conceptualized like this:

.. eql:synopsis::

    with module example

    # select clause
    select
        <expr>  # compute a set of things

    # optional clause
    filter
        <expr>  # filter the computed set

    # optional clause
    order by
        <expr>  # define ordering of the filtered set

    # optional clause
    offset
        <expr>  # slice the filtered/ordered set

    # optional clause
    limit
        <expr>  # slice the filtered/ordered set

Please note that the ``order by`` clause defines ordering that can
only be relied upon if the resulting set is not used in any other
operation. ``select``, ``offset`` and ``limit`` clauses are the only
exception to that rule as they preserve the inherent ordering of the
underlying set.

The first clause is ``select``. It indicates that ``filter``, ``order
by``, ``offset``, or ``limit`` clauses may follow an expression, i.e.
it makes an expression into a ``select`` statement. Without any of the
optional clauses a ``(select Expr)`` is completely equivalent to
``Expr`` for any expression ``Expr``.

Consider an example using the ``filter`` optional clause:

.. code-block:: edgeql

    with module example
    select User {
        name,
        owned := (select
            User.<owner[is Issue] {
                number,
                body
            }
        )
    }
    filter User.name like 'Alice%';



The above example retrieves a single user with a specific name. The
fact that there is only one such user is a detail that can be well-
known and important to the creator of the database, but otherwise non-
obvious. However, forcing the cardinality to be at most 1 by using the
``limit`` clause ensures that a set with a single object or
``{}`` is returned. This way any further code that relies on the
result of this query can safely assume there's only one result
available.

.. code-block:: edgeql

    with module example
    select User {
        name,
        owned := (select
            User.<owner[is Issue] {
                number,
                body
            }
        )
    }
    filter User.name like 'Alice%'
    limit 1;

Next example makes use of ``order by`` and ``limit`` clauses:

.. code-block:: edgeql

    with module example
    select Issue {
        number,
        body,
        due_date
    }
    filter
        exists Issue.due_date
        and
        Issue.status.name = 'Open'
    order by
        Issue.due_date
    limit 3;

The above query retrieves the top 3 open Issues with the closest due
date.


.. _ref_eql_statements_select_filter:

Filter
------

The ``filter`` clause cannot affect anything aggregate-like in the
preceding ``select`` clause. This is due to how ``filter`` clause
works. It can be conceptualized as a function like ``filter($input,
set of $cond)``, where the ``$input`` represents the value of the
preceding clause, while the ``$cond`` represents the filtering
condition expression. Consider the following:

.. code-block:: edgeql

    with module example
    select count(User)
    filter User.name like 'Alice%';

The above can be conceptualized as:

.. code-block:: edgeql

    with module example
    select _filter(
        count(User),
        User.name like 'Alice%'
    );

In this form it is more apparent that ``User`` is a ``set of``
argument (of :eql:func:`count`), while ``User.name like 'Alice%'`` is
also a ``set of`` argument (of ``filter``). So the symbol ``User`` in
these two expressions exists in 2 parallel scopes. Contrast it with:

.. code-block:: edgeql

    # This will actually only count users whose name starts with
    # 'Alice'.

    with module example
    select count(
        (select User
         filter User.name like 'Alice%')
    );

    # which can be represented as:
    with module example
    select count(
        _filter(User,
               User.name like 'Alice%')
    );

Clause signatures
-----------------

Here is a summary of clauses that can be used with ``select``:

- *A* filter ``set of`` *B*
- *A* order by ``set of`` *B*
- ``set of`` *A* offset ``set of`` *B*
- ``set of`` *A* limit ``set of`` *B*

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Select <ref_eql_select>`
  * - :ref:`Cheatsheets > Selecting data <ref_cheatsheet_select>`
