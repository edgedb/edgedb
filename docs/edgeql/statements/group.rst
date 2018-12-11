:orphan:

.. _ref_eql_statements_group:

GROUP
=====

:eql-statement:
:eql-haswith:

A ``GROUP`` statement is used to allow operations on set partitions.

The input set is partitioned using expressions in the ``USING`` and
``BY`` clauses, and then for each partition the expression in the
``UNION`` clause is evaluated and merged with the rest of the results
via a :eql:op:`UNION`. There are various useful functions that require a set
of values as their input - aggregate functions. Simple aggregate
function examples include :eql:func:`count`, :eql:func:`sum`,
:eql:func:`array_agg`. All of these are functions that map a set of
values onto a single value. A ``GROUP`` statement allows to use
aggregate functions to compute various properties of set partitions.

The data flow of a ``GROUP`` block can be conceptualized like this:

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]

    GROUP
        <alias0> := <expr0>     # define a set to partition

    USING

        <alias1> := <expr1>,    # define parameters to use for
        <alias2> := <expr2>,    # grouping
        ...
        <aliasN> := <exprN>

    BY
        <alias1>, ... <aliasN>  # specify which parameters will
                                # be used to partition the set

    INTO
        <sub_alias>             # provide an alias to refer to
                                # the subsets in expressions

    UNION
        <union-expr>            # map every grouped set onto a
                                # result set, merging them all with
                                # a UNION

    [ FILTER <filter-expr> ]

    [ ORDER BY <order-expr> ]

    [ OFFSET <offset-expr> ]

    [ LIMIT <limit-expr> ] ;

Notice that defining aliases in ``USING`` clause is
mandatory. Only the names defined in ``USING`` clause are legal in the
``BY`` clause. Also the names defined in ``USING`` and ``INTO``
clauses allow to unambiguously refer to the specific grouping subset
and the relevant grouping parameter values respectively in the
``UNION`` clause.

Consider the following example of a query that gets some statistics
about Issues, namely what's the total number of issues and time spent
per owner:

.. code-block:: edgeql

    WITH MODULE example
    GROUP Issue
    USING Owner := Issue.owner
    BY Owner
    INTO I
    UNION (
        owner := Owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    );

Although, this particular query may rewritten without using ``GROUP``,
but as a ``SELECT`` it is a useful example to illustrate how ``GROUP``
works.

If there's a need to only look at statistics that end up over a
certain threshold of total time spent, a ``FILTER`` can be used in
conjunction with an alias of the ``UNION`` clause result:

.. code-block:: edgeql

    WITH MODULE example
    GROUP Issue
    USING Owner := Issue.owner
    BY Owner
    INTO I
    UNION _stats = (
        owner := Owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    )
    FILTER _stats.total_time > 10;

The choice of result alias is arbitrary, same as for the ``WITH``
block. The alias defined here exists in the scope of the ``UNION``
block and can be used to apply ``FILTER`` and ``ORDER BY``.

If there's a need to filter the *input* set of Issues, then this can
be done by using a ``SELECT`` expression at the subject clause of the
``GROUP``:

.. code-block:: edgeql

    WITH MODULE example
    GROUP
        I := (
            SELECT Issue
            # in this GROUP only consider issues with watchers
            FILTER EXISTS Issue.watchers
        )
    USING Owner := I.owner
    BY Owner
    INTO I
    UNION _stats = (
        owner := Owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    )
    FILTER _stats.total_time > 10;


Clause signatures
+++++++++++++++++

Here is a summary of clauses that can be used with ``GROUP``:

- GROUP *A* USING ``SET OF`` *B1*, ..., ``SET OF`` *Bn*
- *A* BY ``SET OF`` *B* INTO *alias*
- ``SET OF`` *A* UNION ``SET OF`` *B*
- *A* FILTER ``SET OF`` *B*
- *A* ORDER BY ``SET OF`` *B*
- ``SET OF`` *A* OFFSET ``SET OF`` *B*
- ``SET OF`` *A* LIMIT ``SET OF`` *B*
