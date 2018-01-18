.. _ref_edgeql_expressions:


Operators
=========

Expressions allow to manipulate, query, and modify data in EdgeQL.

All expressions in EdgeQL evaluate to *multisets*. Informally, the
main difference between a set and a multiset is that multiple
instances of the same elements are allowed in a multiset. All
multisets in EdgeQL have to contain elements of the same type. Broadly
all types can be broken down into the following categories: *objects*,
*atomic values*, *array*, *maps*, or *tuples*.

One important consideration is that any path expression denotes a
*multiset* of values contained in a *set* of graph nodes reachable by
that path. This means that a path pointing to *concepts* will always
evaluate to a *multiset* with unique elements, whereas any other path
may evaluate to a *multiset* containing duplicate values. This is
because only *objects* are guaranteed to be unique in the EdgeDB
conceptual relationship graph.

All expressions evaluate to multisets of *objects*, *atomic values*,
*array*, *maps*, or *tuples*. Depending on the set sizes of the
operands, operations produce different sets of results.

It is convenient to treat all expressions as multisets, in particular
``1`` is equivalent to ``{1}`` in EdgeQL. Also, there's no way to
produce nested multisets in EdgeQL as an expression (although
conceptually
:ref:`GROUP<ref_edgeql_statements_group>` statement operates on sets
of multisets as one of its intermediate steps). Because of that nested
multisets are automatically flattened:

    ``{1, 2, {3, 4}, 5}`` ≡ ``{1, 2, 3, 4, 5}``

.. note::

    Those familiar with *bunch theory* will recognize that effectively
    EdgeQL operates on bunches rather than multisets. We choose to use
    the notation typically associated with sets, such as ``{a, b,
    c}``, because of the need to disambiguate *tuples* from sets (or
    bunches) without using symbols that are difficult to type.

        ``a, b, c`` - is a bunch

        ``(a, b, c)`` - is an EdgeQL tuple (but could be confused with
        a bunch)

        ``{a, b, c}`` - is an EdgeQL multiset (which has properties very
        similar to a bunch)

Also, for brevity the term "set" is frequently used in documentation
to denote EdgeDB multisets.


Set and element arguments
-------------------------

In EdgeDB operators and functions can take sets or individual elements
as arguments. For the purpose of generalization all operators can be
viewed as functions. Since fundamentally EdgeDB operates on sets, that
means that all functions that are defined to take elements are
generalized to operate on sets by applying the function to each input
set element separately. Incidentally, since ultimately EdgeDB operates
on sets it is conceptually convenient to treat all functions as
returning sets as their result.

.. note::

    Given an *n-ary* function *f* and A\ :sub:`1`, ..., A\ :sub:`n`
    ⊆ U, define the result set of applying the function as:

        :emphasis:`f`\ (A\ :sub:`1`, ..., A\ :sub:`n`) ≡
        { :emphasis:`f`\ (t): ∀ t ∈ A\ :sub:`1` ⨉ ... ⨉ A\ :sub:`n` }

Functions that operate on sets only, don't require additional
mechanisms to be applied to EdgeDB sets.

In the general case a function can have some arguments as elements and others
as sets. The generalized formula is the given by the following:

.. note::

    Given a function *F* with *n* element-parameters and *m* set-
    parameters as well as A\ :sub:`1`, ..., A\ :sub:`n`, B\ :sub:`1`,
    ..., B\ :sub:`m` ⊆ U, the result set of applying the function is
    as follows:

        :emphasis:`F`\ (A\ :sub:`1`, ..., A\ :sub:`n`, B\ :sub:`1`, ...,
        B\ :sub:`m`) ≡

            { :emphasis:`F`\ (a\ :sub:`1`, ..., a\ :sub:`n`, B\ :sub:`1`,
            ..., B\ :sub:`m`): ∀ a\ :sub:`1`, ..., a\ :sub:`n` ∈ A\
            :sub:`1` ⨉ ... ⨉ A\ :sub:`n` }

One of the basic operators in EdgeQL (``IN``) is an example of such a
mixed function and will be covered in more details below. Most
operators and functions have all their parameters as either all sets
or all elements.

The above definitions assume that all the input sets are different
from each other. What happens when some of the input sets are
semantically the same? In that case there's a particular interaction
between element-parameters and set-parameters.

For simplicity we can consider the case when some 2 input sets are the
same. Let's call them both `X`. This results in 3 possible general
cases:

- The same sets are both used as element-parameters

    :emphasis:`F`\ (X, X, A\ :sub:`3`, ..., A\ :sub:`n`, B\ :sub:`1`, ...,
    B\ :sub:`m`) ≡

            { :emphasis:`F`\ (x, x, a\ :sub:`3`, ..., a\ :sub:`n`, B\ :sub:`1`,
            ..., B\ :sub:`m`): ∀ x, a\ :sub:`3`, ..., a\ :sub:`n` ∈ X ⨉ A\
            :sub:`3` ⨉ ... ⨉ A\ :sub:`n` }

- The same sets are both used as set-parameters

    :emphasis:`F`\ (A\ :sub:`1`, ..., A\ :sub:`n`, X, X, B\ :sub:`3`, ...,
    B\ :sub:`m`) ≡

            { :emphasis:`F`\ (a\ :sub:`1`, ..., a\ :sub:`n`, X, X, B\ :sub:`3`,
            ..., B\ :sub:`m`): ∀ a\ :sub:`1`, ..., a\ :sub:`n` ∈ A\
            :sub:`1` ⨉ ... ⨉ A\ :sub:`n` }

- One of the sets is element-parameter and the other is set-parameter

    :emphasis:`F`\ (X, A\ :sub:`2`, ..., A\ :sub:`n`, X, B\ :sub:`2`, ...,
    B\ :sub:`m`) ≡

            { :emphasis:`F`\ (x, a\ :sub:`2`, ..., a\ :sub:`n`, {x},
            B\ :sub:`2`, ..., B\ :sub:`m`):
            ∀ x, a\ :sub:`2`, ..., a\ :sub:`n` ∈
            X ⨉ A\ :sub:`2` ⨉ ... ⨉ A\ :sub:`n` }

The first two cases are fairly straightforward and intuitive. The
third case is special and defines how EdgeDB processes queries. That
is the basic rule from which
:ref:`longest common prefix<ref_edgeql_paths_prefix>` property follows.

EdgeQL uses ``SET OF`` qualifier in function declarations to
disambiguate between the element-parameters and set-parameters. EdgeQL
operator signatures can be described in a similar way to make it clear
how they are applied.

.. TODO::

    This section requires a significant rewrite w.r.t. classification
    of operations.

+-------------------------------+-------------------------------+
| Set                           | Element                       |
+===============================+===============================+
| - statements (and clauses)    | - OR, AND, NOT                |
| - UNION, UNION ALL, DISTINCT  | - =, !=                       |
| - EXISTS                      | - <, >, <=, >=                |
| - IF..ELSE                    | - LIKE, ILIKE                 |
| - ??                          | - IN, NOT IN                  |
| - all aggregate functions     | - IS, IS NOT                  |
|                               | - +, -, \*, /, %, ^           |
|                               | - all regular functions       |
|                               | - creating an array           |
|                               | - creating a tuple            |
+-------------------------------+-------------------------------+

Set operations treat empty set ``{}`` as one of the possible valid
inputs, but otherwise not very special.

Element operations work on set elements as opposed to sets. So
to reconcile that with the fact that everything is a set in EdgeQL we
define the application of an element operation in the following manner:

.. note::

    Given and *n-ary* operation *op* and A\ :sub:`1`, ..., A\ :sub:`n`
    ⊆ U, define the result set of applying the operation as:

    :emphasis:`op`\ (A\ :sub:`1`, ..., A\ :sub:`n`) ≡
    { :emphasis:`op`\ (t): ∀ t ∈ A\ :sub:`1` ⨉ ... ⨉ A\ :sub:`n` }

One of the consequences of this definition is that it gives a way to
measure the maximum cardinality of the result set for a given element
operation and input sets.

Another consequence of the above definition is that if any of the
operand sets for a element operation is ``{}``, the result is also
``{}`` (since there are no elements produced in the Cartesian
product). This is particularly important for comparisons and boolean
logic operations as all of the following evaluate to ``{}``:

.. code-block:: eql

    SELECT TRUE OR {};
    SELECT FALSE AND {};
    SELECT {} = {};

This can lead to subtle mistakes when using actual paths that involve
non-required links (or the roots of which might not exists):

.. code-block:: eql

    # will evaluate to {} if either 'a' or 'b' link is missing on a
    # given object Foo
    SELECT Foo.a OR Foo.b;

When the desired behavior is to treat ``{}`` as equivalent to
``FALSE``, the coalesce ``??`` operator should be used:

.. code-block:: eql

    # will treat missing 'a' or 'b' links as equivalent to FALSE
    SELECT Foo.a ?? FALSE OR Foo.b ?? FALSE;


Operations and paths
--------------------

There is some important interaction of the rule of
:ref:`longest common prefix<ref_edgeql_paths_prefix>`
for paths and operation cardinality. Consider the following example:

.. code-block:: eql

    SELECT Issue.status.name + Issue.number;

The expression ``Issue.status.name`` is a set of all strings, that are
reachable from any ``Issue`` by following the link ``status`` and then
``name``. Because the link ``status`` has the default cardinality of
``*1`` and so does the link ``name`` overall the expression has the
same cardinality as the set of ``Issues``. Similarly, as a separate
expression ``Issue.number`` would have the same cardinality as
``Issues``. However, due to the common prefix rule that states that a
common prefix denotes *the same* object the operation ``+`` is not
applied to the cross-product of the set ``Issue.status.name`` and
``Issue.number`` as if they were independent. Instead for every common
prefix (``Issue`` in this case), the operation is applied to the
cross-product of the subsets denoted by the remainder of the operand
paths. For the sample query, these subsets happen to be singleton sets
for every ``Issue``, because all the links followed from ``Issue``
have the default cardinality ``*1``, pointing to singleton sets. Thus
the result of the operation for each ``Issue`` is also a singleton set
and the overall cardinality of the expression ``Issue.status.name +
Issue.number`` is the same as the cardinality of ``Issues``.


.. _ref_edgeql_expressions_setops:

Set operations
--------------

Statements and clauses are effectively set operations and are
discussed in more details in the
:ref:`Statements<ref_edgeql_statements>` section. One of the
building blocks used in these examples is a set literal, e.g. ``{1, 2,
3}``. In the simplest form this expression denotes a set of elements.
Like any other EdgeDB sets the elements all have to be of the same
type (all sets are homogeneous).

Basic set operators:

- DISTINCT

    ``DISTINCT`` is a set operator that returns a new set where no
    member is equal to any other member. Considering that any two
    objects are equal if and only if they have the same identity (that
    is to say, the value of an object is equal to its identity), this
    operator is mainly useful when applied to sets of atomic values
    (or any other non-object, such as an array or tuple).

- UNION ALL

    ``UNION ALL`` is only defined for entities that can form
    multisets: *atomic values*, *array*, *maps*, or *tuples*. Formally
    ``UNION ALL`` is a *multiset sum*, so effectively it merges two
    multisets keeping all of their members.

    For example, if we use ``UNION ALL`` on two multisets ``{1, 2,
    2}`` and ``{2}``, we'll get the multiset ``{1, 2, 2, 2}``.

- UNION

    ``UNION`` is a set operator that performs the set union where
    members are compared by *value*. This operation works out
    intuitively for objects because their identity and value are
    equivalent. For atoms it is equivalent to: ``DISTINCT (A UNION ALL
    B)``. In particular that means that:

    ``{1, 2} UNION {2, 3}`` ≡ ``{1, 2, 3}``

    ``{User1, User2} UNION {User1, User3}`` ≡ ``{User1, User2, User3}``

    ``A UNION A UNION A`` ≡ ``A UNION A`` ≡ ``DISTINCT A``

    .. note::

        The main reason why ``UNION`` works like this is that EdgeDB
        is optimized for working with sets of objects. So the simpler
        ``UNION`` operator must work intuitively with those sets. It
        would be very confusing if:

        ``(A UNION B).id`` ≢ ``A.id UNION B.id``

        Conversely, non-objects (e.g. atomic values) are treated
        specially from the beginning so having a special variant
        operator ``UNION ALL`` to preserve the set semantics they
        follow allows to consistently indicate that indeed all the
        individual values are desired throughout the computation.

- {...}

    The set literal has more advanced features in EdgeDB. Basically,
    if any other sets are nested in it, the set literal will *flatten*
    them out. Effectively a set literal is equivalent to applying
    ``UNION ALL`` (or ``UNION`` for objects) to all its elements:

    ``{1, 2, {3, 4}, 5}`` ≡ ``{1, 2, 3, 4, 5}``

    For any two sets ``A``, ``B`` of the same type:
    ``{A, B}`` = ``A UNION B``

- EXISTS

    ``EXISTS`` is a set operator that returns a singleton set
    ``{TRUE}`` if the input set is not ``{}`` and returns
    ``{FALSE}`` otherwise.

    .. note::

        Technically, ``EXISTS`` behaves like a special built-in
        :ref:`aggregate function<ref_edgeql_functions_agg>`. It is
        sufficiently basic and a special case that it is an *operator*
        unlike a built-in aggregate function ``count``.

- IF..ELSE

    It's worth noting that ``IF..ELSE`` is a kind of syntax sugar for
    the following expression:

    .. code-block:: eql

        # SELECT a IF cond ELSE b is equivalent to the below:
        SELECT
            (SELECT a FILTER cond)
            UNION
            (SELECT b FILTER NOT cond);

    .. XXX is it really? what about UNION ALL version?

    One of the consequences of this is that if the ``cond`` expression
    is ``{}``, the whole choice expression evaluates to ``{}``.

.. _ref_edgeql_expressions_coalesce:

- Coalescing

    Coalescing ``a ?? b`` is, in fact, perfectly equivalent to:

    .. code-block:: eql

        SELECT a IF EXISTS a ELSE b;

    A typical use case of coalescing operator is to provide default
    values for optional links.

    .. code-block:: eql

        # get a set of tuples (<issue name>, <priority>) for all
        # issues
        WITH MODULE example
        SELECT (Issue.name, Issue.priority.name ?? 'n/a');

    Without the coalescing operator the above query would skip any
    ``Issue`` without priority.


.. _ref_edgeql_expressions_elops:

Element operations
------------------

Element operations are largely represented by various operators. Most
of these operators require their operands to be of the same
:ref:`type<ref_edgeql_types>`.

- boolean operators ``OR``, ``AND``, ``NOT``

- value equality operators ``=`` and ``!=``

- comparison operators ``<``, ``>``, ``<=``, ``>=``

- string matching operators ``LIKE`` and ``ILIKE`` that work exactly the
  same way as in SQL

- set membership operators ``IN`` and ``NOT IN`` that test whether the
  left operand is an element in the right operand, for each element of
  the left operand

  .. code-block:: eql

    SELECT 1 IN {1, 3, 5};
    # returns [True]

    SELECT 'Alice' IN User.name;

- type-checking operators ``IS`` and ``IS NOT`` that test whether the
  left operand is of any of the types given by the comma-separated
  list of types provided as the right operand

  .. code-block:: eql

    SELECT 1 IS int;
    # returns [True]

    SELECT User IS NOT SystemUser
    FILTER User.name = 'Alice';
    # returns [True]

    SELECT User IS (Text, Named);
    # returns [True, ..., True], one for every user

- arithmetic operators ``+``, ``-``, ``*``, ``/``, ``%`` (modulo),
  ``^`` (power)
