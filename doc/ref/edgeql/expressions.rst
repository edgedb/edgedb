.. _ref_edgeql_expressions:


Expressions
===========

Expressions allow to manipulate, query and modify data in EdgeQL.

All expressions evaluate to sets of *objects*, *atomic values* or
*tuples*. Depending on the set sizes of the operands, operations
produce different sets of results.


Set and element operations
--------------------------

EdgeDB has two kinds of inputs on which operations are defined: sets
and elements.

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

Set operations treat ``EMPTY`` set as one of the possible valid
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
operand sets for a element operation is ``EMPTY``, the result is also
``EMPTY`` (since there are no elements produced in the Cartesian
product). This is particularly important for comparisons and boolean
logic operations as all of the following evaluate to ``EMPTY``:

.. code-block:: eql

    SELECT TRUE OR EMPTY;
    SELECT FALSE AND EMPTY;
    SELECT EMPTY = EMPTY;

This can lead to subtle mistakes when using actual paths that involve
non-required links (or the roots of which might not exists):

.. code-block:: eql

    # will evaluate to EMPTY if either
    # 'a' or 'b' link is missing on a given object Foo
    Foo.a OR Foo.b

When the desired behavior is to treat ``EMPTY`` as equivalent to
``FALSE``, the coalesce ``??`` operator should be used:

.. code-block:: eql

    # will treat missing 'a' or 'b' links as equivalent
    # to FALSE
    Foo.a ?? FALSE OR Foo.b ?? FALSE


Operations and paths
--------------------

There is some important interaction of the rule of
:ref:`"longest common prefix"<ref_edgeql_paths_prefix>`
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
:ref:`Statements<ref_edgeql_statements>` section.

Basic set operators:

- DISTINCT

    ``DISTINCT`` is a set operator that returns a new set where no
    member is equal to any other member. Considering that any two
    objects are equal if and only if they have the same identity, this
    operator is mainly useful when applied to sets of atomic values
    (or any other non-object, such as an array or tuple).

- UNION ALL

    ``UNION ALL`` is only valid for sets of atoms. It performs the set
    union where atoms are compared by *identity*. So effectively it
    merges two sets of atoms keeping all of the members.

    For example, if we use ``UNION ALL`` on two sets ``{1, 2, 2}`` and
    ``{2}``, we'll get the set ``{1, 2, 2, 2}``.

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

    The main reason why ``UNION`` works like this is that EdgeDB is
    optimized for working with sets of objects. So the simpler
    ``UNION`` operator must work intuitively with those sets. It would
    be very confusing if:

    ``(A UNION B).id`` ≢ ``A.id UNION B.id``

    Conversely, non-objects (e.g. atomic values) are treated specially
    from the beginning so having a special variant operator ``UNION
    ALL`` to preserve the set semantics they follow allows to
    consistently indicate that indeed all the individual values are
    desired throughout the computation.

- EXISTS

    ``EXISTS`` is a set operator that returns a singleton set
    ``{TRUE}`` if the input set is not ``EMPTY`` and returns
    ``{FALSE}`` otherwise.

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
    is ``EMPTY``, the whole choice expression evaluates to ``EMPTY``.

- Coalescing

    Coalescing ``a ?? b`` is, in fact, perfectly equivalent to:

    .. code-block:: eql

        SELECT a IF EXISTS a ELSE b;


Aggregate functions
-------------------

Aggregate functions are *set functions* mapping arbitrary sets onto
singletons. Technically, ``EXISTS`` behaves like a special built-in
aggregate. It is sufficiently basic and a special case that it is an
*operator* unlike a built-in aggregate function ``count``.


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

- array membership operators ``IN`` and ``NOT IN`` that test whether
  the left operand is an element in the right operand (which must be
  an array or appropriate type)

  .. code-block:: eql

    SELECT 1 IN [1, 3, 5];
    # returns [True]

  In order to test membership within a set the set must be transformed
  into an array using ``array_agg``:

  .. code-block:: eql

    SELECT 'Alice' IN array_agg(User.name);

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

- arithmetic operators ``+``, ``-``, ``*``, ``/``, ``%`` (modulo), ``^`` (power)


Regular functions
-----------------

Many built-in functions and user-defined functions operate on
elements, so they are also element operations. This implies that if
any of the input sets are ``EMPTY``, the result of applying an element
function is also ``EMPTY``.


Array or tuple creation
-----------------------

Creating an array or tuple via ``[...]`` or ``(...)`` is an element
operation. One way of thinking about these constructors is to treat
them exactly like functions that simply turn their arguments into an
array or a tuple, respectively.

This means that the following code will create a set of tuples with
the first element being ``Issue`` and the second a ``str``
representing the ``Issue.priority.name``:

.. code-block:: eql

    WITH MODULE example
    SELECT (Issue, Issue.priority.name);

Since ``priority`` is not a required link, not every ``Issue`` will
have one. It is important to realize that the above query will *only*
contain Issues with non-empty priorities. If it is desirable to have
*all* Issues, then a :ref:`shape<ref_edgeql_shapes>` query should be
used instead.

On the other hand the following query will include *all* Issues,
because the tuple elements are made from the set of Issues and the set
produced by the aggregator function ``array_agg``, which is never
``EMPTY``:

.. code-block:: eql

    WITH MODULE example
    SELECT (Issue, array_agg(Issue.priority.name));

All of the above works the same way for arrays.
