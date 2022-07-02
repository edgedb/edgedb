.. _ref_std_set:

===
Set
===

:edb-alt-title: Set Functions and Operators
:index: set aggregate


.. list-table::
    :class: funcoptable

    * - :eql:op:`distinct set <distinct>`
      - :eql:op-desc:`distinct`

    * - :eql:op:`anytype in set <in>`
      - :eql:op-desc:`in`

    * - :eql:op:`set union set <union>`
      - :eql:op-desc:`union`

    * - :eql:op:`exists set <exists>`
      - :eql:op-desc:`exists`

    * - :eql:op:`set if bool else set <if..else>`
      - :eql:op-desc:`if..else`

    * - :eql:op:`optional anytype ?? set <coalesce>`
      - :eql:op-desc:`coalesce`

    * - :eql:op:`detached`
      - :eql:op-desc:`detached`

    * - :eql:op:`anytype [is type] <isintersect>`
      - :eql:op-desc:`isintersect`

    * - :eql:func:`assert_distinct`
      - :eql:func-desc:`assert_distinct`

    * - :eql:func:`assert_single`
      - :eql:func-desc:`assert_single`

    * - :eql:func:`assert_exists`
      - :eql:func-desc:`assert_exists`

    * - :eql:func:`count`
      - :eql:func-desc:`count`

    * - :eql:func:`array_agg`
      - :eql:func-desc:`array_agg`

    * - :eql:func:`sum`
      - :eql:func-desc:`sum`

    * - :eql:func:`all`
      - :eql:func-desc:`all`

    * - :eql:func:`any`
      - :eql:func-desc:`any`

    * - :eql:func:`enumerate`
      - :eql:func-desc:`enumerate`

    * - :eql:func:`min`
      - :eql:func-desc:`min`

    * - :eql:func:`max`
      - :eql:func-desc:`max`

    * - :eql:func:`math::mean`
      - :eql:func-desc:`math::mean`

    * - :eql:func:`math::stddev`
      - :eql:func-desc:`math::stddev`

    * - :eql:func:`math::stddev_pop`
      - :eql:func-desc:`math::stddev_pop`

    * - :eql:func:`math::var`
      - :eql:func-desc:`math::var`

    * - :eql:func:`math::var_pop`
      - :eql:func-desc:`math::var_pop`


----------


.. eql:operator:: distinct: distinct set of anytype -> set of anytype

    Return a set without repeating any elements.

    ``distinct`` is a set operator that returns a new set where
    no member is equal to any other member.

    .. code-block:: edgeql-repl

        db> select distinct {1, 2, 2, 3};
        {1, 2, 3}


----------


.. eql:operator:: in: anytype in set of anytype -> bool
                      anytype not in set of anytype -> bool

    :index: intersection

    Test the membership of an element in a set.

    Set membership operators ``in`` and ``not in`` that test for each
    element of ``A`` whether it is present in ``B``.

    .. code-block:: edgeql-repl

        db> select 1 in {1, 3, 5};
        {true}

        db> select 'Alice' in User.name;
        {true}

        db> select {1, 2} in {1, 3, 5};
        {true, false}

    This operator can also be used to implement set intersection:

    .. code-block:: edgeql-repl

        db> with
        ...     A := {1, 2, 3, 4},
        ...     B := {2, 4, 6}
        ... select A filter A in B;
        {2, 4}


----------


.. eql:operator:: union: set of anytype union set of anytype -> set of anytype

    Merge two sets.

    Since EdgeDB sets are formally multisets, ``union`` is a *multiset sum*,
    so effectively it merges two multisets keeping all of their members.

    For example, applying ``union`` to ``{1, 2, 2}`` and
    ``{2}``, results in ``{1, 2, 2, 2}``.

    If you need a distinct union, wrap it with :eql:op:`distinct`.


----------


.. eql:operator:: if..else: set of anytype if bool else set of anytype \
                                -> set of anytype

    :index: if else ifelse elif ternary

    Conditionally provide one or the other result.

    .. eql:synopsis::

        <left_expr> if <condition> else <right_expr>

    If :eql:synopsis:`<condition>` is ``true``, then the value of the
    ``if..else`` expression is the value of :eql:synopsis:`<left_expr>`;
    if :eql:synopsis:`<condition>` is ``false``, the result is the value of
    :eql:synopsis:`<right_expr>`.

    .. code-block:: edgeql-repl

        db> select 'hello' if 2 * 2 = 4 else 'bye';
        {'hello'}

    ``if..else`` expressions can be chained when checking multiple conditions
    is necessary:

    .. code-block:: edgeql-repl

        db> with color := 'yellow'
        ... select 'Apple' if color = 'red' else
        ...        'Banana' if color = 'yellow' else
        ...        'Orange' if color = 'orange' else
        ...        'Other';
        {'Banana'}

-----------


.. eql:operator:: coalesce: optional anytype ?? set of anytype \
                              -> set of anytype

    Coalesce.

    Evaluate to ``A`` for non-empty ``A``, otherwise evaluate to ``B``.

    A typical use case of the coalescing operator is to provide default
    values for optional properties.

    .. code-block:: edgeql

        # Get a set of tuples (<issue name>, <priority>)
        # for all issues.
        select (Issue.name, Issue.priority.name ?? 'n/a');

    Without the coalescing operator the above query would skip any
    ``Issue`` without priority.


----------

.. _ref_stdlib_set_detached:

.. eql:operator:: detached: detached set of anytype -> set of anytype

    Detaches the input set reference from the current scope.

    A ``detached`` expression allows referring to some set as if it were
    defined in the top-level ``with`` block. ``detached``
    expressions ignore all current scopes in which they are nested.
    This makes it possible to write queries that reference the same set
    reference in multiple places.

    .. code-block:: edgeql

        update User
        filter .name = 'Dave'
        set {
            friends := (select detached User filter .name = 'Alice'),
            coworkers := (select detached User filter .name = 'Bob')
        };

    Without ``detached``, the occurrences of ``User`` inside the ``set`` shape
    would be *bound* to the set of users named ``"Dave"``. However, in this
    context we want to run an unrelated query on the "unbound" ``User`` set.

    .. code-block:: edgeql

        # does not work!
        update User
        filter .name = 'Dave'
        set {
            friends := (select User filter .name = 'Alice'),
            coworkers := (select User filter .name = 'Bob')
        };

    Instead of explicitly detaching a set, you can create a reference to it in
    a ``with`` block. All declarations inside a ``with`` block are implicitly
    detached.

    .. code-block:: edgeql

        with U1 := User,
             U2 := User
        update User
        filter .name = 'Dave'
        set {
            friends := (select U1 filter .name = 'Alice'),
            coworkers := (select U2 filter .name = 'Bob')
        };



----------


.. eql:operator:: exists: exists set of anytype -> bool

    Test whether a set is not empty.

    ``exists`` is an aggregate operator that returns a singleton set
    ``{true}`` if the input set is not empty and returns ``{false}``
    otherwise.

    .. code-block:: edgeql-repl

        db> select exists {1, 2};
        {true}


----------


.. eql:operator:: isintersect: anytype [is type] -> anytype

    :index: is type intersection

    Filter the set based on type.

    The type intersection operator removes all elements from the input set
    that aren't of the specified type. Additionally, since it
    guarantees the type of the result set, all the links and properties
    associated with the specified type can now be used on the
    resulting expression. This is especially useful in combination
    with :ref:`backlinks <ref_datamodel_links>`.

    Consider the following types:

    .. code-block:: sdl

        type User {
            required property name -> str;
        }

        abstract type Owned {
            required link owner -> User;
        }

        type Issue extending Owned {
            required property title -> str;
        }

        type Comment extending Owned {
            required property body -> str;
        }

    The following expression will get all :eql:type:`Objects <Object>`
    owned by all users (if there are any):

    .. code-block:: edgeql

        select User.<owner;

    By default :ref:`backlinks <ref_datamodel_links>` don't infer any
    type information beyond the fact that it's an :eql:type:`Object`.
    To ensure that this path specifically reaches ``Issue`` the type
    intersection operator must be used:

    .. code-block:: edgeql

        select User.<owner[is Issue];

        # With the use of type intersection it's possible to refer to
        # specific property of Issue now:
        select User.<owner[is Issue].title;


----------


.. eql:function:: std::assert_distinct( \
                    s: set of anytype, \
                    named only message: optional str = <str>{} \
                  ) -> set of anytype

    :index: multiplicity uniqueness

    Check that the input set contains only unique elements, i.e a *proper set*.

    If the input set contains duplicate elements, ``assert_distinct`` raises a
    ``ConstraintViolationError``.  This function is useful
    as a runtime distinctness assertion in queries and computed
    expressions that should always return proper sets, but where static
    multiplicity inference is not capable enough or outright impossible.
    An optional *message* named argument can be used to customize the error
    message.

    .. code-block:: edgeql-repl

        db> select assert_distinct(
        ...   (select User filter .groups.name = "Administrators")
        ...   union
        ...   (select User filter .groups.name = "Guests")
        ... )
        {default::User {id: ...}}

        db> select assert_distinct(
        ...   (select User filter .groups.name = "Users")
        ...   union
        ...   (select User filter .groups.name = "Guests")
        ... )
        ERROR: ConstraintViolationError: assert_distinct violation: expression
               returned a set with duplicate elements.

        db> select assert_distinct(
        ...   (select User filter .groups.name = "Users")
        ...   union
        ...   (select User filter .groups.name = "Guests"),
        ...   message := "duplicate users!"
        ... )
        ERROR: ConstraintViolationError: duplicate users!

----------


.. eql:function:: std::assert_single( \
                    s: set of anytype, \
                    named only message: optional str = <str>{} \
                  ) -> set of anytype

    :index: cardinality singleton

    Check that the input set contains no more than one element.

    If the input set contains more than one element, ``assert_single``
    raises a ``CardinalityViolationError``.  This function is useful
    as a runtime cardinality assertion in queries and computed
    expressions that should always return sets with at most a single
    element, but where static cardinality inference is not capable
    enough or outright impossible.  An optional *message* named argument
    can be used to customize the error message.

    .. code-block:: edgeql-repl

        db> select assert_single((select User filter .name = "Unique"))
        {default::User {id: ...}}

        db> select assert_single((select User))
        ERROR: CardinalityViolationError: assert_single violation: more than
               one element returned by an expression

        db> select assert_single((select User), message := "too many users!")
        ERROR: CardinalityViolationError: too many users!

----------


.. eql:function:: std::assert_exists( \
                    s: set of anytype, \
                    named only message: optional str = <str>{} \
                  ) -> set of anytype

    :index: cardinality existence empty

    Check that the input set contains at least one element.

    If the input set is empty, ``assert_exists`` raises a
    ``CardinalityViolationError``.  This function is useful
    as a runtime existence assertion in queries and computed
    expressions that should always return sets with at least a single
    element, but where static cardinality inference is not capable
    enough or outright impossible.  An optional *message* named argument
    can be used to customize the error message.

    .. code-block:: edgeql-repl

        db> select assert_exists((select User filter .name = "Administrator"))
        {default::User {id: ...}}

        db> select assert_exists((select User filter .name = "Nonexistent"))
        ERROR: CardinalityViolationError: assert_exists violation: expression
               returned an empty set.

        db> select assert_exists(
        ...   (select User filter .name = "Nonexistent"),
        ...   message := "no users!"
        ... )
        ERROR: CardinalityViolationError: no users!

----------


.. eql:function:: std::count(s: set of anytype) -> int64

    :index: aggregate

    Return the number of elements in a set.

    .. code-block:: edgeql-repl

        db> select count({2, 3, 5});
        {3}

        db> select count(User);  # number of User objects in db
        {4}


----------


.. eql:function:: std::sum(s: set of int32) -> int64
                  std::sum(s: set of int64) -> int64
                  std::sum(s: set of float32) -> float32
                  std::sum(s: set of float64) -> float64
                  std::sum(s: set of bigint) -> bigint
                  std::sum(s: set of decimal) -> decimal

    :index: aggregate

    Return the sum of the set of numbers.

    The result type depends on the input set type. The general rule is
    that the type of the input set is preserved (as if a simple
    :eql:op:`+<plus>` was used) while trying to reduce the chance of
    an overflow (so all integers produce :eql:type:`int64` sum).

    .. code-block:: edgeql-repl

        db> select sum({2, 3, 5});
        {10}

        db> select sum({0.2, 0.3, 0.5});
        {1.0}


----------


.. eql:function:: std::all(values: set of bool) -> bool

    :index: aggregate

    Generalized boolean :eql:op:`and` applied to the set of *values*.

    The result is ``true`` if all of the *values* are ``true`` or the
    set of *values* is ``{}``. Return ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select all(<bool>{});
        {true}

        db> select all({1, 2, 3, 4} < 4);
        {false}


----------


.. eql:function:: std::any(values: set of bool) -> bool

    :index: aggregate

    Generalized boolean :eql:op:`or` applied to the set of *values*.

    The result is ``true`` if any of the *values* are ``true``. Return
    ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select any(<bool>{});
        {false}

        db> select any({1, 2, 3, 4} < 4);
        {true}


----------


.. eql:function:: std::enumerate(values: set of anytype) -> \
                  set of tuple<int64, anytype>

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

        db> select enumerate({2, 3, 5});
        {(1, 3), (0, 2), (2, 5)}

    .. code-block:: edgeql-repl

        db> select enumerate(User.name);
        {(0, 'Alice'), (1, 'Bob'), (2, 'Dave')}


----------


.. eql:function:: std::min(values: set of anytype) -> optional anytype

    :index: aggregate

    Return the smallest value of the input set.

    .. code-block:: edgeql-repl

        db> select min({-1, 100});
        {-1}


----------


.. eql:function:: std::max(values: set of anytype) -> optional anytype

    :index: aggregate

    Return the greatest value of the input set.

    .. code-block:: edgeql-repl

        db> select max({-1, 100});
        {100}
