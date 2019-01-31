.. _ref_eql_operators_type:

================
Type Expressions
================

This section describes EdgeQL expressions related to *types*.


IS
==

.. eql:operator:: IS: A IS B or A IS NOT B

    :optype A: anytype
    :optype B: type
    :resulttype: bool

    Check if ``A`` is an instance of ``B`` or any of ``B``'s subtypes.

    Type-checking operators :eql:op:`IS` and :eql:op:`IS NOT<IS>` that
    test whether the left operand is of any of the types given by the
    comma-separated list of types provided as the right operand.

    Note that ``B`` is special and is not any kind of expression, so
    it does not in any way participate in the interactions of sets and
    longest common prefix rules.

    .. code-block:: edgeql-repl

        db> SELECT 1 IS int64;
        {True}

        db> SELECT User IS NOT SystemUser
        ... FILTER User.name = 'Alice';
        {True}

        db> SELECT User IS (Text, Named);
        {True, ..., True}  # one for every user instance


.. _ref_eql_expr_typecast:

Type Cast Expression
====================

A type cast expression converts the specified value to another value of
the specified type:

.. eql:synopsis::

    "<" <type> ">" <expression>

The *type* must be a non-abstract scalar or a container type.

Type cast is a run-time operation.  The cast will succeed only if a
type conversion was defined for the type pair, and if the source value
satisfies the requirements of a target type. EdgeDB allows casting any
scalar.

It is illegal to cast one :eql:type:`Object` into another. The only
way to construct a new :eql:type:`Object` is by using :ref:`INSERT
<ref_eql_statements_insert>`. However, the :ref:`target filter
<ref_eql_expr_paths_is>` can be used to achieve an effect similar to
casting for Objects.

When a cast is applied to an expression of a known type, it represents a
run-time type conversion. The cast will succeed only if a suitable type
conversion operation has been defined.

Examples:

.. code-block:: edgeql-repl

    # cast a string literal into an integer
    db> SELECT <int64>"42";
    {42}

    # cast an array of integers into an array of str
    db> SELECT <array<str>>[1, 2, 3];
    {['1', '2', '3']}

    # cast an issue number into a string
    db> SELECT <str>example::Issue.number;
    {'142'}

Casts also work for converting tuples or declaring different tuple
element names for convenience.

.. code-block:: edgeql-repl

    db> SELECT <tuple<int64, str>>(1, 3);
    {[1, '3']}

    db> WITH
    ...     # a test tuple set, that could be a result of
    ...     # some other computation
    ...     stuff := (1, 'foo', 42)
    ... SELECT (
    ...     # cast the tuple into something more convenient
    ...     <tuple<a: int64, name: str, b: int64>>stuff
    ... ).name;  # access the 'name' element
    {'foo'}


An important use of *casting* is in defining the type of an empty
set ``{}``, which can be required for purposes of type disambiguation.

.. code-block:: edgeql

    WITH MODULE example
    SELECT Text {
        name :=
            Text[IS Issue].name IF Text IS Issue ELSE
            <str>{},
            # the cast to str is necessary here, because
            # the type of the computable must be defined
        body,
    };

Casting empty sets is also the only situation where casting into an
:eql:type:`Object` is valid:

.. code-block:: edgeql

    WITH MODULE example
    SELECT User {
        name,
        friends := <User>{}
        # the cast is the only way to indicate that the
        # computable 'friends' is supposed to be a set of
        # Users
    };
