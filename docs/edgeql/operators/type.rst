.. _ref_eql_operators_type:

================
Type Expressions
================

This section describes EdgeQL expressions related to *types*. Some of
these expressions produce :ref:`introspection schema objects
<ref_datamodel_modules_schema>`.

Consider the following schema describing a few types used in an image
creation and critiquing system. Some of the code examples in this
section will refer to these types:

.. code-block:: eschema

    type Named:
        required property name -> str

    type User extending Named

    type AdminUser extending User

    abstract type Text:
        required property body -> str

    abstract type Authored:
        required link author -> User:
            constraint exclusive

    type Comment extending Text, Authored:
        property rating -> int64

    type Image extending Authored, Named:
        required property data -> bytes

    type Gallery:
        # something hand-picked by the resource administrators
        required link curator -> AdminUser
        required multi link content -> Authored

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

        db> SELECT User IS NOT AdminUser
        ... FILTER User.name = 'Alice';
        {True}

.. eql:operator:: TYPEUNION: A | B

    :optype A: object type
    :optype B: object type
    :resulttype: union type
    :index: union type

    Create a *union type* from types ``A`` and ``B``.

    A simple example of a *union type* is the type of a set resulting
    from a :eql:op:`UNION` of two or more unrelated object types. This
    expression can only be used on the right-hand-side of
    :eql:op:`IS` or as the :ref:`concrete link
    <ref_datamodel_links_concrete>` target type.

    Consider the *union type* ``User | Authored``. Every ``Image`` is of
    that *union type* (because ``Image`` extends ``Authored``):

    .. code-block:: edgeql-repl

        db> SELECT Image IS (User | Authored);
        {True, ..., True}

    In the example schema, every ``Named`` object happens to be either
    a ``User`` or an ``Image``. Which means that these objects would
    also be of *union type* ``User | Authored``:

    .. code-block:: edgeql-repl

        db> SELECT Named IS (User | Authored);
        {True, ..., True}

    All objects in the ``User UNION Comments`` are of the *union type*
    ``User | Authored``:

    .. code-block:: edgeql-repl

        db> SELECT (User UNION Comments) IS (User | Authored);
        {True, ..., True}

    For an example of using a *union type* as the target of a concrete
    link, consider the following:

    .. code-block:: eschema

        # some sort of report regarding users
        type UserReport extending Text:
            # the user and comments that are relevant for the report
            required multi link relevant_user_data -> User | Comment

.. eql:operator:: TYPEOF: TYPEOF A

    :optype A: anytype
    :resulttype: type
    :index: type

    Get the type of an expression.

    This operation produces a statically-inferred type of an
    expression. The resulting type then behaves just like an
    explicitly specified type. Currently, this expression is only
    supported as the right-hand-side of :eql:op:`IS` or as the
    argument of :eql:op:`INTROSPECT`.

    .. code-block:: edgeql-repl

        db> SELECT AdminUser IS TYPEOF Comment.author LIMIT 1;
        {True}

        db> SELECT 42 IS TYPEOF User.name;
        {False}


.. eql:operator:: INTROSPECT: INTROSPECT A

    :optype A: type
    :resulttype: schema::Type
    :index: type introspect introspection

    Get the ``schema::Type`` object corresponding to a given type.

    The result of this expression is the schema object corresponding to the
    :eql:type:`schema::ObjectType` or :eql:type:`schema::ScalarType`
    provided as an argument.

    .. code-block:: edgeql-repl

        db> SELECT INTROSPECT str { name };
        {Object { name: 'std::str' }}

        db> SELECT INTROSPECT User { name };
        {Object { name: 'example::User' }}

    This operator can be combined with :eql:op:`TYPEOF` to produce an
    effect similar to accessing :eql:type:`__type__ <Object>`. However, both
    :eql:op:`INTROSPECT` and :eql:op:`TYPEOF` are statically
    evaluated, whereas the link ``__type__`` provides run-time
    type information.

    .. code-block:: edgeql-repl

        db> SELECT (INTROSPECT TYPEOF Gallery.content) {
        ...     name,
        ...     is_abstract
        ... };
        {Object { name: 'example::Authored', is_abstract: true }}

        db> SELECT Gallery.content.__type__ {
        ...     name,
        ...     is_abstract
        ... };
        {
            Object { name: 'example::Comment', is_abstract: false },
            Object { name: 'example::Image', is_abstract: false }
        }

    Note that the latter query necessarily produces the types of
    concrete objects, extending the *abstract* type ``Authored``,
    since that is the target type of the multi link ``content``.


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

    # cast a rating from a comment into a string
    db> SELECT <str>example::Comment.rating LIMIT 1;
    {'2'}

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


An important use of *casting* is in defining the type of an empty set
``{}``, which can be required for purposes of type disambiguation.
Especially in numeric calculations the type of an empty set can
significantly affect the result. Consider a summation of a set to the
result of which two large integers are added. This computation could
overflow depending on the integers involved and the type of the set
used in :eql:func:`sum`:

.. code-block:: edgeql-repl

    db> SELECT sum(<int64>{}) +
    ...     4000000000000000000 + 6000000000000000000;
    NumericOutOfRangeError: std::int64 out of range

    db> SELECT sum(<float64>{}) +
    ...     4000000000000000000 + 6000000000000000000;
    {1e+19}


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
