.. _ref_eql_operators_type:


=====
Types
=====

:edb-alt-title: Type Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`IS type <IS>`
      - :eql:op-desc:`IS`

    * - :eql:op:`<type> val <CAST>`
      - :eql:op-desc:`CAST`

    * - :eql:op:`TYPEOF anytype <TYPEOF>`
      - :eql:op-desc:`TYPEOF`

    * - :eql:op:`INTROSPECT type <INTROSPECT>`
      - :eql:op-desc:`INTROSPECT`


----------


.. eql:operator:: IS: anytype IS type -> bool
                      anytype IS NOT type -> bool

    Type checking operator

    Check if ``A`` is an instance of ``B`` or any of ``B``'s subtypes.

    Type-checking operators :eql:op:`IS` and :eql:op:`IS NOT<IS>` that
    test whether the left operand is of any of the types given by the
    comma-separated list of types provided as the right operand.

    Note that ``B`` is special and is not any kind of expression, so
    it does not in any way participate in the interactions of sets and
    longest common prefix rules.

    .. code-block:: edgeql-repl

        db> SELECT 1 IS int64;
        {true}

        db> SELECT User IS NOT SystemUser
        ... FILTER User.name = 'Alice';
        {true}

        db> SELECT User IS (Text | Named);
        {true, ..., true}  # one for every user instance


----------


.. eql:operator:: TYPEOR: type | type -> type

    :index: poly polymorphism polymorphic queries nested shapes

    Type union operator

    This operator is only valid in contexts where type checking is
    done. The most obvious use case is with the :eql:op:`IS` and
    :eql:op:`IS NOT<IS>`. The operator allows to refer to a union of
    types in order to check whether a value is of any of these
    types.

    .. code-block:: edgeql-repl

        db> SELECT User IS (Text | Named);
        {true, ..., true}  # one for every user instance

    It can similarly be used when specifying a link target type. The
    same logic then applies: in order to be a valid link target the
    object must satisfy ``object IS (A | B | C)``.

    .. code-block:: sdl

        abstract type Named {
            required property name -> str;
        }

        abstract type Text {
            required property body -> str;
        }

        type Item extending Named;

        type Note extending Text;

        type User extending Named {
            multi link stuff -> Named | Text;
        }

    With the above schema, the following would be valid:

    .. code-block:: edgeql-repl

        db> INSERT Item {name := 'cube'};
        {Object { id: <uuid>'...' }}
        db> INSERT Note {body := 'some reminder'};
        {Object { id: <uuid>'...' }}
        db> INSERT User {
        ...     name := 'Alice',
        ...     stuff := Note,  # all the notes
        ... };
        {Object { id: <uuid>'...' }}
        db> INSERT User {
        ...     name := 'Bob',
        ...     stuff := Item,  # all the items
        ... };
        {Object { id: <uuid>'...' }}
        db> SELECT User {
        ...     name,
        ...     stuff: {
        ...         [IS Named].name,
        ...         [IS Text].body
        ...     }
        ... };
        {
            Object {
                name: 'Alice',
                stuff: {Object { name: {}, body: 'some reminder' }}
            },
            Object {
                name: 'Bob',
                stuff: {Object { name: 'cube', body: {} }}
            }
        }


-----------


.. eql:operator:: CAST: < type > anytype -> anytype

    Type cast operator.

    A type cast operator converts the specified value to another value of
    the specified type:

    .. eql:synopsis::

        "<" <type> ">" <expression>

    The :eql:synopsis:`<type>` must be a valid :ref:`type expression
    <ref_eql_types>` denoting a non-abstract scalar or a container type.

    Type cast is a run-time operation.  The cast will succeed only if a
    type conversion was defined for the type pair, and if the source value
    satisfies the requirements of a target type. EdgeDB allows casting any
    scalar.

    It is illegal to cast one :eql:type:`Object` into another. The
    only way to construct a new :eql:type:`Object` is by using
    :ref:`INSERT <ref_eql_statements_insert>`. However, the
    :eql:op:`type intersection <ISINTERSECT>` can be used to achieve an
    effect similar to casting for Objects.

    When a cast is applied to an expression of a known type, it represents a
    run-time type conversion. The cast will succeed only if a suitable type
    conversion operation has been defined.

    Examples:

    .. code-block:: edgeql-repl

        db> # cast a string literal into an integer
        ... SELECT <int64>"42";
        {42}

        db> # cast an array of integers into an array of str
        ... SELECT <array<str>>[1, 2, 3];
        {['1', '2', '3']}

        db> # cast an issue number into a string
        ... SELECT <str>example::Issue.number;
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


-----------


.. eql:operator:: TYPEOF: TYPEOF anytype -> type

    :index: type introspect introspection

    Static type inference operator.

    This operator converts an expression into a type, which can be
    used with :eql:op:`IS`, :eql:op:`IS NOT<IS>`, and
    :eql:op:`INTROSPECT`.

    Currently, ``TYPEOF`` operator only supports :ref:`scalars
    <ref_datamodel_scalar_types>` and :ref:`objects
    <ref_datamodel_object_types>`, but **not** the :ref:`collections
    <ref_datamodel_collection_types>` as a valid operand.

    Consider the following types using links and properties with names
    that don't indicate their respective target types:

    .. code-block:: sdl

        type Foo {
            property bar -> int16;
            link baz -> Bar;
        }

        type Bar extending Foo;

    We can use ``TYPEOF`` to determine if certain expression has the
    same type as the property ``bar``:

    .. code-block:: edgeql-repl

        db> INSERT Foo { bar := 1 };
        {Object { id: <uuid>'...' }}
        db> SELECT (Foo.bar / 2) IS TYPEOF Foo.bar;
        {false}

    To determine what is the actual resulting type of an expression we
    can use :eql:op:`INTROSPECT`:

    .. code-block:: edgeql-repl

        db> SELECT INTROSPECT (TYPEOF Foo.bar).name;
        {'std::int16'}
        db> SELECT INTROSPECT (TYPEOF (Foo.bar / 2)).name;
        {'std::float64'}

    Similarly, we can use ``TYPEOF`` to discriminate between the
    different ``Foo`` objects that can and cannot be targets of link
    ``baz``:

    .. code-block:: edgeql-repl

        db> INSERT Bar { bar := 2 };
        {Object { id: <uuid>'...' }}
        db> SELECT Foo {
        ...     bar,
        ...     can_be_baz := Foo IS TYPEOF Foo.baz
        ... };
        {
            Object { bar: 1, can_be_baz: false },
            Object { bar: 2, can_be_baz: true }
        }


-----------


.. eql:operator:: INTROSPECT: INTROSPECT type -> schema::Type

    :index: type typeof introspection

    Static type introspection operator.

    This operator returns the :ref:`introspection type
    <ref_eql_introspection>` corresponding to type provided as
    operand. It works well in combination with :eql:op:`TYPEOF`.

    Currently, ``INTROSPECT`` operator only supports :ref:`scalar
    types <ref_datamodel_scalar_types>` and :ref:`object types
    <ref_datamodel_object_types>`, but **not** the :ref:`collection
    types <ref_datamodel_collection_types>` as a valid operand.

    Consider the following types using links and properties with names
    that don't indicate their respective target types:

    .. code-block:: sdl

        type Foo {
            property bar -> int16;
            link baz -> Bar;
        }

        type Bar extending Foo;

    .. code-block:: edgeql-repl

        db> SELECT (INTROSPECT int16).name;
        {'std::int16'}
        db> SELECT (INTROSPECT Foo).name;
        {'default::Foo'}
        db> SELECT (INTROSPECT TYPEOF Foo.bar).name;
        {'std::int16'}

    .. note::

        For any :ref:`object type <ref_datamodel_object_types>`
        ``SomeType`` the expressions ``INTROSPECT SomeType`` and
        ``INTROSPECT TYPEOF SomeType`` are equivalent as the object
        type name is syntactically identical to the *expression*
        denoting the set of those objects.

    There's an important difference between the combination of
    ``INTROSPECT TYPEOF SomeType`` and ``SomeType.__type__``
    expressions when used with objects. ``INTROSPECT TYPEOF SomeType``
    is statically evaluated and does not take in consideration the
    actual objects contained in the ``SomeType`` set. Conversely,
    ``SomeType.__type__`` is the actual set of all the types reachable
    from all the ``SomeType`` objects. Due to inheritance statically
    inferred types and actual types may not be the same (although the
    actual types will always be a subtype of the statically inferred
    types):

    .. code-block:: edgeql-repl

        db> # first let's make sure we don't have any Foo objects
        ... DELETE Foo;
        { there may be some deleted objects here }
        db> SELECT (INTROSPECT TYPEOF Foo).name;
        {'default::Foo'}
        db> SELECT Foo.__type__.name;
        {}
        db> # let's add an object of type Foo
        ... INSERT Foo;
        {Object { id: <uuid>'...' }}
        db> # Bar is also of type Foo
        ... INSERT Bar;
        {Object { id: <uuid>'...' }}
        db> SELECT (INTROSPECT TYPEOF Foo).name;
        {'default::Foo'}
        db> SELECT Foo.__type__.name;
        {'default::Bar', 'default::Foo'}
