.. _ref_std_type:


=====
Types
=====

:edb-alt-title: Type Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`is type <is>`
      - :eql:op-desc:`is`

    * - :eql:op:`type | type <typeor>`
      - :eql:op-desc:`typeor`

    * - :eql:op:`<type> val <cast>`
      - :eql:op-desc:`cast`

    * - :eql:op:`typeof anytype <typeof>`
      - :eql:op-desc:`typeof`

    * - :eql:op:`introspect type <introspect>`
      - :eql:op-desc:`introspect`


Finding an object's type
------------------------

You can find the type of an object via that object's ``__type__`` link, which
carries various information about the object's type, including the type's
``name``.

.. code-block:: edgeql-repl

    db> select <json>Person {
    ...  __type__: {
    ...    name
    ...    }
    ...  } limit 1;
    {Json("{\"__type__\": {\"name\": \"default::Villain\"}}")}

This information can be pulled into the top level by assigning a name to
the ``name`` property inside ``__type__``:

.. code-block:: edgeql-repl

    db> select <json>Person { type := .__type__.name } limit 1;
    {Json("{\"type\": \"default::Villain\"}")}

.. note::

    There's nothing magical about the ``__type__`` link: it's a standard link
    that exists on every object due to their inheritance from
    :eql:type:`BaseObject`, linking to the current object's type.

----------


.. eql:operator:: is: anytype is type -> bool
                      anytype is not type -> bool

    Type checking operator.

    Check if ``A`` is an instance of ``B`` or any of ``B``'s subtypes.

    Type-checking operators ``is`` and ``is not`` that
    test whether the left operand is of any of the types given by the
    comma-separated list of types provided as the right operand.

    Note that ``B`` is special and is not any kind of expression, so
    it does not in any way participate in the interactions of sets and
    longest common prefix rules.

    .. code-block:: edgeql-repl

        db> select 1 is int64;
        {true}

        db> select User is not SystemUser
        ... filter User.name = 'Alice';
        {true}

        db> select User is (Text | Named);
        {true, ..., true}  # one for every user instance


----------


.. eql:operator:: typeor: type | type -> type

    :index: poly polymorphism polymorphic queries nested shapes

    Type union operator.

    This operator is only valid in contexts where type checking is
    done. The most obvious use case is with the :eql:op:`is` and
    :eql:op:`is not <is>`. The operator allows to refer to a union of
    types in order to check whether a value is of any of these
    types.

    .. code-block:: edgeql-repl

        db> select User is (Text | Named);
        {true, ..., true}  # one for every user instance

    It can similarly be used when specifying a link target type. The
    same logic then applies: in order to be a valid link target the
    object must satisfy ``object is (A | B | C)``.

    .. code-block:: sdl
        :version-lt: 3.0

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

    .. code-block:: sdl

        abstract type Named {
            required name: str;
        }

        abstract type Text {
            required body: str;
        }

        type Item extending Named;

        type Note extending Text;

        type User extending Named {
            multi stuff: Named | Text;
        }

    With the above schema, the following would be valid:

    .. code-block:: edgeql-repl

        db> insert Item {name := 'cube'};
        {Object { id: <uuid>'...' }}
        db> insert Note {body := 'some reminder'};
        {Object { id: <uuid>'...' }}
        db> insert User {
        ...     name := 'Alice',
        ...     stuff := Note,  # all the notes
        ... };
        {Object { id: <uuid>'...' }}
        db> insert User {
        ...     name := 'Bob',
        ...     stuff := Item,  # all the items
        ... };
        {Object { id: <uuid>'...' }}
        db> select User {
        ...     name,
        ...     stuff: {
        ...         [is Named].name,
        ...         [is Text].body
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


.. eql:operator:: cast: < type > anytype -> anytype

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
    :eql:stmt:`insert`. However, the :eql:op:`type intersection
    <isintersect>` can be used to achieve an effect similar to
    casting for Objects.

    When a cast is applied to an expression of a known type, it represents a
    run-time type conversion. The cast will succeed only if a suitable type
    conversion operation has been defined.

    Examples:

    .. code-block:: edgeql-repl

        db> # cast a string literal into an integer
        ... select <int64>"42";
        {42}

        db> # cast an array of integers into an array of str
        ... select <array<str>>[1, 2, 3];
        {['1', '2', '3']}

        db> # cast an issue number into a string
        ... select <str>example::Issue.number;
        {'142'}

    Casts also work for converting tuples or declaring different tuple
    element names for convenience.

    .. code-block:: edgeql-repl

        db> select <tuple<int64, str>>(1, 3);
        {[1, '3']}

        db> with
        ...     # a test tuple set, that could be a result of
        ...     # some other computation
        ...     stuff := (1, 'foo', 42)
        ... select (
        ...     # cast the tuple into something more convenient
        ...     <tuple<a: int64, name: str, b: int64>>stuff
        ... ).name;  # access the 'name' element
        {'foo'}


    An important use of *casting* is in defining the type of an empty
    set ``{}``, which can be required for purposes of type disambiguation.

    .. code-block:: edgeql

        with module example
        select Text {
            name :=
                Text[is Issue].name if Text is Issue else
                <str>{},
                # the cast to str is necessary here, because
                # the type of the computed expression must be
                # defined
            body,
        };

    Casting empty sets is also the only situation where casting into an
    :eql:type:`Object` is valid:

    .. code-block:: edgeql

        with module example
        select User {
            name,
            friends := <User>{}
            # the cast is the only way to indicate that the
            # computed link 'friends' is supposed to refer to
            # a set of Users
        };

    For more information about casting between different types consult
    the :ref:`casting table <ref_eql_casts_table>`.


-----------


.. eql:operator:: typeof: typeof anytype -> type

    :index: type introspect introspection

    Static type inference operator.

    This operator converts an expression into a type, which can be
    used with :eql:op:`is`, :eql:op:`is not<is>`, and
    :eql:op:`introspect`.

    Currently, ``typeof`` operator only supports :ref:`scalars
    <ref_datamodel_scalar_types>` and :ref:`objects
    <ref_datamodel_object_types>`, but **not** the :ref:`collections
    <ref_datamodel_collection_types>` as a valid operand.

    Consider the following types using links and properties with names
    that don't indicate their respective target types:

    .. code-block:: sdl
        :version-lt: 3.0

        type Foo {
            property bar -> int16;
            link baz -> Bar;
        }

        type Bar extending Foo;

    .. code-block:: sdl

        type Foo {
            bar: int16;
            baz: Bar;
        }

        type Bar extending Foo;

    We can use ``typeof`` to determine if certain expression has the
    same type as the property ``bar``:

    .. code-block:: edgeql-repl

        db> insert Foo { bar := 1 };
        {Object { id: <uuid>'...' }}
        db> select (Foo.bar / 2) is typeof Foo.bar;
        {false}

    To determine the actual resulting type of an expression we can
    use :eql:op:`introspect`:

    .. code-block:: edgeql-repl

        db> select introspect (typeof Foo.bar).name;
        {'std::int16'}
        db> select introspect (typeof (Foo.bar / 2)).name;
        {'std::float64'}

    Similarly, we can use ``typeof`` to discriminate between the
    different ``Foo`` objects that can and cannot be targets of link
    ``baz``:

    .. code-block:: edgeql-repl

        db> insert Bar { bar := 2 };
        {Object { id: <uuid>'...' }}
        db> select Foo {
        ...     bar,
        ...     can_be_baz := Foo is typeof Foo.baz
        ... };
        {
            Object { bar: 1, can_be_baz: false },
            Object { bar: 2, can_be_baz: true }
        }


-----------


.. eql:operator:: introspect: introspect type -> schema::Type

    :index: type typeof introspection

    Static type introspection operator.

    This operator returns the :ref:`introspection type
    <ref_datamodel_introspection>` corresponding to type provided as
    operand. It works well in combination with :eql:op:`typeof`.

    Currently, the ``introspect`` operator only supports :ref:`scalar
    types <ref_datamodel_scalar_types>` and :ref:`object types
    <ref_datamodel_object_types>`, but **not** the :ref:`collection
    types <ref_datamodel_collection_types>` as a valid operand.

    Consider the following types using links and properties with names
    that don't indicate their respective target types:

    .. code-block:: sdl
        :version-lt: 3.0

        type Foo {
            property bar -> int16;
            link baz -> Bar;
        }

        type Bar extending Foo;

    .. code-block:: sdl

        type Foo {
            bar: int16;
            baz: Bar;
        }

        type Bar extending Foo;

    .. code-block:: edgeql-repl

        db> select (introspect int16).name;
        {'std::int16'}
        db> select (introspect Foo).name;
        {'default::Foo'}
        db> select (introspect typeof Foo.bar).name;
        {'std::int16'}

    .. note::

        For any :ref:`object type <ref_datamodel_object_types>`
        ``SomeType`` the expressions ``introspect SomeType`` and
        ``introspect typeof SomeType`` are equivalent as the object
        type name is syntactically identical to the *expression*
        denoting the set of those objects.

    There's an important difference between the combination of
    ``introspect typeof SomeType`` and ``SomeType.__type__``
    expressions when used with objects. ``introspect typeof SomeType``
    is statically evaluated and does not take in consideration the
    actual objects contained in the ``SomeType`` set. Conversely,
    ``SomeType.__type__`` is the actual set of all the types reachable
    from all the ``SomeType`` objects. Due to inheritance statically
    inferred types and actual types may not be the same (although the
    actual types will always be a subtype of the statically inferred
    types):

    .. code-block:: edgeql-repl

        db> # first let's make sure we don't have any Foo objects
        ... delete Foo;
        { there may be some deleted objects here }
        db> select (introspect typeof Foo).name;
        {'default::Foo'}
        db> select Foo.__type__.name;
        {}
        db> # let's add an object of type Foo
        ... insert Foo;
        {Object { id: <uuid>'...' }}
        db> # Bar is also of type Foo
        ... insert Bar;
        {Object { id: <uuid>'...' }}
        db> select (introspect typeof Foo).name;
        {'default::Foo'}
        db> select Foo.__type__.name;
        {'default::Bar', 'default::Foo'}
