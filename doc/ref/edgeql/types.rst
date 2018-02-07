.. _ref_edgeql_types:

Types
=====

Objects and classes
-------------------

Every object in EdgeDB has a class. The information about the object's
class is reachable through a special link ``__class__``. It can be
used to retrieve the class name, for example.

.. code-block:: eql

    # retrieve the class name for the concept User
    SELECT example::User.__class__.name LIMIT 1;

    # get all the Text objects together with the name of the specific
    # class they belong to
    WITH MODULE example
    SELECT Text {
        body,
        __class__: {
            name
        }
    };

To determine if an object is of a specific class, the operator ``IS``
can be used. The left operand is the object to be tested and the right
operand is the name of the class to test against.

.. code-block:: eql

    WITH MODULE example
    SELECT Text {
        body
    }
    FILTER Text IS Issue;

    # this query is equivalent to the one above
    WITH MODULE example
    SELECT Issue {
        body
    };

It is also possible to specify several classes as the right operand,
in which case the result is ``True`` if the left operand matches at
least one of the classes on the right.

.. code-block:: eql

    # get all comments and issues (but not log entries)
    WITH MODULE example
    SELECT Text {
        body
    }
    FILTER Text IS (Comment, Issue);

According to the example schema both ``Issue`` and ``Comment`` are
:ref:`derived<ref_schema_architechture_inheritance>` from ``Text``.
There's ``LogEntry`` that's also derived from ``Text``, but it
will be filtered out by the query above.


.. _ref_edgeql_types_nonobjects:

Non-objects
-----------

There are several types of non-objects in EdgeDB: atomic values,
arrays and tuples.


Atomic values
~~~~~~~~~~~~~

Since *atoms* are actually defined in the schema, atomic values also
have a ``__class__`` link that can be accessed to retrieve the
information about the *atom*, much like for objects and *concepts*.

Arrays
~~~~~~

Arrays are homogeneous ordered collections. Something can be an array
element if and only if it can be a set element. At the moment only
one-dimensional arrays are supported in EdgeDB. Array indexing starts
at 0.

Arrays support indexing and slicing operators:

.. code-block:: eql

    SELECT [1, 2, 3];
    # this will return [[1, 2, 3]]

    WITH
        # define an array for testing
        arr := [1, 2, 3]
    SELECT
        # select the element at index 1
        arr[1];
    # this will return [2]

    WITH
        # define an array for testing
        arr := [1, 2, 3]
    SELECT
        # select the slice from
        # 1 (inclusive) to 3 (exclusive)
        arr[1:3];
    # this will return [2, 3]

Another way of creating an array is to use ``array_agg`` built-in,
which converts a set into an array. If the ordering is important the
``ORDER`` clause must be specified for the set, otherwise no specific
ordering guarantee can be made for the ``array_agg`` aggregate
function:

.. code-block:: eql

    WITH MODULE example
    SELECT array_agg(
        (SELECT User ORDER BY User.name)
    );


Associative arrays
~~~~~~~~~~~~~~~~~~

Associative arrays are indexed homogeneous collections, where the
indexes are arbitrary but must be all of the same type. Values don't
have to be the same type as indexes, but they must still be the same
type as each other. No specific ordering of a map is assumed or
guaranteed, thus slicing operators are not available for them.

.. code-block:: eql

    SELECT ['a' -> 1, 'b' -> 2, 'c' -> 3];
    # this will return [{'a': 1, 'b': 2, 'c': 3}]

    WITH
        # define a map for testing
        map := ['a' -> 1, 'b' -> 2, 'c' -> 3]
    SELECT
        # select the element at index 'b'
        map['b'];
    # this will return [2]


.. _ref_edgeql_types_tuples:

Tuples
~~~~~~

Tuples are heterogeneous opaque entities, composed of objects or
non-objects and have implicit ordering of their components. Something
can be a tuple element if and only if it can be a set element. Two
tuples are equal if all of their components are equal and in the same
order.

.. code-block:: eql

    # a simple 2-tuple made of a str and int
    SELECT ('foo', 42);

    WITH
        # define a tuple for testing
        tup := ('foo', 42)
    SELECT
        # select the first element of the tuple
        tup.0;
    # returns ['foo']

    WITH
        tup := ('foo', 42)
    SELECT
        # create a new 2-tuple reversing the elements
        (tup.1, tup.0);
    # returns [[42, 'foo']]

    WITH
        tup := ('foo', 42)
    SELECT
        # compare 2 tuples
        tup = ('foo', 42);
    # returns [True]


Tuple elements can be *named*, however this does not in any way affect
the ordering of these elements within the tuple. The names are used
for convenience to make it easier to refer to different elements as
well as in tuple serialization. Unlike for maps identifiers only valid
identifiers can be used to name tuple elements.

.. code-block:: eql

    # a simple named 2-tuple made of a str and int
    SELECT (a := 'foo', b := 42);

    WITH
        # define a tuple for testing
        tup := (a := 'foo', b := 42)
    SELECT
        # select the element of the tuple denoted by 'a'
        tup.a;
    # returns ['foo']

    WITH
        tup := (a := 'foo', b := 42)
    SELECT
        # compare 2 tuples
        tup = ('foo', 42);
    # returns [True]

    WITH
        tup := (a := 'foo', b := 42)
    SELECT
        # compare 2 tuples
        tup = (b := 42, a := 'foo');
    # returns [False] because the ordering of
    # the tuple elements is different

    WITH
        tup1 := (a := 'foo', b := 42),
        tup2 := (b := 42, a := 'foo')
    SELECT
        # compare tuple elements
        (tup1.a = tup2.a, tup1.b = tup1.b);
    # returns [[True, True]]

It is possible to nest arrays and tuples within each other:

.. code-block:: eql

    # array of 3-tuples
    SELECT [
        # where each tuple has:
        (
            # str,
            'foo',
            # array of int,
            [1, 2],
            # tuple (int, int) as elements
            (3, 5),
        ),
        (
            'bar',
            [100, 200, 9001],
            (-2, 4),
        ),
    ];

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
*all* Issues, then :ref:`coalescing<ref_edgeql_expressions_coalesce>`
or a :ref:`shape<ref_edgeql_shapes>` query should be used instead.

On the other hand the following query will include *all* Issues,
because the tuple elements are made from the set of Issues and the set
produced by the aggregator function ``array_agg``, which is never
``{}``:

.. code-block:: eql

    WITH MODULE example
    SELECT (Issue, array_agg(Issue.priority.name));

All of the above works the same way for arrays.


.. _ref_edgeql_types_casts:

Casts
-----

Sometimes it is necessary to convert data from one type to another.
This is called *casting*. In order to *cast* one expression into a
different type the expression is prefixed with the ``<new_type>``,
as follows:

.. code-block:: eql

    # cast a string literal into an integer
    SELECT <int>"42";

    # cast an array of integers into an array of str
    SELECT <array<str>>[1, 2 , 3];

    # suppose that all the issue numbers are actually valid integers
    # despite being defined as str
    SELECT <int>example::Issue.number;

Casts also work for converting tuples or declaring different tuple
element names for convenience.

.. code-block:: eql

    SELECT <tuple<int, str>>(1, 3);
    # returns [[1, '3']]

    WITH
        # a test tuple set, that could be a result of
        # some other computation
        stuff := (1, 'foo', 42)
    SELECT (
        # cast the tuple into something more convenient
        <tuple<a: int, name: str, b: int>>stuff
    ).name;  # access the 'name' element

An important use of *casting* is in defining the type of an empty
set ``{}``, which can be required for purposes of type disambiguation.

.. code-block:: eql

    WITH MODULE example
    SELECT Text {
        name :=
            Text[IS Issue].name IF Text IS Issue ELSE
            <str>{},
            # the cast to str is necessary here, because
            # the type of the computable must be defined
        body,
    };


Class filtering in paths
------------------------

It is possible to restrict any path (or path-like expression) to only
a subset of all of the possible objects that it describes by
restricting the class of the target objects by using ``[IS Concept]``.
For example, consider the path that starts with ``User`` and follows
the ``owner`` link backwards. There are potentially many
``OwnedObjects`` that is can refer to, so in order to only get
``Issues`` owned by a user the path filter can be used:

.. code-block:: eql

    WITH MODULE example
    SELECT User.<owner[IS Issue]
    FILTER User.name = 'Alice';

This feature makes it possible to traverse links in paths in any
direction conveniently without the use of a more bulky ``FILTER``
clause.

The same filtering operator can be used when it is necessary to refer
to the attributes that exist only in the descendant classes (like
``number``, that only those ``Text`` objects that are actually
``Issues`` would have). The expression ``Text[IS Issue]`` evaluates to
an empty set if for all ``Text`` objects that are not of class
``Issue`` and it evaluates to the object itself if it is an ``Issue``.
Importantly this syntactical construct allows to refer to links that
only exist on Issue.

.. code-block:: eql

    WITH MODULE example
    SELECT Text {
        body,
        Issue.number
    }
    FILTER
        # material implication
        # "if text is an issue, then it must have specific number"
        Text IS NOT Issue
        OR
        Text[IS Issue].number = '42';
