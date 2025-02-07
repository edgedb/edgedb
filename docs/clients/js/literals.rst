.. _edgedb-js-literals:


Literals
--------

The query builder provides a set of "helper functions" that convert JavaScript
literals into *expressions* that can be used in queries. For the most part,
these helper functions correspond to the *name* of the type.



Primitives
^^^^^^^^^^

Primitive literal expressions are created using constructor functions that
correspond to EdgeDB datatypes. Each expression below is accompanied by the
EdgeQL it produces.

.. code-block:: typescript

  e.str("asdf")            // "asdf"
  e.int64(123)             // 123
  e.float64(123.456)       // 123.456
  e.bool(true)             // true
  e.bigint(12345n)         // 12345n
  e.decimal("1234.1234n")  // 1234.1234n
  e.uuid("599236a4...")    // <uuid>"599236a4..."

  e.bytes(Uint8Array.from('binary data'));
  // b'binary data'

Strings
^^^^^^^

String expressions have some special functionality: they support indexing and
slicing, as in EdgeQL.

.. code-block:: typescript

  const myString = e.str("hello world");

  myString[5];         //  "hello world"[5]
  myString['2:5'];     //  "hello world"[2:5]
  myString[':5'];      //  "hello world"[:5]
  myString['2:'];      //  "hello world"[2:]

There are also equivalent ``.index`` and ``.slice`` methods that can accept
integer expressions as arguments.

.. code-block:: typescript

  const myString = e.str("hello world");
  const start = e.int64(2);
  const end = e.int64(5);

  myString.index(start);          //  "hello world"[2]
  myString.slice(start, end);     //  "hello world"[2:5]
  myString.slice(null, end);      //  "hello world"[:5]
  myString.slice(start, null);    //  "hello world"[2:]

Enums
^^^^^

Enum literals are available as properties defined on the enum type.

.. code-block:: typescript

  e.Colors.green;
  // Colors.green;

  e.sys.VersionStage.beta;
  // sys::VersionStage.beta

Dates and times
^^^^^^^^^^^^^^^

To create an instance of ``datetime``, pass a JavaScript ``Date`` object into
``e.datetime``:

.. code-block:: typescript

  e.datetime(new Date('1999-01-01'));
  // <datetime>'1999-01-01T00:00:00.000Z'

EdgeDB's other temporal datatypes don't have equivalents in the JavaScript
type system: ``duration``, ``cal::relative_duration``, ``cal::date_duration``,
``cal::local_date``, ``cal::local_time``, and ``cal::local_datetime``,

To resolve this, each of these datatypes can be represented with an instance
of a corresponding class, as defined in ``edgedb`` module. Clients use
these classes to represent these values in query results; they are documented
on the :ref:`Client API <edgedb-js-datatypes>` docs.

.. list-table::

  * - ``e.duration``
    - :js:class:`Duration`
  * - ``e.cal.relative_duration``
    - :js:class:`RelativeDuration`
  * - ``e.cal.date_duration``
    - :js:class:`DateDuration`
  * - ``e.cal.local_date``
    - :js:class:`LocalDate`
  * - ``e.cal.local_time``
    - :js:class:`LocalTime`
  * - ``e.cal.local_datetime``
    - :js:class:`LocalDateTime`
  * - ``e.cal.local_datetime``
    - :js:class:`LocalDateTime`
  * - ``e.cal.local_datetime``
    - :js:class:`LocalDateTime`

The code below demonstrates how to declare each kind of temporal literal,
along with the equivalent EdgeQL.

.. code-block:: typescript

  import * as edgedb from "edgedb";

  const myDuration = new edgedb.Duration(0, 0, 0, 0, 1, 2, 3);
  e.duration(myDuration);

  const myLocalDate = new edgedb.LocalDate(1776, 7, 4);
  e.cal.local_date(myLocalDate);

  const myLocalTime = new edgedb.LocalTime(13, 15, 0);
  e.cal.local_time(myLocalTime);

  const myLocalDateTime = new edgedb.LocalDateTime(1776, 7, 4, 13, 15, 0);
  e.cal.local_datetime(myLocalDateTime);


You can also declare these literals by casting an appropriately formatted
``str`` expression, as in EdgeQL. Casting :ref:`is documented
<ref_qb_casting>` in more detail later in the docs.

.. code-block:: typescript

  e.cast(e.duration, e.str('5 minutes'));
  // <std::duration>'5 minutes'

  e.cast(e.cal.local_datetime, e.str('1999-03-31T15:17:00'));
  // <cal::local_datetime>'1999-03-31T15:17:00'

  e.cast(e.cal.local_date, e.str('1999-03-31'));
  // <cal::local_date>'1999-03-31'

  e.cast(e.cal.local_time, e.str('15:17:00'));
  // <cal::local_time>'15:17:00'


JSON
^^^^

JSON literals are created with the ``e.json`` function. You can pass in any
EdgeDB-compatible data structure.


What does "EdgeDB-compatible" mean? It means any JavaScript data structure
with an equivalent in EdgeDB: strings, number, booleans, ``bigint``\ s,
``Uint8Array``\ s, ``Date``\ s, and instances of EdgeDB's built-in classes:
(``LocalDate`` ``LocalTime``, ``LocalDateTime``, ``DateDuration``,
``Duration``, and ``RelativeDuration``), and any array or object of these
types. Other JavaScript data structures like symbols, instances of custom
classes, sets, maps, and `typed arrays <https://developer.mozilla.org/en-US/
docs/Web/JavaScript/Typed_arrays>`_ are not supported.

.. code-block:: typescript

  const query = e.json({ name: "Billie" })
  // to_json('{"name": "Billie"}')

  const data = e.json({
    name: "Billie",
    numbers: [1,2,3],
    nested: { foo: "bar"},
    duration: new edgedb.Duration(1, 3, 3)
  })

JSON expressions support indexing, as in EdgeQL. The returned expression also
has a ``json`` type.

.. code-block:: typescript

  const query = e.json({ numbers: [0,1,2] });

  query.toEdgeQL(); // to_json((numbers := [0,1,2]))

  query.numbers[0].toEdgeQL();
  // to_json('{"numbers":[0,1,2]}')['numbers'][0]

.. Keep in mind that JSON expressions are represented as strings when returned from a query.

.. .. code-block:: typescript

..   await e.json({
..     name: "Billie",
..     numbers: [1,2,3]
..   }).run(client)
..   // => '{"name": "Billie", "numbers": [1, 2, 3]}';

The inferred type associated with a ``json`` expression is ``unknown``.

.. code-block:: typescript

  const result = await query.run(client)
  // unknown

Arrays
^^^^^^

Declare array expressions by passing an array of expressions into ``e.array``.

.. code-block:: typescript

  e.array([e.str("a"), e.str("b"), e.str("b")]);
  // ["a", "b", "c"]

EdgeQL semantics are enforced by TypeScript, so arrays can't contain elements
with incompatible types.

.. code-block:: typescript

  e.array([e.int64(5), e.str("foo")]);
  // TypeError!

For convenience, the ``e.array`` can also accept arrays of plain JavaScript
data as well.

.. code-block:: typescript

  e.array(['a', 'b', 'c']);
  // ['a', 'b', 'c']

  // you can intermixing expressions and plain data
  e.array([1, 2, e.int64(3)]);
  // [1, 2, 3]

Array expressions also support indexing and slicing operations.

.. code-block:: typescript

  const myArray = e.array(['a', 'b', 'c', 'd', 'e']);
  // ['a', 'b', 'c', 'd', 'e']

  myArray[1];
  // ['a', 'b', 'c', 'd', 'e'][1]

  myArray['1:3'];
  // ['a', 'b', 'c', 'd', 'e'][1:3]

There are also equivalent ``.index`` and ``.slice`` methods that can accept
other expressions as arguments.

.. code-block:: typescript

  const start = e.int64(1);
  const end = e.int64(3);

  myArray.index(start);
  // ['a', 'b', 'c', 'd', 'e'][1]

  myArray.slice(start, end);
  // ['a', 'b', 'c', 'd', 'e'][1:3]

Tuples
^^^^^^

Declare tuples with ``e.tuple``. Pass in an array to declare a "regular"
(unnamed) tuple; pass in an object to declare a named tuple.

.. code-block:: typescript

  e.tuple([e.str("Peter Parker"), e.int64(18)]);
  // ("Peter Parker", 18)

  e.tuple({
    name: e.str("Peter Parker"),
    age: e.int64(18)
  });
  // (name := "Peter Parker", age := 18)

Tuple expressions support indexing.

.. code-block:: typescript

  // Unnamed tuples
  const spidey = e.tuple([
    e.str("Peter Parker"),
    e.int64(18)
  ]);
  spidey[0];                 // => ("Peter Parker", 18)[0]

  // Named tuples
  const spidey = e.tuple({
    name: e.str("Peter Parker"),
    age: e.int64(18)
  });
  spidey.name;
  // (name := "Peter Parker", age := 18).name

Set literals
^^^^^^^^^^^^

Declare sets with ``e.set``.

.. code-block:: typescript

  e.set(e.str("asdf"), e.str("qwer"));
  // {'asdf', 'qwer'}

As in EdgeQL, sets can't contain elements with incompatible types. These
semantics are enforced by TypeScript.

.. code-block:: typescript

  e.set(e.int64(1234), e.str('sup'));
  // TypeError

Empty sets
^^^^^^^^^^

To declare an empty set, cast an empty set to the desired type. As in EdgeQL,
empty sets are not allowed without a cast.

.. code-block:: typescript

  e.cast(e.int64, e.set());
  // <std::int64>{}


Range literals
^^^^^^^^^^^^^^

As in EdgeQL, declare range literals with the built-in ``range`` function.

.. code-block:: typescript

  const myRange = e.range(0, 8);

  myRange.toEdgeQL();
  // => std::range(0, 8);

Ranges can be created for all numerical types, as well as ``datetime``, ``local_datetime``, and ``local_date``.

.. code-block:: typescript

  e.range(e.decimal('100'), e.decimal('200'));
  e.range(Date.parse("1970-01-01"), Date.parse("2022-01-01"));
  e.range(new LocalDate(1970, 1, 1), new LocalDate(2022, 1, 1));

Supply named parameters as the first argument.

.. code-block:: typescript

  e.range({inc_lower: true, inc_upper: true, empty: true}, 0, 8);
  // => std::range(0, 8, true, true);

JavaScript doesn't have a native way to represent range values. Any range value returned from a query will be encoded as an instance of the :js:class:`Range` class, which is exported from the ``edgedb`` package.

.. code-block:: typescript

  const query = e.range(0, 8);
  const result = await query.run(client);
  // => Range<number>;

  console.log(result.lower);       // 0
  console.log(result.upper);       // 8
  console.log(result.isEmpty);     // false
  console.log(result.incLower);    // true
  console.log(result.incUpper);    // false


.. Modules
.. -------

.. All *types*, *functions*, and *commands* are available on the ``e`` object, properly namespaced by module.

.. .. code-block:: typescript

..   // commands
..   e.select;
..   e.insert;
..   e.update;
..   e.delete;

..   // types
..   e.std.str;
..   e.std.int64;
..   e.std.bool;
..   e.cal.local_datetime;
..   e.default.User; // user-defined object type
..   e.my_module.Foo; // object type in user-defined module

..   // functions
..   e.std.len;
..   e.std.str_upper;
..   e.math.floor;
..   e.sys.get_version;

.. For convenience, the contents of the ``std`` and ``default`` modules are also exposed at the top-level of ``e``.

.. .. code-block:: typescript

..   e.str;
..   e.int64;
..   e.bool;
..   e.len;
..   e.str_upper;
..   e.User;

.. .. note::

..   If there are any name conflicts (e.g. a user-defined module called ``len``),
..   ``e.len`` will point to the user-defined module; in that scenario, you must
..   explicitly use ``e.std.len`` to access the built-in ``len`` function.
