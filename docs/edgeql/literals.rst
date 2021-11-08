.. _ref_eql_literals:

Literals
========

EdgeQL is *inextricably tied* to EdgeDB's rigorous type system. Below is an
overview of how to declare a literal value of each *primitive type*.

.. list-table::

    * - String type
      - ``str``

    * - Boolean type
      - ``bool``

    * - Numerical types
      - ``int16`` ``int32`` ``int64``
        ``float32`` ``float64`` ``bigint``
        ``decimal``

    * - JSON type
      - ``json``

    * - UUID type
      - ``uuid``

    * - Binary data
      - ``bytes``

    * - Temporal types
      - ``datetime`` ``duration``
        ``cal::local_datetime`` ``cal::local_date``
        ``cal::local_time`` ``cal::relative_duration``

    * - Array types
      - ``array<x>``

    * - Tuple types
      - ``tuple<x, y, ...>`` or
        ``tuple<foo: x, bar: y, ...>``



.. _ref_eql_literal_strings:

Strings
-------

The :eql:type:`str` type is a variable-length string of Unicode characters. A
string can be declared with either single or double quotes.

.. code-block:: edgeql

  select 'i ❤️ edgedb';
  select 'hello there!';
  select r'hello\nthere!'; # raw string


.. code-block:: edgeql-repl

  db> select 'i ❤️ edgedb';
  {'i ❤️ edgedb'}
  db> select 'hello there!';
  {'hello there!'}
  db> select r'hello\nthere!';
  {'hello
  there!'}

There is a special syntax for declaring "raw strings". Raw strings treat the
backslash ``\`` as a literal character instead of an escape character.

.. code-block:: edgeql-repl

  db> select r'hello\nthere'; # raw string
  {r'hello\\nthere'}
  db> select $$one
  ... two
  ... three$$; # multiline raw string
  {'one
  two
  three'}



EdgeQL contains a set of built-in functions and operators for searching,
comparing, and manipulating strings.

.. code-block:: edgeql-repl

  db> select 'hellothere'[5:10];
  {'there'}
  db> select 'hello' ++ 'there';
  {'hellothere'}
  db> select len('hellothere');
  {10}
  db> select str_trim('  hello there  ');
  {'hello there'}
  db> select str_split('hello there', ' ');
  {['hello', 'there']}


For a complete reference on strings, see :ref:`Standard Library > String
<ref_std_string>` or click an item below.

.. list-table::

  * - Indexing and slicing
    - :eql:op:`str[i] <STRIDX>` :eql:op:`str[from:to] <STRSLICE>`
  * - Concatenation
    - :eql:op:`str ++ str <STRPLUS>`
  * - Utilities
    - :eql:func:`len`
  * - Transformation functions
    - :eql:func:`str_split` :eql:func:`str_lower` :eql:func:`str_upper`
      :eql:func:`str_title` :eql:func:`str_pad_start` :eql:func:`str_pad_end`
      :eql:func:`str_trim` :eql:func:`str_trim_start` :eql:func:`str_trim_end`
      :eql:func:`str_repeat`
  * - Comparison operators
    - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
      :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
      :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
  * - Search
    - :eql:func:`contains` :eql:func:`find`
  * - Pattern matching and regexes
    - :eql:op:`str LIKE pattern <LIKE>` :eql:op:`str ILIKE pattern <ILIKE>`
      :eql:func:`re_match` :eql:func:`re_match_all` :eql:func:`re_replace`
      :eql:func:`re_test`


.. _ref_eql_literal_boolean:

Booleans
--------

The :eql:type:`str` type represents a true/false value.

.. code-block:: edgeql-repl

  db> select true;
  {true}
  db> select false;
  {false}

EdgeDB provides a set of operators that operate on boolean values.

.. list-table::

  * - Comparison operators
    - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
      :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
      :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
  * - Logical operators
    - :eql:op:`OR <OR>` :eql:op:`AND <AND>` :eql:op:`NOT <NOT>`
  * - Aggregation
    - :eql:func:`all` :eql:func:`any`


.. _ref_eql_literal_numbers:

Numbers
-------

There are several numerical types in EdgeDB's type system.

.. list-table::

    * - :eql:type:`int16`
      - 16-bit integer

    * - :eql:type:`int32`
      - 32-bit integer

    * - :eql:type:`int64`
      - 64-bit integer

    * - :eql:type:`float32`
      - 32-bit floating point number

    * - :eql:type:`float64`
      - 64-bit floating point number

    * - :eql:type:`bigint`
      - Arbitrary precision integer.

    * - :eql:type:`decimal`
      - Arbitrary precision number.

Number literals that *do not* contain a decimal are interpreted as ``int64``.
Numbers containing decimals are interpreted as ``float64``. The ``n`` suffix
designates a number with *arbitrary precision*: either ``bigint`` or
``decimal``.

====================================== =============================
 Syntax                                 Inferred type
====================================== =============================
 :eql:code:`SELECT 3;`                  :eql:type:`int64`
 :eql:code:`SELECT 3.14;`               :eql:type:`float64`
 :eql:code:`SELECT 314e-2;`             :eql:type:`float64`
 :eql:code:`SELECT 42n;`                :eql:type:`bigint`
 :eql:code:`SELECT 42.0n;`              :eql:type:`decimal`
 :eql:code:`SELECT 42e+100n;`           :eql:type:`decimal`

====================================== =============================

To declare an ``int16``, ``int32``, or ``float32``, you must provide an
explicit type cast. For details on type casting, see :ref:`Casting
<ref_eql_types>`.

====================================== =============================
 Syntax                                 Type
====================================== =============================
 :eql:code:`SELECT <int16>1234;`        :eql:type:`int16`
 :eql:code:`SELECT <int32>123456;`      :eql:type:`int32`
 :eql:code:`SELECT <float32>123.456;`   :eql:type:`float32`
====================================== =============================

EdgeQL includes a full set of arithmetic and comparison operators. Parentheses
can be used to indicate the order-of-operations or visually group
subexpressions; this is true across all EdgeQL queries.

.. code-block:: edgeql-repl

  db> select 5 > 2;
  {true}
  db> select 2 + 2;
  {4}
  db> select 2 ^ 10;
  {1024}
  db> select (1 + 1) * 2 / (3 + 8);
  {0.36363636363636365}


EdgeQL provides a comprehensive set of built-in functions and operators on
numerical data.

.. list-table::

  * - Comparison operators
    - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
      :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
      :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
  * - Arithmetic
    - :eql:op:`+ <PLUS>` :eql:op:`- <MINUS>` :eql:op:`- <UMINUS>`
      :eql:op:`* <MULT>` :eql:op:`/ <DIV>` :eql:op:`/  <FLOORDIV>`
      :eql:op:`% <MOD>` :eql:op:`^ <POW>`
  * - Statistics
    - :eql:func:`sum` :eql:func:`min` :eql:func:`max` :eql:func:`math::mean`
      :eql:func:`math::stddev` :eql:func:`math::stddev_pop`
      :eql:func:`math::var` :eql:func:`math::var_pop`
  * - Math
    - :eql:func:`round` :eql:func:`math::abs` :eql:func:`math::ceil`
      :eql:func:`math::floor` :eql:func:`math::ln` :eql:func:`math::lg`
      :eql:func:`math::log`
  * - Random number
    - :eql:func:`random`

.. _ref_eql_literal_json:

JSON
----

The :eql:type:`json` scalar type is a stringified representation of structured
data. JSON literals are declared by explicitly casting other values or passing
a properly formatted JSON string into :eql:func:`to_json`. Any type can be
converted into JSON except :eql:type:`bytes`.

.. code-block:: edgeql-repl

  db> select <json>5;
  {'5'}
  db> select <json>"a string";
  {'"a string"'}
  db> select <json>["this", "is", "an", "array"];
  {'["this", "is", "an", "array"]'}
  db> select <json>("unnamed tuple", 2);
  {'["unnamed tuple", 2]'}
  db> select <json>(name := "named tuple", count := 2);
  {'{
    "name": "named tuple",
    "count": 2
  }'}
  db> select to_json('{"a": 2, "b": 5}');
  {'{"a": 2, "b": 5}'}

JSON values support indexing operators. The resulting value is a ``json``.

.. code-block:: edgeql-repl

  db> select to_json('{"a": 2, "b": 5}')['a'];
  {1}
  db> select to_json('["a", "b", "c"]')[2];
  {'"c"'}


EdgeQL supports a set of functions and operators on ``json`` values. Refer to
the :ref:`Standard Library > JSON <ref_std_json>` or click an item below for
details documentation.

.. list-table::

    * - Indexing
      - :eql:op:`json[i] <JSONIDX>` :eql:op:`json[from:to] <JSONSLICE>`
        :eql:op:`json[name] <JSONOBJDEST>` :eql:func:`json_get`
    * - Merging
      - :eql:op:`json ++ json <JSONPLUS>`
    * - Comparison operators
      - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
        :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
        :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
    * - Conversion to/from strings
      - :eql:func:`to_json` :eql:func:`to_str`
    * - Conversion to/from sets
      - :eql:func:`json_array_unpack` :eql:func:`json_object_unpack`
    * - Introspection
      - :eql:func:`json_typeof`

.. _ref_eql_literal_uuid:

UUID
----

The :eql:type:`uuid` type is commonly used to represent object identifiers.
UUID literal must be explicitly cast from a string value matching the UUID
specification.

.. code-block:: edgeql-repl

  db> select <uuid>'a5ea6360-75bd-4c20-b69c-8f317b0d2857';
  {a5ea6360-75bd-4c20-b69c-8f317b0d2857}

Generate a random UUID.

.. code-blocK:: edgeql-repl

  db> select uuid_generate_v1mc();
  {b4d94e6c-3845-11ec-b0f4-93e867a589e7}


.. _ref_eql_literal_bytes:

Bytes
-----

The ``bytes`` type represents raw binary data.

.. code-block:: edgeql-repl

  db> SELECT b'bina\\x01ry';
  {b'bina\\x01ry'}


.. _ref_eql_literal_enum:

Enums
-----

Enum types must be :ref:`declared in your schema <ref_datamodel_enums>`.

.. code-block:: sdl

  scalar type Color extending enum<Red, Green, Blue>;

Once declared, their values can be referenced with dot notation.

.. code-block:: edgeql

  select Color.Red;


.. _ref_eql_literal_dates:

Dates and times
---------------

EdgeDB's typesystem contains several temporal types.

.. list-table::

  * - :eql:type:`datetime`
    - Timezone-aware point in time

  * - :eql:type:`cal::local_datetime`
    - Date and time w/o timezone

  * - :eql:type:`cal::local_date`
    - Date type

  * - :eql:type:`cal::local_time`
    - Time type

All temporal literals are declared by casting an appropriately formatted
string.

.. code-block:: edgeql-repl

  db> select <datetime>'1999-03-31T15:17:00Z';
  {<datetime>'1999-03-31T15:17:00Z'}
  db> select <datetime>'1999-03-31T17:17:00+02';
  {<datetime>'1999-03-31T15:17:00Z'}
  db> select <cal::local_datetime>'1999-03-31T15:17:00';
  {<cal::local_datetime>'1999-03-31T15:17:00'}
  db> select <cal::local_date>'1999-03-31';
  {<cal::local_date>'1999-03-31'}
  db> select <cal::local_time>'15:17:00';
  {<cal::local_time>'15:17:00'}

EdgeQL supports a set of functions and operators on datetime types.

.. list-table::

  * - Comparison operators
    - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
      :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
      :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
  * - Arithmetic
    - :eql:op:`dt + dt <DTPLUS>` :eql:op:`dt - dt <DTMINUS>`
  * - String parsing
    - :eql:func:`to_datetime` :eql:func:`cal::to_local_datetime`
      :eql:func:`cal::to_local_date` :eql:func:`cal::to_local_time`
  * - Component extraction
    - :eql:func:`datetime_get` :eql:func:`cal::time_get`
      :eql:func:`cal::date_get`
  * - Truncation
    - :eql:func:`duration_truncate`
  * - System timestamps
    - :eql:func:`datetime_current` :eql:func:`datetime_of_transaction`
      :eql:func:`datetime_of_statement`


.. _ref_eql_literal_durations:

Durations
---------


EdgeDB's typesystem contains two duration types.


The :eql:type:`duration` type represents *exact* durations that can be
represented by some fixed number of microseconds. It can be negative and it
supports units of ``microseconds``, ``milliseconds``, ``seconds``, ``minutes``,
and ``hours``.

.. code-block:: edgeql-repl

  db> SELECT <duration>'45.6 seconds';
  {<duration>'0:00:45.6'}
  db> SELECT <duration>'-15 microseconds';
  {<duration>'-0:00:00.000015'}
  db> SELECT <duration>'5 hours 4 minutes 3 seconds';
  {<duration>'5:04:03'}

The :eql:type:`cal::relative_duration` type represents a "calendar" duration,
like ``1 month``. Because months have different number of days, ``1 month``
doesn't correspond to a fixed number of milliseconds, but it's often a useful
quantity to represent recurring events, postponements, etc.

.. note::

  The ``cal::relative_duration`` type supports the same units as ``duration``,
  plus ``days``, ``weeks``, ``months``, ``years``, ``decades``, ``centuries``,
  and ``millennium``.

To declare relative duration literals:

.. code-block:: edgeql-repl

  db> SELECT <cal::relative_duration>'15 milliseconds';
  {<cal::relative_duration>'PT.015S'}
  db> SELECT <cal::relative_duration>'2 months 3 weeks 45 minutes';
  {<cal::relative_duration>'P2M21DT45M'}
  db> SELECT <cal::relative_duration>'-7 millennium';
  {<cal::relative_duration>'P-7000Y'}


EdgeQL supports a set of functions and operators on duration types.

.. list-table::

  * - Comparison operators
    - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
      :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
      :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
  * - Arithmetic
    - :eql:op:`dt + dt <DTPLUS>` :eql:op:`dt - dt <DTMINUS>`
  * - Duration string parsing
    - :eql:func:`to_duration` :eql:func:`cal::to_relative_duration`
  * - Truncation
    - :eql:func:`duration_truncate`

.. _ref_eql_literal_tuple:

Tuples
------

A tuple is *fixed-length*, *ordered* collection of values, each of which may
have a *different type*. The elements of a tuple can be of any type, including
scalars, arrays, tuples, and object types.

.. list-table::
  :header-rows: 1

  * - **Syntax**
    - **Inferred type**
  * - :eql:code:`SELECT (true, 3.14, 'red');`
    - ``tuple<bool, float64, str>``
  * - :eql:code:`SELECT (name := "billie");`
    - ``tuple<name: str>``


Indexing tuples
^^^^^^^^^^^^^^^

.. code-block:: edgeql-repl

    db> SELECT (1, 3.14, 'red').0;
    {1}
    db> SELECT (1, 3.14, 'red').2;
    {'red'}
    db> SELECT (name := 'george', age := 12).name;
    {('george')}
    db> SELECT (name := 'george', age := 12).0;
    {('george')}

.. important::

  When you query an *unnamed* tuple using one of EdgeQL's :ref:`client
  libraries <ref_clients_index>`, its value is converted to a list/array. When
  you fetch a *named tuple*, it is converted to an object/dictionary/hashmap.

For a full reference on tuples, see :ref:`Standard Library > Tuple
<ref_std_tuple>`.


.. _ref_eql_literal_array:

Arrays
------

An array is an *ordered* collection of values of the *same type*. For example:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3];
    {[1, 2, 3]}
    db> SELECT ['hello', 'world'];
    {['hello', 'world']}
    db> SELECT [(1, 2), (100, 200)];
    {[(1, 2), (100, 200)]}

EdgeQL provides a set of functions and operators on arrays.

.. list-table::

  * - Indexing and slicing
    - :eql:op:`array[i] <ARRAYIDX>` :eql:op:`array[from:to] <ARRAYSLICE>`
      :eql:func:`array_get`
  * - Concatenation
    - :eql:op:`array ++ array <ARRAYPLUS>`
  * - Comparison operators
    - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
      :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
      :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
  * - Utilities
    - :eql:func:`len` :eql:func:`array_join`
  * - Search
    - :eql:func:`contains` :eql:func:`find`
  * - Conversion to/from sets
    - :eql:func:`array_agg` :eql:func:`array_unpack`

See :ref:`Standard Library > Array <ref_std_array>` for a complete
reference on array data types.

