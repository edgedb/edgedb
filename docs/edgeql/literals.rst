.. _ref_eql_literals:

Literals
========

EdgeQL is *inextricably tied* to EdgeDB's rigorous type system. Below is an
overview of how to declare a literal value of each *primitive type*. Click a
link in the left column to jump to the associated section.

.. list-table::

  * - :ref:`String <ref_eql_literal_strings>`
    - ``str``

  * - :ref:`Boolean <ref_eql_literal_boolean>`
    - ``bool``

  * - :ref:`Numbers <ref_eql_literal_numbers>`
    - ``int16`` ``int32`` ``int64``
      ``float32`` ``float64`` ``bigint``
      ``decimal``

  * - :ref:`UUID <ref_eql_literal_uuid>`
    - ``uuid``

  * - :ref:`Enums <ref_eql_literal_enum>`
    - ``enum<X, Y, Z>``

  * - :ref:`Dates and times <ref_eql_literal_dates>`
    - ``datetime`` ``duration``
      ``cal::local_datetime`` ``cal::local_date``
      ``cal::local_time`` ``cal::relative_duration``

  * - :ref:`Durations <ref_eql_literal_durations>`
    - ``duration`` ``cal::relative_duration`` ``cal::date_duration``

  * - :ref:`Ranges <ref_eql_ranges>`
    - ``range<x>``

  * - :ref:`Bytes <ref_eql_literal_bytes>`
    - ``bytes``

  * - :ref:`Arrays <ref_eql_literal_array>`
    - ``array<x>``

  * - :ref:`Tuples <ref_eql_literal_tuple>`
    - ``tuple<x, y, ...>`` or
      ``tuple<foo: x, bar: y, ...>``

  * - :ref:`JSON <ref_eql_literal_json>`
    - ``json``

.. _ref_eql_literal_strings:

Strings
-------

The :eql:type:`str` type is a variable-length string of Unicode characters. A
string can be declared with either single or double quotes.

.. code-block:: edgeql-repl

  db> select 'i ❤️ edgedb';
  {'i ❤️ edgedb'}
  db> select "hello there!";
  {'hello there!'}
  db> select 'hello\nthere!';
  {'hello
  there!'}
  db> select 'hello
  ... there!';
  {'hello
  there!'}
  db> select r'hello
  ... there!'; # multiline
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
  db> select $label$You can add an interstitial label
  ... if you need to use "$$" in your string.$label$;
  {
    'You can add an interstital label
    if you need to use "$$" in your string.',
  }

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
    - :eql:op:`str[i] <stridx>` :eql:op:`str[from:to] <strslice>`
  * - Concatenation
    - :eql:op:`str ++ str <strplus>`
  * - Utilities
    - :eql:func:`len`
  * - Transformation functions
    - :eql:func:`str_split` :eql:func:`str_lower` :eql:func:`str_upper`
      :eql:func:`str_title` :eql:func:`str_pad_start` :eql:func:`str_pad_end`
      :eql:func:`str_trim` :eql:func:`str_trim_start` :eql:func:`str_trim_end`
      :eql:func:`str_repeat`
  * - Comparison operators
    - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
      :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
      :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
  * - Search
    - :eql:func:`contains` :eql:func:`find`
  * - Pattern matching and regexes
    - :eql:op:`str like pattern <like>` :eql:op:`str ilike pattern <ilike>`
      :eql:func:`re_match` :eql:func:`re_match_all` :eql:func:`re_replace`
      :eql:func:`re_test`


.. _ref_eql_literal_boolean:

Booleans
--------

The :eql:type:`bool` type represents a true/false value.

.. code-block:: edgeql-repl

  db> select true;
  {true}
  db> select false;
  {false}

EdgeDB provides a set of operators that operate on boolean values.

.. list-table::

  * - Comparison operators
    - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
      :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
      :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
  * - Logical operators
    - :eql:op:`or` :eql:op:`and` :eql:op:`not`
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
 :eql:code:`select 3;`                  :eql:type:`int64`
 :eql:code:`select 3.14;`               :eql:type:`float64`
 :eql:code:`select 314e-2;`             :eql:type:`float64`
 :eql:code:`select 42n;`                :eql:type:`bigint`
 :eql:code:`select 42.0n;`              :eql:type:`decimal`
 :eql:code:`select 42e+100n;`           :eql:type:`decimal`

====================================== =============================

To declare an ``int16``, ``int32``, or ``float32``, you must provide an
explicit type cast. For details on type casting, see :ref:`Casting
<ref_eql_types>`.

====================================== =============================
 Syntax                                 Type
====================================== =============================
 :eql:code:`select <int16>1234;`        :eql:type:`int16`
 :eql:code:`select <int32>123456;`      :eql:type:`int32`
 :eql:code:`select <float32>123.456;`   :eql:type:`float32`
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
    - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
      :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
      :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
  * - Arithmetic
    - :eql:op:`+ <plus>` :eql:op:`- <minus>` :eql:op:`- <uminus>`
      :eql:op:`* <mult>` :eql:op:`/ <div>` :eql:op:`//  <floordiv>`
      :eql:op:`% <mod>` :eql:op:`^ <pow>`
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


.. _ref_eql_literal_enum:

Enums
-----

Enum types must be :ref:`declared in your schema <ref_datamodel_enums>`.

.. code-block:: sdl

  scalar type Color extending enum<Red, Green, Blue>;

Once declared, an enum literal can be declared with dot notation, or by
casting an appropriate string literal:

.. code-block:: edgeql-repl

  db> select Color.Red;
  {Red}
  db> select <Color>"Red";
  {Red}


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
    - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
      :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
      :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
  * - Arithmetic
    - :eql:op:`dt + dt <dtplus>` :eql:op:`dt - dt <dtminus>`
  * - String parsing
    - :eql:func:`to_datetime` :eql:func:`cal::to_local_datetime`
      :eql:func:`cal::to_local_date` :eql:func:`cal::to_local_time`
  * - Component extraction
    - :eql:func:`datetime_get` :eql:func:`cal::time_get`
      :eql:func:`cal::date_get`
  * - Truncation
    - :eql:func:`datetime_truncate`
  * - System timestamps
    - :eql:func:`datetime_current` :eql:func:`datetime_of_transaction`
      :eql:func:`datetime_of_statement`


.. _ref_eql_literal_durations:

Durations
---------

EdgeDB's type system contains three duration types.


.. list-table::

  * - :eql:type:`duration`
    - Exact duration
  * - :eql:type:`cal::relative_duration`
    - Duration in relative units
  * - :eql:type:`cal::date_duration`
    - Duration in months and days only

Exact durations
^^^^^^^^^^^^^^^

The :eql:type:`duration` type represents *exact* durations that can be
represented by some fixed number of microseconds. It can be negative and it
supports units of ``microseconds``, ``milliseconds``, ``seconds``, ``minutes``,
and ``hours``.

.. code-block:: edgeql-repl

  db> select <duration>'45.6 seconds';
  {<duration>'0:00:45.6'}
  db> select <duration>'-15 microseconds';
  {<duration>'-0:00:00.000015'}
  db> select <duration>'5 hours 4 minutes 3 seconds';
  {<duration>'5:04:03'}
  db> select <duration>'8760 hours'; # about a year
  {<duration>'8760:00:00'}

All temporal units beyond ``hour`` no longer correspond to a fixed duration of
time; the length of a day/month/year/etc changes based on daylight savings
time, the month in question, leap years, etc.

Relative durations
^^^^^^^^^^^^^^^^^^

By contrast, the :eql:type:`cal::relative_duration` type represents a
"calendar" duration, like ``1 month``. Because months have different number of
days, ``1 month`` doesn't correspond to a fixed number of milliseconds, but
it's often a useful quantity to represent recurring events, postponements, etc.

.. note::

  The ``cal::relative_duration`` type supports the same units as ``duration``,
  plus ``days``, ``weeks``, ``months``, ``years``, ``decades``, ``centuries``,
  and ``millennia``.

To declare relative duration literals:

.. code-block:: edgeql-repl

  db> select <cal::relative_duration>'15 milliseconds';
  {<cal::relative_duration>'PT.015S'}
  db> select <cal::relative_duration>'2 months 3 weeks 45 minutes';
  {<cal::relative_duration>'P2M21DT45M'}
  db> select <cal::relative_duration>'-7 millennia';
  {<cal::relative_duration>'P-7000Y'}

Date durations
^^^^^^^^^^^^^^

.. versionadded:: 2.0

The :eql:type:`cal::date_duration` represents spans consisting of some number
of *months* and *days*. This type is primarily intended to simplify logic
involving :eql:type:`cal::local_date` values.

.. code-block:: edgeql-repl

  db> select <cal::date_duration>'5 days';
  {<cal::date_duration>'P5D'}
  db> select <cal::local_date>'2022-06-25' + <cal::date_duration>'5 days';
  {<cal::local_date>'2022-06-30'}
  db> select <cal::local_date>'2022-06-30' - <cal::local_date>'2022-06-25';
  {<cal::date_duration>'P5D'}

EdgeQL supports a set of functions and operators on duration types.

.. list-table::

  * - Comparison operators
    - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
      :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
      :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
  * - Arithmetic
    - :eql:op:`dt + dt <dtplus>` :eql:op:`dt - dt <dtminus>`
  * - Duration string parsing
    - :eql:func:`to_duration` :eql:func:`cal::to_relative_duration`
      :eql:func:`cal::to_date_duration`
  * - Component extraction
    - :eql:func:`duration_get`
  * - Conversion
    - :eql:func:`duration_truncate` :eql:func:`cal::duration_normalize_hours`
      :eql:func:`cal::duration_normalize_days`


.. _ref_eql_ranges:

Ranges
------

.. versionadded:: 2.0

Ranges represent a range of orderable scalar values. A range comprises a lower
bound, upper bound, and two boolean flags indicating whether each bound is
inclusive.

Create a range literal with the ``range`` constructor function.

.. code-block:: edgeql-repl

    db> select range(1, 10);
    {range(1, 10, inc_lower := true, inc_upper := false)}
    db> select range(2.2, 3.3);
    {range(2.2, 3.3, inc_lower := true, inc_upper := false)}

Ranges can be *empty*, when the upper and lower bounds are equal.

.. code-block:: edgeql-repl

    db> select range(1, 1);
    {range({}, empty := true)}

Ranges can be *unbounded*. An empty set is used to indicate the
lack of a particular upper or lower bound.

.. code-block:: edgeql-repl

    db> select range(4, <int64>{});
    {range(4, {})}
    db> select range(<int64>{}, 4);
    {range({}, 4)}
    db> select range(<int64>{}, <int64>{});
    {range({}, {})}

To compute the set of concrete values defined by a range literal, use
``range_unpack``. An empty range will unpack to the empty set. Unbounded
ranges cannot be unpacked.

.. code-block:: edgeql-repl

    db> select range_unpack(range(0, 10));
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    db> select range_unpack(range(1, 1));
    {}
    db> select range_unpack(range(0, <int64>{}));
    edgedb error: InvalidValueError: cannot unpack an unbounded range

.. _ref_eql_literal_bytes:

Bytes
-----

The ``bytes`` type represents raw binary data.

.. code-block:: edgeql-repl

  db> select b'bina\\x01ry';
  {b'bina\\x01ry'}

There is a special syntax for declaring "raw byte strings". Raw byte strings
treat the backslash ``\`` as a literal character instead of an escape
character.

.. code-block:: edgeql-repl

  db> select rb'hello\nthere';
  {b'hello\\nthere'}
  db> select br'\';
  {b'\\'}


.. _ref_eql_literal_array:

Arrays
------

An array is an *ordered* collection of values of the *same type*. For example:

.. code-block:: edgeql-repl

    db> select [1, 2, 3];
    {[1, 2, 3]}
    db> select ['hello', 'world'];
    {['hello', 'world']}
    db> select [(1, 2), (100, 200)];
    {[(1, 2), (100, 200)]}

EdgeQL provides a set of functions and operators on arrays.

.. list-table::

  * - Indexing and slicing
    - :eql:op:`array[i] <arrayidx>` :eql:op:`array[from:to] <arrayslice>`
      :eql:func:`array_get`
  * - Concatenation
    - :eql:op:`array ++ array <arrayplus>`
  * - Comparison operators
    - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
      :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
      :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
  * - Utilities
    - :eql:func:`len` :eql:func:`array_join`
  * - Search
    - :eql:func:`contains` :eql:func:`find`
  * - Conversion to/from sets
    - :eql:func:`array_agg` :eql:func:`array_unpack`

See :ref:`Standard Library > Array <ref_std_array>` for a complete
reference on array data types.


.. _ref_eql_literal_tuple:

Tuples
------

A tuple is *fixed-length*, *ordered* collection of values, each of which may
have a *different type*. The elements of a tuple can be of any type, including
scalars, arrays, other tuples, and object types.

.. code-block:: edgeql-repl

  db> select ('Apple', 7, true);
  {('Apple', 7, true)}

Optionally, you can assign a key to each element of a tuple. These are known
as *named tuples*. You must assign keys to all or none of the elements; you
can't mix-and-match.

.. code-block:: edgeql-repl

  db> select (fruit := 'Apple', quantity := 3.14, fresh := true);
  {(fruit := 'Apple', quantity := 3.14, fresh := true)}

Indexing tuples
^^^^^^^^^^^^^^^

Tuple elements can be accessed with dot notation. Under the hood, there's no
difference between named and unnamed tuples. Named tuples support key-based
and numerical indexing.

.. code-block:: edgeql-repl

    db> select (1, 3.14, 'red').0;
    {1}
    db> select (1, 3.14, 'red').2;
    {'red'}
    db> select (name := 'george', age := 12).name;
    {('george')}
    db> select (name := 'george', age := 12).0;
    {('george')}

.. important::

  When you query an *unnamed* tuple using one of EdgeQL's :ref:`client
  libraries <ref_clients_index>`, its value is converted to a list/array. When
  you fetch a *named tuple*, it is converted to an object/dictionary/hashmap.

For a full reference on tuples, see :ref:`Standard Library > Tuple
<ref_std_tuple>`.

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

JSON values support indexing operators. The resulting value is also of type
``json``.

.. code-block:: edgeql-repl

  db> select to_json('{"a": 2, "b": 5}')['a'];
  {2}
  db> select to_json('["a", "b", "c"]')[2];
  {'"c"'}


EdgeQL supports a set of functions and operators on ``json`` values. Refer to
the :ref:`Standard Library > JSON <ref_std_json>` or click an item below for
detailed documentation.

.. list-table::

    * - Indexing
      - :eql:op:`json[i] <jsonidx>` :eql:op:`json[from:to] <jsonslice>`
        :eql:op:`json[name] <jsonobjdest>` :eql:func:`json_get`
    * - Merging
      - :eql:op:`json ++ json <jsonplus>`
    * - Comparison operators
      - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
    * - Conversion to/from strings
      - :eql:func:`to_json` :eql:func:`to_str`
    * - Conversion to/from sets
      - :eql:func:`json_array_unpack` :eql:func:`json_object_unpack`
    * - Introspection
      - :eql:func:`json_typeof`
