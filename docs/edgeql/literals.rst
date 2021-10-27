.. _ref_eql_literals:

Literals
========

Strings
-------

A string is a variable-length string of Unicode characters.

.. code-block:: edgeql-repl

  db> select 'hello there!';
  {'hello there!'}
  db> select 'i ❤️ edgedb';
  {'i ❤️ edgedb'}
  db> select r'hello\nthere'; # raw string
  {r'hello\\nthere'}
  db> select $$one
  ... two
  ... three$$; # multiline raw string
  {'one
  two
  three'}


EdgeQL contains a set of built-in functions and operators for searching, comparing, and manipulating strings. These are comprehensively documented in the :ref:`str <ref_std_string>` reference docs.

.. list-table::

  * - Indexing and slicing
    - :eql:op:`str[i] <STRIDX>` :eql:op:`str[from:to] <STRSLICE>`
  * - Concatenation
    - :eql:op:`str ++ str <STRPLUS>`
  * - Utilities
    - :eql:func:`len` :eql:func:`str_split`
  * - Transformation functions
    - :eql:func:`str_lower` :eql:func:`str_upper` :eql:func:`str_title`
      :eql:func:`str_pad_start` :eql:func:`str_pad_end` :eql:func:`str_trim`
      :eql:func:`str_trim_start` :eql:func:`str_trim_end` :eql:func:`str_repeat`
  * - Comparison operators
    - :eql:op:`str = str <EQ>`, :eql:op:`str \< str <LT>`, etc.
  * - Search
    - :eql:func:`contains` :eql:func:`find`
  * - Pattern matching and regexes
    - :eql:op:`str LIKE pattern <LIKE>` :eql:op:`str ILIKE pattern <ILIKE>`
      :eql:func:`re_match` :eql:func:`re_match_all` :eql:func:`re_replace`
      :eql:func:`re_test`


.. _ref_eql_literal_boolean:

Booleans
--------


The ``bool`` type represents a true/false value.

.. code-block:: edgeql-repl

  db> select true;
  {true}
  db> select false;
  {false}


Numbers
-------

All numbers that *do not* contain a decimal are interpreted as ``int64``. Numbers containing decimals are interpreted as ``float64``. Scientific notation is supported. The ``n`` suffix designates a number to be *variable-precision* (either ``bigint`` or ``decimal``).

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

UUID
----

A ``uuid`` must be explicitly cast from a string value matching the UUID specification.

.. code-block:: edgeql-repl

  db> select <uuid>'a5ea6360-75bd-4c20-b69c-8f317b0d2857';
  {a5ea6360-75bd-4c20-b69c-8f317b0d2857}

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

Temporal types
--------------

// TODO

.. _ref_eql_literal_tuple:

Tuples
------

A tuple is *fixed-length*, *ordered* collection of values, each of which may have a *different type*. The elements of a tuple can be of any type, including scalars, arrays, tuples, and object types.

========================================== =====================================
 Syntax                                     Inferred type
========================================== =====================================
:eql:code:`SELECT (true, 3.14, 'red');`    ``tuple<bool, float64, str>``
:eql:code:`SELECT (true, (3.14, 'red'));`  ``tuple<int64, tuple<float64, str>>``
:eql:code:`SELECT (name := "billie");`     ``tuple<name: str>``
========================================== =====================================

**Indexing tuples**

.. code-block:: edgeql-repl

    db> SELECT (1, 3.14, 'red').0;
    {1}
    db> SELECT (1, 3.14, 'red').2;
    {'red'}


**Named tuples**

Optionally, you can attach *keys* to each element of a tuple. This is known as a *named tuple*.

.. code-block:: edgeql-repl

    db> SELECT (name := 'george', age := 12);
    {(name := 'george', age := 12)}
    db> SELECT (name := 'george', age := 12).name;
    {('george')}

Though each element of a named tuple is accessible via its key, the elements are *still ordered* and can be referenced with numerical indices.

.. code-block:: edgeql-repl

    db> SELECT (name := 'george', age := 12).0;
    {('george')}

.. important::

  When you query an *unnamed* tuple using one of EdgeQL's :ref:`client
  libraries <ref_clients_index>`, its value is represented as a list/array. When
  you fetch a *named tuple*, it is represented as an object/dictionary/hashmap.

For a full reference on tuples, see the :ref:`Tuple reference documentation
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

See the Standard Library :ref:`Array page <ref_std_array>` for a complete
reference on array data types.
