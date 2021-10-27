.. _ref_eql_strings:

Strings
-------

A string is a variable-length string of Unicode characters.

Literals
^^^^^^^^

A string can be declared with either single or double quotes.

.. code-block:: edgeql-repl

  db> select 'i ❤️ edgedb';
  {'hello there!'}
  db> select "i ❤️ edgedb";
  {'hello there!'}
  db> select 'i\n❤️\nedgedb';
  {'i
  ❤️
  edgedb'}


There is special syntax for declaring *raw strings*, which ignore escape characters like ``\n`` or ``\t``.

.. code-block:: edgeql-repl

  db> select r'i\n❤️\nedgedb'; # raw string
  {'i\\n❤️\\nedgedb'}
  db> select $$i
  ... ❤️
  ... edgedb$$; # multiline raw string
  {'i
  ❤️
  edgedb'}


Standard library
^^^^^^^^^^^^^^^^

EdgeQL contains a set of built-in functions and operators for searching, comparing, and manipulating strings.

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


Indexing and slicing
********************

.. code-block:: edgeql-repl

  db> select 'abcdef'[0];
  {'a'}
  db> select 'abcdef'[2:];
  {'cdef'}
  db> select 'abcdef'[1:5];
  {'bcde'}


Concatenation
*************


.. code-block:: edgeql-repl

  db> select 'abc' ++ 'def';
  {'abcdef'}


Utilities
*********

.. code-block:: edgeql-repl

  db> select len('abc');
  {3}
  db> select str_split('hello world', ' ');
  {['hello', 'world']}  # array of strings


Transformation functions
************************

.. code-block:: edgeql-repl

  db> select str_upper('abcdef'); # also: str_lower
  {'ABCDEF'}
  db> select str_title('hello world');
  {'Hello World'}
  db> select str_trim('  hello   ');
  {'hello'}

Comparison operators
********************

.. code-block:: edgeql-repl

  db> select 'aaa' = 'aaa'
  {true}
  db> select 'aaa' = 'bbb'
  {false}
  db> select 'aaa' < 'true'
  {false}

Search
******


.. code-block:: edgeql-repl

  db> select contains('hello world', 'hello');
  {true}
  db> select find('hello world', 'world');
  {6} # first occurence

Pattern matching and regex
**************************

.. code-block:: edgeql-repl

  db> select 'The Iliad' like 'the %';
  {false}
  db> select 'The Iliad' ilike 'the %';
  {true}
  db> SELECT re_match(r'edge\w+', 'I ❤️ edgeql');
  {['edgeql']}
  db> SELECT re_match_all(r'edge\w+', 'I ❤️ edgeql and edgedb');
  {['edgeql'], ['edgeqb']}
  db> SELECT re_replace(r'world', r'kitty', 'hello world')
  {'Goodbye '}
