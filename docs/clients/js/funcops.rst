.. _edgedb-js-funcops:

Functions and Operators
-----------------------

Function syntax
^^^^^^^^^^^^^^^

All built-in standard library functions are reflected as functions in ``e``.

.. code-block:: typescript

  e.str_upper(e.str("hello"));
  // str_upper("hello")

  e.op(e.int64(2), '+', e.int64(2));
  // 2 + 2

  const nums = e.set(e.int64(3), e.int64(5), e.int64(7))
  e.op(e.int64(4), 'in', nums);
  // 4 in {3, 5, 7}

  e.math.mean(nums);
  // math::mean({3, 5, 7})


.. _edgedb-js-funcops-prefix:

Prefix operators
^^^^^^^^^^^^^^^^

Unlike functions, operators do *not* correspond to a top-level function on the
``e`` object. Instead, they are expressed with the ``e.op`` function.

Prefix operators operate on a single argument: ``OPERATOR <arg>``.

.. code-block:: typescript

  e.op('not', e.bool(true));      // not true
  e.op('exists', e.set('hi'));    // exists {'hi'}
  e.op('distinct', e.set('hi', 'hi'));    // distinct {'hi', 'hi'}

.. list-table::

  * - ``"exists"`` ``"distinct"`` ``"not"``


.. _edgedb-js-funcops-infix:

Infix operators
^^^^^^^^^^^^^^^

Infix operators operate on two arguments: ``<arg> OPERATOR <arg>``.

.. code-block:: typescript

  e.op(e.str('Hello '), '++', e.str('World!'));
  // 'Hello ' ++ 'World!'

.. list-table::

  * - ``"="`` ``"?="`` ``"!="`` ``"?!="`` ``">="`` ``">"`` ``"<="`` ``"<"``
      ``"or"`` ``"and"`` ``"+"`` ``"-"`` ``"*"`` ``"/"`` ``"//"`` ``"%"``
      ``"^"`` ``"in"`` ``"not in"`` ``"union"`` ``"??"`` ``"++"`` ``"like"``
      ``"ilike"`` ``"not like"`` ``"not ilike"``


.. _edgedb-js-funcops-ternary:

Ternary operators
^^^^^^^^^^^^^^^^^

Ternary operators operate on three arguments: ``<arg> OPERATOR <arg> OPERATOR
<arg>``. Currently there's only one ternary operator: the ``if else``
statement.

.. code-block:: typescript

  e.op(e.str('😄'), 'if', e.bool(true), 'else', e.str('😢'));
  // 😄 if true else 😢

