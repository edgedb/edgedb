.. _ref_eql_sets:

Sets
====

.. _ref_eql_everything_is_a_set:

Everything is a set
-------------------

All values in EdgeQL are actually **sets**: a collection of values of a given
**type**. All elements of a set must have the same type. The number of items in
a set is known as its **cardinality**. A set with a cardinality of zero is
referred to as an **empty set**. A set with a cardinality of one is known as a
**singleton**.

.. _ref_eql_set_constructor:

Constructing sets
-----------------

Set literals are declared with *set constructor* syntax: a comma-separated
list of values inside a set of ``{curly braces}``.

.. code-block:: edgeql-repl

  db> select {"set", "of", "strings"};
  {"set", "of", "strings"}
  db> select {1, 2, 3};
  {1, 2, 3}

In actuality, curly braces are a syntactic sugar for the
:eql:op:`union` operator. The  previous examples are perfectly
equivalent to the following:

.. code-block:: edgeql-repl

  db> select "set" union "of" union "strings";
  {"set", "of", "strings"}
  db> select 1 union 2 union 3;
  {1, 2, 3}

A consequence of this is that nested sets are *flattened*.

.. code-block:: edgeql-repl

  db> select {1, {2, {3, 4}}};
  {1, 2, 3, 4}
  db> select 1 union (2 union (3 union 4));
  {1, 2, 3, 4}

All values in a set must have the same type. For convenience, EdgeDB will
*implicitly cast* values to other types, as long as there is no loss of
information (e.g. converting a ``int16`` to an ``int64``). For a full
reference, see the casting table in :ref:`Standard Library > Casts
<ref_eql_casts_table>`.

.. code-block:: edgeql-repl

  db> select {1, 1.5};
  {1.0, 1.5}
  db> select {1, 1234.5678n};
  {1.0n, 1234.5678n}

Attempting to declare a set containing elements of *incompatible* types is not
permitted.

.. code-block:: edgeql-repl

  db> select {"apple", 3.14};
  error: QueryError: set constructor has arguments of incompatible types
  'std::str' and 'std::float64'

.. note::

  Types are considered *compatible* if one can be implicitly cast into the
  other. For reference on implicit castability, see :ref:`Standard Library >
  Casts <ref_eql_casts_table>`.

.. _ref_eql_set_literals_are_singletons:

Literals are singletons
-----------------------

Literal syntax like ``6`` or ``"hello world"`` is just a shorthand for
declaring a *singleton* of a given type. This is why the literals we created in
the previous section were printed inside braces: to indicate that these values
are *actually sets*.

.. code-block:: edgeql-repl

  db> select 6;
  {6}
  db> select "hello world";
  {"hello world"}

Wrapping a literal in curly braces does not change the meaning of the
expression. For instance, ``"hello world"`` is *exactly equivalent* to
``{"hello world"}``.

.. code-block:: edgeql-repl

  db> select {"hello world"};
  {"hello world"}
  db> select "hello world" = {"hello world"};
  {true}


You can retrieve the cardinality of a set with the :eql:func:`count` function.

.. code-block:: edgeql-repl

  db> select count('aaa');
  {1}
  db> select count({'aaa', 'bbb'});
  {2}


.. _ref_eql_empty_sets:

Empty sets
----------

The reason EdgeQL introduced the concept of *sets* is to eliminate the concept
of ``null``. In SQL databases ``null`` is a special value denoting the absence
of data; in EdgeDB the absence of data is just an empty set.

.. note::

  Why is the existence of NULL a problem? Put simply, it's an edge case that
  permeates all of SQL and is often handled inconsistently in different
  circumstances. A number of specific inconsistencies are documented in detail
  in the `We Can Do Better Than SQL
  <https://www.edgedb.com/blog/we-can-do-better-than-sql#null-a-bag-of-surprises>`_
  post on the EdgeDB blog. For broader context, see Tony Hoare's talk
  `"The Billion Dollar Mistake" <https://bit.ly/3H238oG>`_.


Declaring empty sets isn't as simple as ``{}``; in EdgeQL, all expressions are
*strongly typed*, including empty sets. With nonempty sets (like ``{1, 2, 3}``)
, the type is inferred from the set's contents (``int64``). But with empty sets
this isn't possible, so an *explicit cast* is required.

.. code-block:: edgeql-repl

  db> select {};
  error: QueryError: expression returns value of indeterminate type
    ┌─ query:1:8
    │
  1 │ select {};
    │        ^^ Consider using an explicit type cast.

  db> select <int64>{};
  {}
  db> select <str>{};
  {}
  db> select count(<str>{});
  {0}

You can check whether or not a set is *empty* with the :eql:op:`exists`
operator.

.. code-block:: edgeql-repl

  db> select exists <str>{};
  {false}
  db> select exists {'not', 'empty'};
  {true}


.. _ref_eql_set_references:

Set references
--------------

A set reference is a *pointer* to a set of values. Most commonly, this is the
name of an :ref:`object type <ref_datamodel_object_types>` you've declared in
your schema.

.. code-block:: edgeql-repl

  db> select User;
  {
    default::User {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::User {id: b0e0dd0c-35e8-11ec-acc3-abf1752973be},
  }
  db> select count(User);
  {2}

It may also be an *alias*, which can be defined in a :ref:`with block
<ref_eql_with>` or as an :ref:`alias declaration <ref_eql_sdl_aliases>` in your
schema.

.. note::

  In the example above, the ``User`` object type was declared inside the
  ``default`` module. If it was in a non-``default`` module (say,
  ``my_module``, we would need to use its *fully-qualified* name.

  .. code-block:: edgeql-repl

    db> select my_module::User;


.. _ref_eql_set_distinct:

Multisets
---------

Technically sets in EdgeDB are actually *multisets*, because they can contain
duplicates of the same element. To eliminate duplicates, use the
:eql:op:`distinct` set operator.

.. code-block:: edgeql-repl

  db> select {'aaa', 'aaa', 'aaa'};
  {'aaa', 'aaa', 'aaa'}
  db> select distinct {'aaa', 'aaa', 'aaa'};
  {'aaa'}

.. _ref_eql_set_in:

Checking membership
-------------------

Use the :eql:op:`in` operator to check whether a set contains a particular
element.

.. code-block:: edgeql-repl

  db> select 'aaa' in {'aaa', 'bbb', 'ccc'};
  {true}
  db> select 'ddd' in {'aaa', 'bbb', 'ccc'};
  {false}


.. _ref_eql_set_union:

Merging sets
------------

Use the :eql:op:`union` operator to merge two sets.

.. code-block:: edgeql-repl

  db> select 'aaa' union 'bbb' union 'ccc';
  {'aaa', 'bbb', 'ccc'}
  db> select {1, 2} union {3.1, 4.4};
  {1.0, 2.0, 3.1, 4.4}

Finding common members
----------------------

.. versionadded:: 3.0

Use the :eql:op:`intersect` operator to find common members between two sets.

.. code-block:: edgeql-repl

    db> select {1, 2, 3, 4, 5} intersect {3, 4, 5, 6, 7};
    {3, 5, 4}
    db> select {'a', 'b', 'c', 'd', 'e'} intersect {'c', 'd', 'e', 'f', 'g'};
    {'e', 'd', 'c'}

If set members are repeated in both sets, they will be repeated in the set
produced by :eql:op:`intersect` the same number of times they are repeated in
both of the operand sets.

.. code-block:: edgeql-repl

    db> select {0, 1, 1, 1, 2, 3, 3} intersect {1, 3, 3, 3, 3, 3};
    {1, 3, 3}

In this example, ``1`` appears three times in the first set but only once in
the second, so it appears only once in the result. ``3`` appears twice in the
first set and five times in the second. Both ``3`` appearances in the first set
are overlapped by ``3`` appearances in the second, so they both end up in the
resulting set.


Removing common members
-----------------------

.. versionadded:: 3.0

Use the :eql:op:`except` operator to leave only the members in the first set
that do not appear in the second set.

.. code-block:: edgeql-repl

    db> select {1, 2, 3, 4, 5} except {3, 4, 5, 6, 7};
    {1, 2}
    db> select {'a', 'b', 'c', 'd', 'e'} except {'c', 'd', 'e', 'f', 'g'};
    {'b', 'a'}

When :eql:op:`except` eliminates a common member that is repeated, it never
eliminates more than the number of instances of that member appearing in the
second set.

.. code-block:: edgeql-repl

    db> select {0, 1, 1, 1, 2, 3, 3} except {1, 3, 3, 3, 3, 3};
    {0, 1, 1, 2}

In this example, both sets share the member ``1``. The first set contains three
of them while the second contains only one. The result retains two ``1``
members from the first set since the sets shared only a single ``1`` in common.
The second set has five ``3`` members to the first set's two, so both of the
first set's ``3`` members are eliminated from the resulting set.


.. _ref_eql_set_coalesce:

Coalescing
----------

Occasionally in queries, you need to handle the case where a set is empty. This
can be achieved with a coalescing operator :eql:op:`?? <coalesce>`. This is
commonly used to provide default values for optional :ref:`query parameters
<ref_eql_params>`.

.. code-block:: edgeql-repl

  db> select 'value' ?? 'default';
  {'value'}
  db> select <str>{} ?? 'default';
  {'default'}

.. note::

  Coalescing is an example of a function/operator with :ref:`optional inputs
  <ref_sdl_function_typequal>`. By default, passing an empty set into a
  function/operator will "short circuit" the operation and return an empty set.
  However it's possible to mark inputs as *optional*, in which case the
  operation will be defined over empty sets. Another example is
  :eql:func:`count`, which returns ``{0}`` when an empty set is passed as
  input.

.. _ref_eql_set_type_filter:

Inheritance
-----------

EdgeDB schemas support :ref:`inheritance <ref_datamodel_objects_inheritance>`;
types (usually object types) can extend one or more other types. For instance
you may declare an abstract object type ``Media`` that is extended by ``Movie``
and ``TVShow``.

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Media {
      required property title -> str;
    }

    type Movie extending Media {
      property release_year -> int64;
    }

    type TVShow extending Media {
      property num_seasons -> int64;
    }

.. code-block:: sdl

    abstract type Media {
      required title: str;
    }

    type Movie extending Media {
      release_year: int64;
    }

    type TVShow extending Media {
      num_seasons: int64;
    }

A set of type ``Media`` may contain both ``Movie`` and ``TVShow``
objects.

.. code-block:: edgeql-repl

  db> select Media;
  {
    default::Movie {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::Movie {id: 3bfe4900-3743-11ec-90ee-cb73d2740820},
    default::TVShow {id: b0e0dd0c-35e8-11ec-acc3-abf1752973be},
  }

We can use the *type intersection* operator ``[is <type>]`` to restrict the
elements of a set by subtype.

.. code-block:: edgeql-repl

  db> select Media[is Movie];
  {
    default::Movie {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::Movie {id: 3bfe4900-3743-11ec-90ee-cb73d2740820},
  }
  db> select Media[is TVShow];
  {
    default::TVShow {id: b0e0dd0c-35e8-11ec-acc3-abf1752973be}
  }

Type filters are commonly used in conjunction with :ref:`backlinks
<ref_eql_select_backlinks>`.

.. _ref_eql_set_aggregate:

Aggregate vs element-wise operations
------------------------------------

EdgeQL provides a large library of built-in functions and operators for
handling data structures. It's useful to consider functions/operators as either
*aggregate* or *element-wise*.

.. note::

  This is an over-simplification, but it's a useful mental model when just
  starting out with EdgeDB. For a more complete guide, see :ref:`Reference >
  Cardinality <ref_reference_cardinality>`.

*Aggregate* operations are applied to the set *as a whole*; they
accept a set with arbitrary cardinality and return a *singleton* (or perhaps an
empty set if the input was also empty).

.. code-block:: edgeql-repl

  db> select count({'aaa', 'bbb'});
  {2}
  db> select sum({1, 2, 3});
  {6}
  db> select min({1, 2, 3});
  {1}

Element-wise operations are applied on *each element* of a set.

.. code-block:: edgeql-repl

  db> select str_upper({'aaa', 'bbb'});
  {'AAA', 'BBB'}
  db> select {1, 2, 3} ^ 2;
  {1, 4, 9}
  db> select str_split({"hello world", "hi again"}, " ");
  {["hello", "world"], ["hi", "again"]}

When an *element-wise* operation accepts two or more inputs, the operation is
applied to all possible combinations of inputs; in other words, the operation
is applied to the *Cartesian product* of the inputs.

.. code-block:: edgeql-repl

  db> select {'aaa', 'bbb'} ++ {'ccc', 'ddd'};
  {'aaaccc', 'aaaddd', 'bbbccc', 'bbbddd'}

Accordingly, operations involving an empty set typically return an empty set.
In constrast, aggregate operations like :eql:func:`count` are able to operate
on empty sets.

.. code-block:: edgeql-repl

  db> select <str>{} ++ 'ccc';
  {}
  db> select count(<str>{});
  {0}

For a more complete discussion of cardinality, see :ref:`Reference >
Cardinality <ref_reference_cardinality>`.

.. _ref_eql_set_array_conversion:

Conversion to/from arrays
-------------------------

Both arrays and sets are collections of values that share a type. EdgeQL
provides ways to convert one into the other.

.. note::

  Remember that *all values* in EdgeQL are sets; an array literal is just a
  singleton set of arrays. So here, "converting" a set into an array means
  converting a set of type ``x`` into another set with cardinality
  ``1`` (a singleton) and type ``array<x>``.

.. code-block:: edgeql-repl

  db> select array_unpack([1,2,3]);
  {1, 2, 3}
  db> select array_agg({1,2,3});
  {[1, 2, 3]}

Arrays are an *ordered collection*, whereas sets are generally unordered
(unless explicitly sorted with an ``order by`` clause in a :ref:`select
<ref_eql_select_order>` statement).

Element-wise scalar operations in the standard library cannot be applied to
arrays, so sets of scalars are typically easier to manipulate, search, and
transform than arrays.

.. code-block:: edgeql-repl

  db> select str_trim({'  hello', 'world  '});
  {'hello', 'world'}
  db> select str_trim(['  hello', 'world  ']);
  error: QueryError: function "str_trim(arg0: array<std::str>)" does not exist

Most :ref:`aggregate <ref_reference_cardinality_aggregate>` operations have
analogs that operate on arrays. For instance, the set function
:eql:func:`count` is analogous to the array function :eql:func:`len`.


Reference
---------

.. list-table::

  * - Set operators
    - :eql:op:`distinct` :eql:op:`in` :eql:op:`union`
      :eql:op:`exists` :eql:op:`if..else`
      :eql:op:`?? <coalesce>` :eql:op:`detached`
      :eql:op:`[is type] <isintersect>`
  * - Utility functions
    - :eql:func:`count` :eql:func:`enumerate`
  * - Cardinality assertion
    - :eql:func:`assert_distinct` :eql:func:`assert_single`
      :eql:func:`assert_exists`

.. list-table::
  :class: seealso

  * - **See also**
  * - `Tutorial > Building Blocks > Sets
      </tutorial/building-blocks/working-with-sets>`_
