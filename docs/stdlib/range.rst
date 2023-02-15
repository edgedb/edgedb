.. _ref_std_range:

===========
Ranges #New
===========

:edb-alt-title: Range Functions and Operators

.. note::

  This type is only available in EdgeDB 2.0 or later.

Ranges represent some interval of values. The intervals can include or exclude
their boundaries or can even omit one or both boundaries. Only some scalar
types have corresponding range types:

- ``range<int32>``
- ``range<int64>``
- ``range<float32>``
- ``range<float64>``
- ``range<decimal>``
- ``range<datetime>``
- ``range<cal::local_datetime>``
- ``range<cal::local_date>``

Constructing ranges
^^^^^^^^^^^^^^^^^^^

There's a special :eql:func:`range` constructor function for making range
values. This is a little different from how scalars, arrays and tuples are
created typically in EdgeDB.

For example:

.. code-block:: edgeql-repl

    db> select range(1, 10);
    {range(1, 10, inc_lower := true, inc_upper := false)}
    db> select range(2.2, 3.3);
    {range(2.2, 3.3, inc_lower := true, inc_upper := false)}

Broadly there are two kinds of ranges: :eql:type:`discrete <anydiscrete>` and
:eql:type:`contiguous <anycontiguous>`. The discrete ranges are
``range<int32>``, ``range<int64>``, and ``range<cal::local_date>``. All ranges
over discrete types get normalized such that the lower bound is included
(if present) and the upper bound is excluded:

.. code-block:: edgeql-repl

    db> select range(1, 10) = range(1, 9, inc_upper := true);
    {true}
    db> select range(1, 10) = range(0, 10, inc_lower := false);
    {true}

Ranges over contiguous types don't have the same normalization mechanism
because the underlying types don't have granularity which could be used to
easily include or exclude a boundary value.

Sometimes a range cannot contain any values, this is called an *empty* range.
These kinds of ranges can arise from performing various operations on them,
but they can also be constructed. There are basically two equivalent ways of
constructing an *empty* range. It can be explicitly constructed by providing
the same upper and lower bounds and specifying that at least one of them is
not *inclusive* (which is the default for all range constructors):

.. code-block:: edgeql-repl

    db> select range(1, 1);
    {range({}, empty := true)}

Alternatively, it's possible to specify ``{}`` as a boundary and also provide
the ``empty := true`` named-only argument. If the empty set is provided as a
literal, it also needs to have a type cast, to specify which type of the range
is being constructed:

.. code-block:: edgeql-repl

    db> select range(<int64>{}, empty := true);
    {range({}, empty := true)}

Since empty ranges contain no values, they are all considered to be equal to
each other (as long as the types are compatible):

.. code-block:: edgeql-repl

    db> select range(1, 1) = range(<int64>{}, empty := true);
    {true}
    db> select range(1, 1) = range(42.0, 42.0);
    {true}

    db> select range(1, 1) = range(<cal::local_date>{}, empty := true);
    error: InvalidTypeError: operator '=' cannot be applied to operands of
    type 'range<std::int64>' and 'range<cal::local_date>'
      ┌─ query:1:8
      │
    1 │ select range(1, 1) = range(<cal::local_date>{}, empty := true);
      │        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      Consider using an explicit type cast or a conversion function.


JSON representation
^^^^^^^^^^^^^^^^^^^

Much like :ref:`arrays<ref_std_array>` and :ref:`tuples<ref_std_tuple>`, the
range types cannot be directly cast to a :eql:type:`str`, but instead can be
cast into a :eql:type:`json` structure:

.. code-block:: edgeql-repl

    db> select <json>range(1, 10);
    {"inc_lower": true, "inc_upper": false, "lower": 1, "upper": 10}

It's also possible to cast in the other direction - from :eql:type:`json` to a
specific range type:

.. code-block:: edgeql-repl

    db> select <range<int64>>to_json('{
    ...   "lower": 1,
    ...   "inc_lower": true,
    ...   "upper": 10,
    ...   "inc_upper": false
    ... }');
    {range(1, 10, inc_lower := true, inc_upper := false)}

Empty ranges have a shorthand :eql:type:`json` representation:

.. code-block:: edgeql-repl

    db> select <json>range(<int64>{}, empty := true);
    {"empty": true}

When casting from :eql:type:`json` to an empty range, all other fields may be
omitted, but if they are present, they must be consistent with an empty range:

.. code-block:: edgeql-repl

    db> select <range<int64>>to_json('{"empty": true}');
    {range({}, empty := true)}

    db> select <range<int64>>to_json('{
    ...   "lower": 1,
    ...   "inc_lower": true,
    ...   "upper": 1,
    ...   "inc_upper": false
    ... }');
    {range({}, empty := true)}

    db> select <range<int64>>to_json('{
    ...   "lower": 1,
    ...   "inc_lower": true,
    ...   "upper": 1,
    ...   "inc_upper": false,
    ...   "empty": true
    ... }');
    {range({}, empty := true)}

    db> select <range<int64>>to_json('{
    ...   "lower": 1,
    ...   "inc_lower": true,
    ...   "upper": 2,
    ...   "inc_upper": false,
    ...   "empty": true
    ... }');
    edgedb error: InvalidValueError: conflicting arguments in range
    constructor: "empty" is ``true`` while the specified bounds suggest
    otherwise

.. note::

  When casting from :eql:type:`json` to a range the ``lower`` and ``upper``
  fields are optional, but the *inclusivity* fields ``inc_lower`` and
  ``inc_upper`` are *mandatory*. This is to address the fact that whether the
  range boundaries are included by default can vary based on system or context
  and being explicit avoids subtle errors. The only exception to this are
  empty ranges that can have just the ``"empty": true`` field.


Functions and operators
^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
    :class: funcoptable

    * - :eql:op:`range \< range <rangelt>`
      - :eql:op-desc:`rangelt`
    * - :eql:op:`range \> range <rangegt>`
      - :eql:op-desc:`rangegt`
    * - :eql:op:`range \<= range <rangelteq>`
      - :eql:op-desc:`rangelteq`
    * - :eql:op:`range \>= range <rangegteq>`
      - :eql:op-desc:`rangegteq`
    * - :eql:op:`range + range <rangeplus>`
      - :eql:op-desc:`rangeplus`
    * - :eql:op:`range - range <rangeminus>`
      - :eql:op-desc:`rangeminus`
    * - :eql:op:`range * range <rangemult>`
      - :eql:op-desc:`rangemult`
    * - :eql:func:`range`
      - :eql:func-desc:`range`
    * - :eql:func:`range_get_lower`
      - :eql:func-desc:`range_get_lower`
    * - :eql:func:`range_get_upper`
      - :eql:func-desc:`range_get_upper`
    * - :eql:func:`range_is_inclusive_lower`
      - :eql:func-desc:`range_is_inclusive_lower`
    * - :eql:func:`range_is_inclusive_upper`
      - :eql:func-desc:`range_is_inclusive_upper`
    * - :eql:func:`range_is_empty`
      - :eql:func-desc:`range_is_empty`
    * - :eql:func:`range_unpack`
      - :eql:func-desc:`range_unpack`
    * - :eql:func:`contains`
      - Check if an element or a range is within another range.
    * - :eql:func:`overlaps`
      - :eql:func-desc:`overlaps`



Reference
^^^^^^^^^

.. eql:operator:: rangelt: range<anypoint> < range<anypoint> -> bool

    One range is before the other.

    Returns ``true`` if the lower bound of the first range is smaller than the
    lower bound of the second range. The unspecified lower bound is considered
    to be smaller than any specified lower bound. If the lower bounds are
    equal then the upper bounds are compared. Unspecified upper bound is
    considered to be greater than any specified upper bound.

    .. code-block:: edgeql-repl

        db> select range(1, 10) < range(2, 5);
        {true}
        db> select range(1, 10) < range(1, 15);
        {true}
        db> select range(1, 10) < range(1);
        {true}
        db> select range(1, 10) < range(<int64>{}, 10);
        {false}

    An empty range is considered to come before any non-empty range.

    .. code-block:: edgeql-repl

        db> select range(1, 10) < range(10, 10);
        {false}
        db> select range(1, 10) < range(<int64>{}, empty := true);
        {false}

    This is also how the ``order by`` clauses compares ranges.


----------


.. eql:operator:: rangegt: range<anypoint> > range<anypoint> -> bool

    One range is after the other.

    Returns ``true`` if the lower bound of the first range is greater than the
    lower bound of the second range. The unspecified lower bound is considered
    to be smaller than any specified lower bound. If the lower bounds are
    equal then the upper bounds are compared. Unspecified upper bound is
    considered to be greater than any specified upper bound.

    .. code-block:: edgeql-repl

        db> select range(1, 10) > range(2, 5);
        {false}
        db> select range(1, 10) > range(1, 5);
        {true}
        db> select range(1, 10) > range(1);
        {false}
        db> select range(1, 10) > range(<int64>{}, 10);
        {true}

    An empty range is considered to come before any non-empty range.

    .. code-block:: edgeql-repl

        db> select range(1, 10) > range(10, 10);
        {true}
        db> select range(1, 10) > range(<int64>{}, empty := true);
        {true}

    This is also how the ``order by`` clauses compares ranges.


----------


.. eql:operator:: rangelteq: range<anypoint> <= range<anypoint> -> bool

    One range is before or same as the other.

    Returns ``true`` if the ranges are identical or if the lower bound of the
    first range is smaller than the lower bound of the second range. The
    unspecified lower bound is considered to be smaller than any specified
    lower bound. If the lower bounds are equal then the upper bounds are
    compared. Unspecified upper bound is considered to be greater than any
    specified upper bound.

    .. code-block:: edgeql-repl

        db> select range(1, 10) <= range(1, 10);
        {true}
        db> select range(1, 10) <= range(2, 5);
        {true}
        db> select range(1, 10) <= range(1, 15);
        {true}
        db> select range(1, 10) <= range(1);
        {true}
        db> select range(1, 10) <= range(<int64>{}, 10);
        {false}

    An empty range is considered to come before any non-empty range.

    .. code-block:: edgeql-repl

        db> select range(1, 10) <= range(10, 10);
        {false}
        db> select range(1, 1) <= range(10, 10);
        {true}
        db> select range(1, 10) <= range(<int64>{}, empty := true);
        {false}

    This is also how the ``order by`` clauses compares ranges.


----------


.. eql:operator:: rangegteq: range<anypoint> >= range<anypoint> -> bool

    One range is after or same as the other.

    Returns ``true`` if the ranges are identical or if the lower bound of the
    first range is greater than the lower bound of the second range. The
    unspecified lower bound is considered to be smaller than any specified
    lower bound. If the lower bounds are equal then the upper bounds are
    compared. Unspecified upper bound is considered to be greater than any
    specified upper bound.

    .. code-block:: edgeql-repl

        db> select range(1, 10) >= range(2, 5);
        {false}
        db> select range(1, 10) >= range(1, 10);
        {true}
        db> select range(1, 10) >= range(1, 5);
        {true}
        db> select range(1, 10) >= range(1);
        {false}
        db> select range(1, 10) >= range(<int64>{}, 10);
        {true}

    An empty range is considered to come before any non-empty range.

    .. code-block:: edgeql-repl

        db> select range(1, 10) >= range(10, 10);
        {true}
        db> select range(1, 1) >= range(10, 10);
        {true}
        db> select range(1, 10) >= range(<int64>{}, empty := true);
        {true}

    This is also how the ``order by`` clauses compares ranges.


.. eql:operator:: rangeplus: range<anypoint> + range<anypoint> \
                    -> range<anypoint>

    :index: plus add

    Range union.

    Find the union of two ranges as long as the result is a single range
    without any discontinuities inside.

    .. code-block:: edgeql-repl

        db> select range(1, 10) + range(5, 15);
        {range(1, 15, inc_lower := true, inc_upper := false)}
        db> select range(1, 10) + range(5);
        {range(1, {}, inc_lower := true, inc_upper := false)}


----------


.. eql:operator:: rangeminus: range<anypoint> - range<anypoint> \
                    -> range<anypoint>

    :index: minus subtract

    Range subtraction.

    Subtract one range from another. This is only valid if the resulting range
    does not have any discontinuities inside.

    .. code-block:: edgeql-repl

        db> select range(1, 10) - range(5, 15);
        {range(1, 5, inc_lower := true, inc_upper := false)}
        db> select range(1, 10) - range(<int64>{}, 5);
        {range(5, 10, inc_lower := true, inc_upper := false)}
        db> select range(1, 10) - range(0, 15);
        {range({}, empty := true)}


----------


.. eql:operator:: rangemult: range<anypoint> * range<anypoint> \
                    -> range<anypoint>

    :index: intersect intersection

    Range intersection.

    Find the intersection of two ranges.

    .. code-block:: edgeql-repl

        db> select range(1, 10) * range(5, 15);
        {range(5, 10, inc_lower := true, inc_upper := false)}
        db> select range(1, 10) * range(-15, 15);
        {range(1, 10, inc_lower := true, inc_upper := false)}
        db> select range(1) * range(-15, 15);
        {range(1, 15, inc_lower := true, inc_upper := false)}
        db> select range(10) * range(<int64>{}, 1);
        {range({}, empty := true)}


----------


.. eql:function:: std::range(lower: optional std::anypoint = {}, \
                             upper: optional std::anypoint = {}, \
                             named only inc_lower: bool = true, \
                             named only inc_upper: bool = false, \
                             named only empty: bool = false) \
                    -> range<std::anypoint>

    Construct a range.

    Either one of *lower* or *upper* bounds can be set to ``{}`` to indicate
    an unbounded interval.

    By default the *lower* bound is included and the *upper* bound is excluded
    from the range, but this can be controlled explicitly via the *inc_lower*
    and *inc_upper* named-only arguments.

    .. code-block:: edgeql-repl

        db> select range(1, 10);
        {range(1, 10, inc_lower := true, inc_upper := false)}
        db> select range(1.5, 7.5, inc_lower := false);
        {range(1.5, 7.5, inc_lower := false, inc_upper := false)}

    Finally, an empty range can be created by using the *empty* named-only
    flag. The first argument still needs to be passed as an ``{}`` so that the
    type of the range can be inferred from it.

    .. code-block:: edgeql-repl

        db> select range(<int64>{}, empty := true);
        {range({}, empty := true)}


----------


.. eql:function:: std::range_get_lower(r: range<anypoint>) \
                    -> optional anypoint

    Return lower bound value.

    Return the lower bound of the specified range.

    .. code-block:: edgeql-repl

        db> select range_get_lower(range(1, 10));
        {1}
        db> select range_get_lower(range(1.5, 7.5));
        {1.5}


----------


.. eql:function:: std::range_is_inclusive_lower(r: range<anypoint>) \
                    -> std::bool

    Check whether lower bound is inclusive.

    Return ``true`` if the lower bound is inclusive and ``false`` otherwise.
    If there is no lower bound, then it is never considered inclusive.

    .. code-block:: edgeql-repl

        db> select range_is_inclusive_lower(range(1, 10));
        {true}
        db> select range_is_inclusive_lower(
        ...     range(1.5, 7.5, inc_lower := false));
        {false}
        db> select range_is_inclusive_lower(range(<int64>{}, 10));
        {false}


----------


.. eql:function:: std::range_get_upper(r: range<anypoint>) \
                    -> optional anypoint

    Return upper bound value.

    Return the upper bound of the specified range.

    .. code-block:: edgeql-repl

        db> select range_get_upper(range(1, 10));
        {10}
        db> select range_get_upper(range(1.5, 7.5));
        {7.5}


----------


.. eql:function:: std::range_is_inclusive_upper(r: range<anypoint>) \
                    -> std::bool

    Check whether upper bound is inclusive.

    Return ``true`` if the upper bound is inclusive and ``false`` otherwise.
    If there is no upper bound, then it is never considered inclusive.

    .. code-block:: edgeql-repl

        db> select range_is_inclusive_upper(range(1, 10));
        {false}
        db> select range_is_inclusive_upper(
        ...     range(1.5, 7.5, inc_upper := true));
        {true}
        db> select range_is_inclusive_upper(range(1));
        {false}


----------


.. eql:function:: std::range_is_empty(val: range<anypoint>) \
                    -> bool

    Check whether a range is empty.

    Return ``true`` if the range contains no values and ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select range_is_empty(range(1, 10));
        {false}
        db> select range_is_empty(range(1, 1));
        {true}
        db> select range_is_empty(range(<int64>{}, empty := true));
        {true}


----------


.. eql:function:: std::range_unpack(val: range<anydiscrete>) \
                    -> set of anydiscrete
                  std::range_unpack(val: range<anypoint>, step: anypoint) \
                    -> set of anypoint

    Return values from a range.

    For a range of discrete values this function when called without
    indicating a *step* value simply produces a set of all the values within
    the range, in order.

    .. code-block:: edgeql-repl

        db> select range_unpack(range(1, 10));
        {1, 2, 3, 4, 5, 6, 7, 8, 9}
        db> select range_unpack(range(
        ...   <cal::local_date>'2022-07-01',
        ...   <cal::local_date>'2022-07-10'));
        {
          <cal::local_date>'2022-07-01',
          <cal::local_date>'2022-07-02',
          <cal::local_date>'2022-07-03',
          <cal::local_date>'2022-07-04',
          <cal::local_date>'2022-07-05',
          <cal::local_date>'2022-07-06',
          <cal::local_date>'2022-07-07',
          <cal::local_date>'2022-07-08',
          <cal::local_date>'2022-07-09',
        }

    For any range type a *step* value can be specified. Then the values will
    be picked from the range, starting at the lower boundary (skipping the
    boundary value itself if it's not included in the range) and then
    producing the next value by adding the *step* to the previous one.

    .. code-block:: edgeql-repl

        db> select range_unpack(range(1.5, 7.5), 0.7);
        {1.5, 2.2, 2.9, 3.6, 4.3, 5, 5.7, 6.4}
        db> select range_unpack(
        ...   range(
        ...     <cal::local_datetime>'2022-07-01T00:00:00',
        ...     <cal::local_datetime>'2022-12-01T00:00:00'
        ...   ),
        ...   <cal::relative_duration>'25 days 5 hours');
        {
          <cal::local_datetime>'2022-07-01T00:00:00',
          <cal::local_datetime>'2022-07-26T05:00:00',
          <cal::local_datetime>'2022-08-20T10:00:00',
          <cal::local_datetime>'2022-09-14T15:00:00',
          <cal::local_datetime>'2022-10-09T20:00:00',
          <cal::local_datetime>'2022-11-04T01:00:00',
        }


----------


.. eql:function:: std::overlaps(l: range<anypoint>, r: range<anypoint>) \
                    -> std::bool

    Check whether ranges overlap.

    Return ``true`` if the ranges have any elements in common and ``false``
    otherwise.

    .. code-block:: edgeql-repl

        db> select overlaps(range(1, 10), range(5));
        {true}
        db> select overlaps(range(1, 10), range(10));
        {false}
