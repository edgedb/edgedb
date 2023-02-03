.. versionadded:: 2.0

.. _ref_eql_statements_group:

Group
=====

:eql-statement:
:eql-haswith:

:index: group using by

.. note::

  The ``group`` statement is only available in EdgeDB 2.0 or later.

``group``--partition a set into subsets based on one or more keys

.. eql:synopsis::

    [ with <with-item> [, ...] ]

    group [<alias> := ] <expr>

    [ using <using-alias> := <expr>, [, ...] ]

    by <grouping-element>, ... ;

    # where a <grouping-element> is one of

      <ref-or-list>
      { <grouping-element>, ... }
      ROLLUP( <ref-or-list>, ... )
      CUBE( <ref-or-list>, ... )

    # where a <ref-or-list> is one of

      ()
      <grouping-ref>
      ( <grouping-ref>, ... )

    # where a <grouping-ref> is one of

      <using-alias>
      .<field-name>

:eql:synopsis:`group <expr>`
    The ``group`` clause sets up the input set that will be operated on.

    Much like in :eql:stmt:`select` it is possible to define an ad-hoc alias
    at this stage to make referring to the starting set concisely.

:eql:synopsis:`using <using-alias> := <expr>`
    The ``using`` clause defines one or more aliases which can then be used as
    part of the grouping key.

    If the :eql:synopsis:`by` clause only refers to
    :eql:synopsis:`.<field-name>` the ``using`` clause is optional.

:eql:synopsis:`by <grouping-element>`
    The ``by`` clause sepecifies which parameters will be used to partition
    the starting set.

    There are only two basic components for defining
    :eql:synopsis:`<grouping-element>`: references to
    :eql:synopsis:`<using-alias>` defined in the :eql:synopsis:`using` clause
    or by references to the short-path format of
    :eql:synopsis:`.<field-name>`. The :eql:synopsis:`.<field-name>` has to
    refer to properties or links immediately present on the type of starting
    set.

    The basic building blocks can also be combined by using parentheses ``(
    )`` to indicate that partitioning will happen based on several parameters
    at once.

    It is also possible to specify *grouping sets*, which are denoted using
    curly braces ``{ }``. The results will contain different partitioning
    based on each of the grouping set elements. When there are multiple
    top-level grouping-elements then the cartesian product of them is taken to
    determine the grouping set. Thus ``a, {b, c}`` is equivalent to ``{(a, b),
    (a, c)}`` grouping sets.

    :eql:synopsis:`ROLLUP` and :eql:synopsis:`CUBE` are a shorthand to specify
    particular grouping sets. :eql:synopsis:`ROLLUP` groups by all prefixes
    of a list of elements, so ``ROLLUP (a, b, c)`` is equivalent to ``{(),
    (a), (a, b), (a, b, c)}``. :eql:synopsis:`CUBE` groups by all elements of
    the power set, so ``CUBE (a, b)`` is equivalent to ``{(), (a), (b), (a,
    b)}``.


Output
------

The ``group`` statement partitions a starting set into subsets based on some
specified parameters. The output is organized into a set of :ref:`free objects
<ref_eql_select_free_objects>` of the following structure:

.. eql:synopsis::

    {
      "key": { <using-alias> := <value> [, ...] },
      "grouping": <set of keys used in grouping>,
      "elements": <the subset matching to the key>,
    }

:eql:synopsis:`"key"`
    The :eql:synopsis:`"key"` contains another :ref:`free object
    <ref_eql_select_free_objects>`, which contains all the aliases or field
    names used as the key together with the specific values these parameters
    take for this particular subset.

:eql:synopsis:`"grouping"`
    The :eql:synopsis:`"grouping"` contains a :eql:type:`str` set of all the
    names of the parameters used as the key for this particular subset. This
    is especially useful when using grouping sets and the parameters used in
    the key are not the same for all partitionings.

:eql:synopsis:`"elements"`
    The :eql:synopsis:`"elements"` contains the actual subset of values that
    match the :eql:synopsis:`"key"`.


Examples
--------

Here's a simple example without using any aggregation or any further
processing:

.. code-block:: edgeql-repl

    db> group Movie {title} by .release_year;
    {
      {
        key: {release_year: 2016},
        grouping: {'release_year'},
        elements: {
          default::Movie {title: 'Captain America: Civil War'},
          default::Movie {title: 'Doctor Strange'},
        },
      },
      {
        key: {release_year: 2017},
        grouping: {'release_year'},
        elements: {
          default::Movie {title: 'Spider-Man: Homecoming'},
          default::Movie {title: 'Thor: Ragnarok'},
        },
      },
      {
        key: {release_year: 2018},
        grouping: {'release_year'},
        elements: {default::Movie {title: 'Ant-Man and the Wasp'}},
      },
      {
        key: {release_year: 2019},
        grouping: {'release_year'},
        elements: {default::Movie {title: 'Spider-Man: No Way Home'}},
      },
      {
        key: {release_year: 2021},
        grouping: {'release_year'},
        elements: {default::Movie {title: 'Black Widow'}},
      },
      ...
    }

Or we can group by an expression instead, such as whether the title starts
with a vowel or not:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015)
    ... group M {title}
    ... using vowel := re_test('(?i)^[aeiou]', .title)
    ... by vowel;
    {
      {
        key: {vowel: false},
        grouping: {'vowel'},
        elements: {
          default::Movie {title: 'Thor: Ragnarok'},
          default::Movie {title: 'Doctor Strange'},
          default::Movie {title: 'Spider-Man: Homecoming'},
          default::Movie {title: 'Captain America: Civil War'},
          default::Movie {title: 'Black Widow'},
          default::Movie {title: 'Spider-Man: No Way Home'},
        },
      },
      {
        key: {vowel: true},
        grouping: {'vowel'},
        elements: {default::Movie {title: 'Ant-Man and the Wasp'}},
      },
    }

It is also possible to group scalars instead of objects, in which case you
need to define an ad-hoc alias to refer to the scalar set in order to specify
how it will be grouped:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015)
    ... group T := M.title
    ... using vowel := re_test('(?i)^[aeiou]', T)
    ... by vowel;
    {
      {
        key: {vowel: false},
        grouping: {'vowel'},
        elements: {
          'Captain America: Civil War',
          'Doctor Strange',
          'Spider-Man: Homecoming',
          'Thor: Ragnarok',
          'Spider-Man: No Way Home',
          'Black Widow',
        },
      },
      {
        key: {vowel: true},
        grouping: {'vowel'},
        elements: {'Ant-Man and the Wasp'}
      },
    }

Often the results of ``group`` are immediately used in a :eql:stmt:`select`
statement to provide some kind of analytical results:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015),
    ...   groups := (
    ...     group M {title}
    ...     using vowel := re_test('(?i)^[aeiou]', .title)
    ...     by vowel
    ...   )
    ... select groups {
    ...   starts_with_vowel := .key.vowel,
    ...   count := count(.elements),
    ...   mean_title_length :=
    ...     round(math::mean(len(.elements.title)))
    ... };
    {
      {starts_with_vowel: false, count: 6, mean_title_length: 18},
      {starts_with_vowel: true, count: 1, mean_title_length: 20},
    }

It's possible to group by more than one parameter. For example, we can add the
release decade to whether the ``title`` starts with a vowel:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015),
    ...   groups := (
    ...     group M {title}
    ...     using
    ...       vowel := re_test('(?i)^[aeiou]', .title),
    ...       decade := .release_year // 10
    ...     by vowel, decade
    ...   )
    ... select groups {
    ...   key := .key {vowel, decade},
    ...   count := count(.elements),
    ...   mean_title_length :=
    ...     math::mean(len(.elements.title))
    ... };
    {
      {
        key: {vowel: false, decade: 201},
        count: 5,
        mean_title_length: 19.8,
      },
      {
        key: {vowel: false, decade: 202},
        count: 1,
        mean_title_length: 11,
      },
      {
        key: {vowel: true, decade: 201},
        count: 1,
        mean_title_length: 20
      },
    }

Having more than one grouping parameter opens up the possibility to using
*grouping sets* to see the way grouping parameters interact with the analytics
we're gathering:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015),
    ...   groups := (
    ...     group M {title}
    ...     using
    ...       vowel := re_test('(?i)^[aeiou]', .title),
    ...       decade := .release_year // 10
    ...     by CUBE(vowel, decade)
    ...   )
    ... select groups {
    ...   key := .key {vowel, decade},
    ...   grouping,
    ...   count := count(.elements),
    ...   mean_title_length :=
    ...     (math::mean(len(.elements.title)))
    ... } order by array_agg(.grouping);
    {
      {
        key: {vowel: {}, decade: {}},
        grouping: {},
        count: 7,
        mean_title_length: 18.571428571428573,
      },
      {
        key: {vowel: {}, decade: 202},
        grouping: {'decade'},
        count: 1,
        mean_title_length: 11,
      },
      {
        key: {vowel: {}, decade: 201},
        grouping: {'decade'},
        count: 6,
        mean_title_length: 19.833333333333332,
      },
      {
        key: {vowel: true, decade: {}},
        grouping: {'vowel'},
        count: 1,
        mean_title_length: 20,
      },
      {
        key: {vowel: false, decade: {}},
        grouping: {'vowel'},
        count: 6,
        mean_title_length: 18.333333333333332,
      },
      {
        key: {vowel: false, decade: 201},
        grouping: {'vowel', 'decade'},
        count: 5,
        mean_title_length: 19.8,
      },
      {
        key: {vowel: true, decade: 201},
        grouping: {'vowel', 'decade'},
        count: 1,
        mean_title_length: 20,
      },
      {
        key: {vowel: false, decade: 202},
        grouping: {'vowel', 'decade'},
        count: 1,
        mean_title_length: 11,
      },
    }



.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Group <ref_eql_group>`
