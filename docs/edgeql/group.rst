.. _ref_eql_group:

Group #New
==========

.. note::

  The ``group`` statement is only available in EdgeDB 2.0 or later.

EdgeQL supports a top-level ``group`` statement. This is used to partition
sets into subsets based on some parameters. These subsets then can be
additionally aggregated to provide some analytics.

The most basic format is just using the bare :eql:stmt:`group` to group a set
of objects by some property:

.. code-block:: edgeql-repl

    db> group Movie by .release_year;
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

Notice that the result of ``group`` is a set of :ref:`free objects
<ref_eql_select_free_objects>` with three fields:

* ``key``: another free object containing the specific value of the
  grouping parameter for a given subset.
* ``grouping``: set of names of grouping parameters, i.e. the specific
  names that also appear in the ``key`` free object.
* ``elements``: the actual subset of values that match the ``key``.

In the ``group`` statement, referring to the property in the ``by`` clause
**must** be done by using the leading dot shothand ``.release_year``. The
property name then shows up in ``grouping`` and ``key`` to indicate the
defining characteristics of the particular result. Alternatively, we can give
it an alias in an optional ``using`` clause and then that alias can be used in
the ``by`` clause and will appear in the results:

.. code-block:: edgeql-repl

    db> group Movie {title}
    ... using year := .release_year by year;
    {
      {
        key: {year: 2016},
        grouping: {'year'},
        elements: {
          default::Movie {title: 'Captain America: Civil War'},
          default::Movie {title: 'Doctor Strange'},
        },
      },
      {
        key: {year: 2017},
        grouping: {'year'},
        elements: {
          default::Movie {title: 'Spider-Man: Homecoming'},
          default::Movie {title: 'Thor: Ragnarok'},
        },
      },
      {
        key: {year: 2018},
        grouping: {'year'},
        elements: {default::Movie {title: 'Ant-Man and the Wasp'}},
      },
      {
        key: {year: 2019},
        grouping: {'year'},
        elements: {default::Movie {title: 'Spider-Man: No Way Home'}},
      },
      {
        key: {year: 2021},
        grouping: {'year'},
        elements: {default::Movie {title: 'Black Widow'}},
      },
      ...
    }

The ``using`` clause is perfect for defining a more complex expression to
group things by. For example, instead of grouping by the ``release_year`` we
can group by the release decade:

.. code-block:: edgeql-repl

    db> group Movie {title}
    ... using decade := .release_year // 10
    ... by decade;
    {
    {
      {
        key: {decade: 200},
        grouping: {'decade'},
        elements: {
          default::Movie {title: 'Spider-Man'},
          default::Movie {title: 'Spider-Man 2'},
          default::Movie {title: 'Spider-Man 3'},
          default::Movie {title: 'Iron Man'},
          default::Movie {title: 'The Incredible Hulk'},
        },
      },
      {
        key: {decade: 201},
        grouping: {'decade'},
        elements: {
          default::Movie {title: 'Iron Man 2'},
          default::Movie {title: 'Thor'},
          default::Movie {title: 'Captain America: The First Avenger'},
          default::Movie {title: 'The Avengers'},
          default::Movie {title: 'Iron Man 3'},
          default::Movie {title: 'Thor: The Dark World'},
          default::Movie {title: 'Captain America: The Winter Soldier'},
          default::Movie {title: 'Ant-Man'},
          default::Movie {title: 'Captain America: Civil War'},
          default::Movie {title: 'Doctor Strange'},
          default::Movie {title: 'Spider-Man: Homecoming'},
          default::Movie {title: 'Thor: Ragnarok'},
          default::Movie {title: 'Ant-Man and the Wasp'},
          default::Movie {title: 'Spider-Man: No Way Home'},
        },
      },
      {
        key: {decade: 202},
        grouping: {'decade'},
        elements: {default::Movie {title: 'Black Widow'}},
      },
    }

It's also possible to group by more than one parameter, so we can group by
whether the movie ``title`` contains a colon *and* the decade it was released.
Additionally, let's only consider more recent movies, say, released after
2015, so that we're not overwhelmed by all the combination of results:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015)
    ... group M {title}
    ... using
    ...   decade := .release_year // 10,
    ...   has_colon := .title like '%:%'
    ... by decade, has_colon;
    {
      {
        key: {decade: 201, has_colon: false},
        grouping: {'decade', 'has_colon'},
        elements: {
          default::Movie {title: 'Ant-Man and the Wasp'},
          default::Movie {title: 'Doctor Strange'},
        },
      },
      {
        key: {decade: 201, has_colon: true},
        grouping: {'decade', 'has_colon'},
        elements: {
          default::Movie {title: 'Captain America: Civil War'},
          default::Movie {title: 'Spider-Man: No Way Home'},
          default::Movie {title: 'Thor: Ragnarok'},
          default::Movie {title: 'Spider-Man: Homecoming'},
        },
      },
      {
        key: {decade: 202, has_colon: false},
        grouping: {'decade', 'has_colon'},
        elements: {default::Movie {title: 'Black Widow'}},
      },
    }

Once we break a set into partitions, we can also use :ref:`aggregate
<ref_eql_set_aggregate>` functions to provide some analytics about the data.
For example, for the above partitioning (by decade and presence of ``:`` in
the ``title``) we can calculate how many movies are in each subset as well as
the average number of words in the movie titles:

.. code-block:: edgeql-repl

    db> with
    ...   # Apply the group query only to more recent movies
    ...   M := (select Movie filter .release_year > 2015),
    ...   groups := (
    ...     group M {title}
    ...     using
    ...       decade := .release_year // 10 - 200,
    ...       has_colon := .title like '%:%'
    ...     by decade, has_colon
    ...   )
    ... select groups {
    ...   key := .key {decade, has_colon},
    ...   count := count(.elements),
    ...   avg_words := math::mean(
    ...     len(str_split(.elements.title, ' ')))
    ... };
    {
      {key: {decade: 1, has_colon: false}, count: 2, avg_words: 3},
      {key: {decade: 1, has_colon: true}, count: 4, avg_words: 3},
      {key: {decade: 2, has_colon: false}, count: 1, avg_words: 2},
    }

.. note::

    It is possible to produce results that are grouped in multiple different
    ways using :ref:`grouping sets <ref_eql_statements_group>`. This may be
    useful in more sophisticated analytics.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > Commands > Group <ref_eql_statements_group>`
