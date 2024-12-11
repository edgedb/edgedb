.. _ref_eql_path_resolution:

============
Path scoping
============

Beginning with EdgeDB 6.0, we are phasing out our historical (and
somewhat notorious)
:ref:`"path scoping" algorithm <ref_eql_old_path_resolution>`
in favor of a much simpler algorithm that nevertheless behaves
identically on *most* idiomatic EdgeQL queries.

EdgeDB 6.0 will contain features to support migration to and testing
of the new semantics.  We expect the migration to be relatively
painless for most users.

Discussion of rationale for this change is available in
`the RFC <rfc_>`_.


New path scoping
----------------

.. versionadded:: 6.0

When applying a shape to a path (or to a path that has shapes applied
to it already), the path will be be bound inside computed
pointers in that shape:

.. code-block:: edgeql-repl

    db> select User {
    ...   name := User.first_name ++ ' ' ++ User.last_name
    ... }
    {User {name: 'Peter Parker'}, User {name: 'Tony Stark'}}


When doing ``SELECT``, ``UPDATE``, or ``DELETE``, if the subject is a
path, optionally with shapes applied to it, the path will be
bound in ``FILTER`` and ``ORDER BY`` clauses:

.. code-block:: edgeql-repl

    db> select User {
    ...   name := User.first_name ++ ' ' ++ User.last_name
    ... }
    ... filter User.first_name = 'Peter'
    {User {name: 'Peter Parker'}}


However, when a path is used multiple times in "sibling" contexts,
a cross-product will be computed:

.. code-block:: edgeql-repl

    db> select User.first_name ++ ' ' ++ User.last_name;
    {'Peter Parker', 'Peter Stark', 'Tony Parker', 'Tony Stark'}


If you want to produce one value per ``User``, you can rewrite the query
with a ``FOR`` to make the intention explicit:

.. code-block:: edgeql-repl

    db> for u in User
    ... select u.first_name ++ ' ' ++ u.last_name;
    {'Peter Parker', 'Tony Stark'}

The most idiomatic way to fetch such data in EdgeQL, however,
remains:

.. code-block:: edgeql-repl

    db> select User { name := .first_name ++ ' ' ++ .last_name }
    {User {name: 'Peter Parker'}, User {name: 'Tony Stark'}}

(And, of course, you probably `shouldn't have first_name and last_name
properties anyway
<https://www.kalzumeus.com/2010/06/17/falsehoods-programmers-believe-about-names/>`_)


Path scoping configuration
--------------------------

.. versionadded:: 6.0

EdgeDB 6.0 introduces a new
:ref:`future feature <ref_datamodel_future>`
named ``simple_scoping`` alongside a
configuration setting also named ``simple_scoping``.  The future
feature presence will determine which behavior is used inside
expressions within the schema, as well as serve as the default value
if the configuration value is not set. The configuration setting will
allow overriding the presence or absence of the feature.

For concreteness, here are all of the posible combinations of whether
``using future simple_scoping`` is set and the value of the
configuration value ``simple_scoping``:

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - Future exists?
     - Config value
     - Query is simply scoped
     - Schema is simply scoped
   * - No
     - ``{}``
     - No
     - No
   * - No
     - ``true``
     - Yes
     - No
   * - No
     - ``false``
     - No
     - No
   * - Yes
     - ``{}``
     - Yes
     - Yes
   * - Yes
     - ``true``
     - Yes
     - Yes
   * - Yes
     - ``false``
     - No
     - Yes

Warning on old scoping
----------------------

.. versionadded:: 6.0

To make the migration process safer, we have also introduced a
``warn_old_scoping`` :ref:`future feature <ref_datamodel_future>` and
config setting.

When active, the server will emit a warning to the client when a query
is detected to depend on the old scoping behavior.  The behavior of
warnings can be configured in client bindings, but by default they are
logged.

The check is known to sometimes produce false positives, on queries
that will not actually have changed behavior, but is intended to not
have false negatives.

Recommended upgrade plan
------------------------

.. versionadded:: 6.0

The safest approach is to first get your entire schema and application
working with ``warn_old_scoping`` without producing any warnings. Once
that is done, it should be safe to switch to ``simple_scoping``
without changes in behavior.

If you are very confident in your test coverage, though, you can try
skipping dealing with ``warn_old_scoping`` and go straight to
``simple_scoping``.

There are many different potential migration strategies. One that
should work well:

1. Run ``CONFIGURE CURRENT DATABASE SET warn_old_scoping := true``
2. Try running all of your queries against the database.
3. Fix any that produce warnings.
4. Adjust your schema until setting ``using future warn_old_scoping`` works
   without producing warnings.

If you wish to proceed incrementally with steps 2 and 3, you can
configure ``warn_old_scoping`` in your clients, having it enabled for
queries that you have verified work with it and disabled for queries
that have not yet been verified or updated.


.. _ref_eql_old_path_resolution:

===================
Legacy path scoping
===================

This section describes the path scoping algorithm used exclusively
until EdgeDB 5.0 and by default in EdgeDB 6.0.
It will be removed in EdgeDB 7.0.

Element-wise operations with multiple arguments in EdgeDB are generally applied
to the :ref:`cartesian product <ref_reference_cardinality_cartesian>` of all
the input sets.

.. code-block:: edgeql-repl

    db> select {'aaa', 'bbb'} ++ {'ccc', 'ddd'};
    {'aaaccc', 'aaaddd', 'bbbccc', 'bbbddd'}

However, in cases where multiple element-wise arguments share a common path
(``User.`` in this example), EdgeDB factors out the common path rather than
using cartesian multiplication.

.. code-block:: edgeql-repl

    db> select User.first_name ++ ' ' ++ User.last_name;
    {'Mina Murray', 'Jonathan Harker', 'Lucy Westenra', 'John Seward'}

We assume this is what you want, but if your goal is to get the cartesian
product, you can accomplish it one of three ways. You could use
:eql:op:`detached`.

.. code-block:: edgeql-repl

    edgedb> select User.first_name ++ ' ' ++ detached User.last_name;
    {
      'Mina Murray',
      'Mina Harker',
      'Mina Westenra',
      'Mina Seward',
      'Jonathan Murray',
      'Jonathan Harker',
      'Jonathan Westenra',
      'Jonathan Seward',
      'Lucy Murray',
      'Lucy Harker',
      'Lucy Westenra',
      'Lucy Seward',
      'John Murray',
      'John Harker',
      'John Westenra',
      'John Seward',
    }

You could use :ref:`with <ref_eql_with>` to attach a different symbol to
your set of ``User`` objects.

.. code-block:: edgeql-repl

    edgedb> with U := User
    ....... select U.first_name ++ ' ' ++ User.last_name;
    {
      'Mina Murray',
      'Mina Harker',
      'Mina Westenra',
      'Mina Seward',
      'Jonathan Murray',
      'Jonathan Harker',
      'Jonathan Westenra',
      'Jonathan Seward',
      'Lucy Murray',
      'Lucy Harker',
      'Lucy Westenra',
      'Lucy Seward',
      'John Murray',
      'John Harker',
      'John Westenra',
      'John Seward',
    }

Or you could leverage the effect scopes have on path resolution. More on that
:ref:`in the Scopes section <ref_eql_path_resolution_scopes>`.

The reason ``with`` works here even though the alias ``U`` refers to the exact
same set is that we only assume you want the path factored in this way when you
use the same *symbol* to refer to a set. This means operations with
``User.first_name`` and ``User.last_name`` *do* get the common path factored
while ``U.first_name`` and ``User.last_name`` *do not* and are resolved with
cartesian multiplication.

That may leave you still wondering why ``U`` and ``User`` did not get a common
path factored. ``U`` is just an alias of ``select User`` and ``User`` is the
same symbol that we use in our name query. That's true, but EdgeDB doesn't
factor in this case because of the queries' scopes.

.. _ref_eql_path_resolution_scopes:

Scopes
------

Scopes change the way path resolution works. Two sibling select queries — that
is, queries at the same level — do not have their paths factored even when they
use a common symbol.

.. code-block:: edgeql-repl

    edgedb> select ((select User.first_name), (select User.last_name));
    {
      ('Mina', 'Murray'),
      ('Mina', 'Harker'),
      ('Mina', 'Westenra'),
      ('Mina', 'Seward'),
      ('Jonathan', 'Murray'),
      ('Jonathan', 'Harker'),
      ('Jonathan', 'Westenra'),
      ('Jonathan', 'Seward'),
      ('Lucy', 'Murray'),
      ('Lucy', 'Harker'),
      ('Lucy', 'Westenra'),
      ('Lucy', 'Seward'),
      ('John', 'Murray'),
      ('John', 'Harker'),
      ('John', 'Westenra'),
      ('John', 'Seward'),
    }

Common symbols in nested scopes *are* factored when they use the same symbol.
In this example, the nested queries both use the same ``User`` symbol as the
top-level query. As a result, the ``User`` in those queries refers to a single
object because it has been factored.

.. code-block:: edgeql-repl

    edgedb> select User {
    ....... name:= (select User.first_name) ++ ' ' ++ (select User.last_name)
    ....... };
    {
      default::User {name: 'Mina Murray'},
      default::User {name: 'Jonathan Harker'},
      default::User {name: 'Lucy Westenra'},
      default::User {name: 'John Seward'},
    }

If you have two common scopes and only *one* of them is in a nested scope, the
paths are still factored.

.. code-block:: edgeql-repl

    edgedb> select (Person.name, count(Person.friends));
    {('Fran', 3), ('Bam', 2), ('Emma', 3), ('Geoff', 1), ('Tyra', 1)}

In this example, ``count``, like all aggregate function, creates a nested
scope, but this doesn't prevent the paths from being factored as you can see
from the results. If the paths were *not* factored, the friend count would be
the same for all the result tuples and it would reflect the total number of
``Person`` objects that are in *all* ``friends`` links rather than the number
of ``Person`` objects that are in the named ``Person`` object's ``friends``
link.

If you have two aggregate functions creating *sibling* nested scopes, the paths
are *not* factored.

.. code-block:: edgeql-repl

    edgedb> select (array_agg(distinct Person.name), count(Person.friends));
    {(['Fran', 'Bam', 'Emma', 'Geoff'], 3)}

This query selects a tuple containing two nested scopes. Here, EdgeDB assumes
you want an array of all unique names and a count of the total number of people
who are anyone's friend.

Clauses & Nesting
^^^^^^^^^^^^^^^^^

Most clauses are nested and are subjected to the same rules described above:
common symbols are factored and assumed to refer to the same object as the
outer query. This is because clauses like :ref:`filter
<ref_eql_select_filter>` and :ref:`order by <ref_eql_select_order>` need to
be applied to each value in the result.

The :ref:`offset <ref_eql_select_pagination>` and
:ref:`limit <ref_eql_select_pagination>` clauses are not nested in the scope
because they need to be applied globally to the entire result set of your
query.

.. _rfc: https://github.com/edgedb/rfcs/blob/master/text/1027-no-factoring.rst
