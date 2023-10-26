.. _ref_eql_path_resolution:

===============
Path resolution
===============

Element-wise operations with multiple arguments in EdgeDB are generally applied
to the :ref:`cartesian product <ref_reference_cardinality_cartesian>` of all
the input sets.

.. code-block:: edgeql-repl

    db> select {'aaa', 'bbb'} ++ {'ccc', 'ddd'};
    {'aaaccc', 'aaaddd', 'bbbccc', 'bbbddd'}

In some cases, this works out fine, but in others it doesn't make sense. Take
this example:

.. code-block:: edgeql

    select User.first_name ++ ' ' ++ User.last_name;

Should the result of this query be every ``first_name`` in your ``User``
objects concatenated with every ``last_name``? That's probably not what you
want.

This is why, in cases where multiple element-wise arguments share a common path
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
