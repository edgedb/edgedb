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
product, you can accomplish it one of two ways. You could use
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

Or you could use :ref:`with <ref_eql_with>` to attach a different symbol to
your set of ``User`` objects.

.. code-block:: edgeql-repl

    edgedb> with U := (select User)
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

The reason ``with`` works here even though the alias ``U`` refers to the exact
same set is that we only assume you want the path factored in this way when you
use the same *symbol* to refer to a set. This means operations with
``User.first_name`` and ``User.last_name`` do get the common path factored
while ``U.first_name`` and ``User.last_name`` do not and are resolved with
cartesian multiplication.

Scopes
------

Scopes change the was path resolution works. Two sibling select queries — that
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

Clauses & Nesting
^^^^^^^^^^^^^^^^^

Most clauses are nested and are subjected to the same rules described above:
common symbols are factored and assumed to refer to the same object as the
outer query. This is because clauses like :ref:`filter
<ref_eql_select_filter>` and :ref:`order by <ref_eql_select_order>` need to
be applied to each row.

The :ref:`limit <ref_eql_select_pagination>` clause is not nested in the scope
because it needs to be applied globally to your query.
