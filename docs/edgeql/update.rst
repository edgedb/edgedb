.. _ref_eql_update:

Update
======

The ``update`` command is used to update existing objects.

.. code-block:: edgeql-repl

  db> update Hero
  ... filter .name = "Hawkeye"
  ... set { name := "Ronin" };
  {default::Hero {id: d476b12e-3e7b-11ec-af13-2717f3dc1d8a}}

If you omit the ``filter`` clause, all objects will be updated. This is useful
for updating values across all objects of a given type. The example below
cleans up all ``Hero.name`` values by trimming whitespace and converting them
to title case.

.. code-block:: edgeql-repl

  db> update Hero
  ... set { name := str_trim(str_title(.name)) };
  {default::Hero {id: d476b12e-3e7b-11ec-af13-2717f3dc1d8a}}

Syntax
^^^^^^

The structure of the ``update`` statement (``update...filter...set``) is an
intentional inversion of SQL's ``UPDATE...SET...WHERE`` syntax. Curiously, in
SQL, the ``where`` clauses typically occur *last* despite being applied before
the ``set`` statement. EdgeQL is structured to reflect this; first, a target
set is specified, then filters are applied, then the data is updated.

Updating properties
-------------------

To explicitly unset a property that is not required, set it to an empty set.

.. code-block:: edgeql

   update Person filter .id = <uuid>$id set { middle_name := {} };

Updating links
--------------

When updating links, the ``:=`` operator will *replace* the set of linked
values.

.. code-block:: edgeql-repl

  db> update movie
  ... filter .title = "Black Widow"
  ... set {
  ...  characters := (
  ...   select Person
  ...   filter .name in { "Black Widow", "Yelena", "Dreykov" }
  ...  )
  ... };
  {default::Title {id: af706c7c-3e98-11ec-abb3-4bbf3f18a61a}}
  db> select Movie { num_characters := count(.characters) }
  ... filter .title = "Black Widow";
  {default::Movie {num_characters: 3}}

To add additional linked items, use the ``+=`` operator.

.. code-block:: edgeql-repl

  db> update Movie
  ... filter .title = "Black Widow"
  ... set {
  ...  characters += (insert Villain {name := "Taskmaster"})
  ... };
  {default::Title {id: af706c7c-3e98-11ec-abb3-4bbf3f18a61a}}
  db> select Movie { num_characters := count(.characters) }
  ... filter .title = "Black Widow";
  {default::Movie {num_characters: 4}}

To remove items, use ``-=``.

.. code-block:: edgeql-repl

  db> update Movie
  ... filter .title = "Black Widow"
  ... set {
  ...  characters -= Villain # remove all villains
  ... };
  {default::Title {id: af706c7c-3e98-11ec-abb3-4bbf3f18a61a}}
  db> select Movie { num_characters := count(.characters) }
  ... filter .title = "Black Widow";
  {default::Movie {num_characters: 2}}

Returning data on update
------------------------

By default, ``update`` returns only the inserted object's ``id`` as seen in the
examples above. If you want to get additional data back, you may wrap your
``update`` with a ``select`` and apply a shape specifying any properties and
links you want returned:

.. code-block:: edgeql-repl

  db> select (update Hero
  ...   filter .name = "Hawkeye"
  ...   set { name := "Ronin" }
  ... ) {id, name};
  {
    default::Hero {
      id: d476b12e-3e7b-11ec-af13-2717f3dc1d8a,
      name: "Ronin"
    }
  }

With blocks
-----------

All top-level EdgeQL statements (``select``, ``insert``, ``update``, and
``delete``) can be prefixed with a ``with`` block. This is useful for updating
the results of a complex query.

.. code-block:: edgeql-repl

  db> with people := (
  ...     select Person
  ...     order by .name
  ...     offset 3
  ...     limit 3
  ...   )
  ... update people
  ... set { name := str_trim(.name) };
  {
    default::Hero {id: d4764c66-3e7b-11ec-af13-df1ba5b91187},
    default::Hero {id: d7d7e0f6-40ae-11ec-87b1-3f06bed494b9},
    default::Villain {id: d477a836-3e7b-11ec-af13-4fea611d1c31},
  }

.. note::

  You can pass any object-type expression into ``update``, including
  polymorphic ones (as above).

You can also use ``with`` to make returning additional data from an update more
readable:

.. code-block:: edgeql-repl

  db> with UpdatedHero := (update Hero
  ...   filter .name = "Hawkeye"
  ...   set { name := "Ronin" }
  ... )
  ... select UpdatedHero {
  ...   id,
  ...   name
  ... };
  {
    default::Hero {
      id: d476b12e-3e7b-11ec-af13-2717f3dc1d8a,
      name: "Ronin"
    }
  }


See also
--------

For documentation on performing *upsert* operations, see :ref:`EdgeQL > Insert
> Upserts <ref_eql_upsert>`.

.. list-table::

  * - :ref:`Reference > Commands > Update <ref_eql_statements_update>`
  * - :ref:`Cheatsheets > Updating data <ref_cheatsheet_update>`
  * - `Tutorial > Data Mutations > Update
      </tutorial/data-mutations/update>`_
