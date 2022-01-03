.. _ref_eql_with:

With
====

All top-level EdgeQL statements (``select``, ``insert``, ``update``, and
``delete``) can be prefixed with a ``with`` block. These blocks contain
declarations of standalone expressions that can be used in your query.

.. code-block:: edgeql-repl

  db> with my_str := "hello world"
  ... select str_title(my_str);
  {'Hello World'}


The ``with`` clause can contain more than one variable. Earlier variables can
be referenced by later ones. Taken together, it becomes possible to write
"script-like" queries that execute several statements in sequence.

.. code-block:: edgeql-repl

  db> with a := 5,
  ...   b := 2,
  ...   c := a ^ b
  ... select c;
  {25}


Subqueries
^^^^^^^^^^

There's no limit to the complexity of computed expressions. EdgeQL is fully
composable; queries can be embedded inside each other simply. The following
query fetches a list of all movies featuring at least one of the original six
Avengers.

.. code-block:: edgeql-repl

  db> with avengers := (select Hero filter .name in {
  ...     'Iron Man',
  ...     'Black Widow',
  ...     'Captain America',
  ...     'Thor',
  ...     'Hawkeye',
  ...     'The Hulk'
  ...   })
  ... select Movie {title}
  ... filter avengers in .characters;
  {

    default::Movie {title: 'Iron Man'},
    default::Movie {title: 'The Incredible Hulk'},
    default::Movie {title: 'Iron Man 2'},
    default::Movie {title: 'Thor'},
    default::Movie {title: 'Captain America: The First Avenger'},
    ...
  }


Query parameters
^^^^^^^^^^^^^^^^

A common use case for ``with`` clauses is the initialization of :ref:`query
parameters <ref_eql_params>`.

.. code-block:: edgeql

  with user_id := <uuid>$user_id
  select User { name }
  filter .id = user_id;

For a full reference on using query parameters, see :ref:`EdgeQL > Parameters
<ref_eql_params>`.

Module selection
^^^^^^^^^^^^^^^^


By default, the *active module* is ``default``, so all schema objects inside
this module can be referenced by their *short name*, e.g. ``User``,
``BlogPost``, etc. To reference objects in other modules, we must use
fully-qualified names (``default::Hero``).

However, ``with`` clauses also provide a mechanism for changing the *active
module* on a per-query basis.

.. code-block:: edgeql-repl

  db> with module schema
  ... select ObjectType;

This ``with module`` clause changes the default module to schema, so we can
refer to ``schema::ObjectType`` (a built-in EdgeDB type) as simply
``ObjectType``.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > Commands > With <ref_eql_statements_with>`
