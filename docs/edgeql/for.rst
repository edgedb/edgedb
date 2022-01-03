.. _ref_eql_for:

For
===

EdgeQL supports a top-level ``for`` statement. These "for loops" iterate over
each element of some input set, execute some expression with it, and merge the
results into a single output set.

.. code-block:: edgeql-repl

  db> for number in {0, 1, 2, 3}
  ... union (
  ...   select { number, number + 0.5 }
  ... );
  {0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5}

This statements iterates through each number in the set. Inside the loop, the
``number`` variable is bound to a singleton set. The inner expression is
executed for every element of the input set, and the results of each execution
are merged into a single output set.

.. note::

  The ``union`` keyword is a required part of the ``for`` statement syntax; it
  is intended to indicate explicitly that the results of each loop execution
  are ultimately merged.

Bulk inserts
------------

The ``for`` statement is commonly used for bulk inserts.

.. code-block:: edgeql-repl

  db> for hero_name in {'Cersi', 'Ikaris', 'Thena'}
  ... union (
  ...   insert Hero { name := hero_name }
  ... );
  {
    default::Hero {id: d7d7e0f6-40ae-11ec-87b1-3f06bed494b9},
    default::Hero {id: d7d7f870-40ae-11ec-87b1-f712a4efc3a5},
    default::Hero {id: d7d7f8c0-40ae-11ec-87b1-6b8685d56610}
  }

This statements iterates through each name in the list of names. Inside the
loop, ``hero_name`` is bound to a ``str`` singleton, so it can be assigned to
``Hero.name``.

Instead of literal sets, it's common to use a :ref:`json <ref_std_json>`
parameter for bulk inserts. This value is then "unpacked" into a set of
``json`` elements and used inside the ``for`` loop:

.. code-block:: edgeql-repl

  db> with
  ...   raw_data := <json>$data,
  ... for item in json_array_unpack(raw_data) union (
  ...   insert Hero { name := <str>item['name'] }
  ... );
  Parameter <json>$data: [{"name":"Sersi"},{"name":"Ikaris"},{"name":"Thena"}]
  {
    default::Hero {id: d7d7e0f6-40ae-11ec-87b1-3f06bed494b9},
    default::Hero {id: d7d7f870-40ae-11ec-87b1-f712a4efc3a5},
    default::Hero {id: d7d7f8c0-40ae-11ec-87b1-6b8685d56610}
  }


A similar approach can be used for bulk updates.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > Commands > For <ref_eql_statements_for>`
