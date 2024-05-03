.. _ref_eql_statements_analyze:

.. versionadded:: 3.0

Analyze
=======

:eql-statement:

``analyze`` -- trigger performance analysis of the appended query

.. eql:synopsis::

    analyze <query>;

    # where <query> is any EdgeQL query


Description
-----------

``analyze`` returns a table with performance metrics broken down by node.

You may prepend the ``analyze`` keyword in either of our REPLs (CLI or :ref:`UI
<ref_cli_edgedb_ui>`) or you may prepend in the UI's query builder for a
helpful visualization of your query's performance.

After any ``analyze`` in a REPL, run the ``\expand`` command to see
fine-grained performance analysis of the previously analyzed query.


Example
-------

.. code-block:: edgeql-repl

  db> analyze select Hero {
  ...   name,
  ...   secret_identity,
  ...   villains: {
  ...     name,
  ...     nemesis: {
  ...       name
  ...     }
  ...   }
  ... };
  ──────────────────────────────────────── Query ────────────────────────────────────────
  analyze select ➊  Hero {name, secret_identity, ➋  villains: {name, ➌  nemesis: {name}}};

  ──────────────────────── Coarse-grained Query Plan ────────────────────────
                    │ Time     Cost Loops Rows Width │ Relations
  ➊ root            │  0.0 69709.48   1.0  0.0    32 │ Hero
  ╰──➋ .villains    │  0.0     92.9   0.0  0.0    32 │ Villain, Hero.villains
  ╰──➌ .nemesis     │  0.0     8.18   0.0  0.0    32 │ Hero


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`CLI > edgedb analyze <ref_cli_edgedb_analyze>`
  * - :ref:`EdgeQL > Analyze <ref_eql_analyze>`
