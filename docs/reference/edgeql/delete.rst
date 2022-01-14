.. _ref_eql_statements_delete:

Delete
======

:eql-statement:
:eql-haswith:

``delete`` -- remove objects from a database.

.. eql:synopsis::

    [ with <with-item> [, ...] ]

    delete <expr>

    [ filter <filter-expr> ]

    [ order by <order-expr> [direction] [then ...] ]

    [ offset <offset-expr> ]

    [ limit  <limit-expr> ] ;

:eql:synopsis:`with`
    Alias declarations.

    The ``with`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``delete``
    statement.  See :ref:`ref_eql_statements_with` for more information.

:eql:synopsis:`delete ...`
    The entire :eql:synopsis:`delete ...` statement is syntactic
    sugar for ``delete (select ...)``. Therefore, the base
    :eql:synopsis:`<expr>` and the following :eql:synopsis:`filter`,
    :eql:synopsis:`order by`, :eql:synopsis:`offset`, and
    :eql:synopsis:`limit` clauses shape the set to
    be deleted the same way an explicit :eql:stmt:`select` would.


Output
~~~~~~

On successful completion, a ``delete`` statement returns the set
of deleted objects.


Examples
~~~~~~~~

Here's a simple example of deleting a specific user:

.. code-block:: edgeql

    with module example
    delete User
    filter User.name = 'Alice Smith';

And here's the equivalent ``delete (select ...)`` statement:

.. code-block:: edgeql

    with module example
    delete (select User
            filter User.name = 'Alice Smith');

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Delete <ref_eql_delete>`
  * - :ref:`Cheatsheets > Deleting data <ref_cheatsheet_delete>`
