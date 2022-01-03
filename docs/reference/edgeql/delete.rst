.. _ref_eql_statements_delete:

DELETE
======

:eql-statement:
:eql-haswith:

``DELETE`` -- remove objects from a database.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]

    DELETE <expr>

    [ FILTER <filter-expr> ]

    [ ORDER BY <order-expr> [direction] [THEN ...] ]

    [ OFFSET <offset-expr> ]

    [ LIMIT  <limit-expr> ] ;

:eql:synopsis:`WITH`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``DELETE``
    statement.  See :ref:`ref_eql_statements_with` for more information.

:eql:synopsis:`DELETE ...`
    The entire :eql:synopsis:`DELETE ...` statement is syntactic
    sugar for ``DELETE (SELECT ...)``. Therefore, the base
    :eql:synopsis:`<expr>` and the following :eql:synopsis:`FILTER`,
    :eql:synopsis:`ORDER BY`, :eql:synopsis:`OFFSET`, and
    :eql:synopsis:`LIMIT` clauses shape the set to
    be deleted the same way an explicit :ref:`SELECT
    <ref_eql_statements_select>` would.


Output
~~~~~~

On successful completion, a ``DELETE`` statement returns the set
of deleted objects.


Examples
~~~~~~~~

Here's a simple example of deleting a specific user:

.. code-block:: edgeql

    WITH MODULE example
    DELETE User
    FILTER User.name = 'Alice Smith';

And here's the equivalent ``DELETE (SELECT ...)`` statement:

.. code-block:: edgeql

    WITH MODULE example
    DELETE (SELECT User
            FILTER User.name = 'Alice Smith');

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Delete <ref_eql_delete>`
  * - :ref:`Cheatsheets > Deleting data <ref_cheatsheet_delete>`
