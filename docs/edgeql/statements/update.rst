.. _ref_eql_statements_update:

UPDATE
======

:eql-statement:
:eql-haswith:

``UPDATE`` -- update objects in a database

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]

    UPDATE <selector-expr>

    [ FILTER <filter-expr> ]

    SET <shape> ;

``UPDATE`` changes the values of the specified links in all objects
selected by *update-selector-expr* and, optinally, filtered by
*filter-expr*.

:eql:synopsis:`WITH`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``UPDATE``
    statement.  See :ref:`ref_eql_with` for more information.

:eql:synopsis:`UPDATE <selector-expr>`
    An arbitrary expression returning a set of objects to be updated.

:eql:synopsis:`FILTER <filter-expr>`
    An expression of type :eql:type:`bool` used to filter the
    set of updated objects.

    :eql:synopsis:`<filter-expr>` is an expression that has a result
    of type :eql:type:`bool`.  Only objects that satisfy the filter
    expression will be updated.  See the description of the
    ``FILTER`` clause of the :eql:stmt:`SELECT` statement for more
    information.

:eql:synopsis:`SET <shape>`
    A :ref:`shape <ref_eql_expr_shapes_update>` expression with
    the new values for the links of the updated object.

    .. eql:synopsis::

        SET { <link> := <update-expr> [, ...] }

Output
~~~~~~

On successful completion, an ``UPDATE`` statement returns the
set of updated objects.


Examples
~~~~~~~~

Here are a couple of examples of using the ``UPDATE`` statement:

.. code-block:: edgeql

    # update the user with the name 'Alice Smith'
    WITH MODULE example
    UPDATE User
    FILTER .name = 'Alice Smith'
    SET {
        name := 'Alice J. Smith'
    };

    # update all users whose name is 'Bob'
    WITH MODULE example
    UPDATE User
    FILTER .name LIKE 'Bob%'
    SET {
        name := User.name + '*'
    };

The statement ``FOR <x> IN <expr>`` allows to express certain bulk
updates more clearly. See
:ref:`Usage of FOR statement<ref_eql_forstatement>` for more details.
