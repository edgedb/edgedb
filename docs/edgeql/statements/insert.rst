.. _ref_eql_statements_insert:

INSERT
======

:eql-statement:
:eql-haswith:

``INSERT`` -- create a new object in a database

.. eql:synopsis::

    [ WITH <with-spec> [ ,  ... ] ]
    INSERT <expression> [ <insert-shape> ] ;


Description
-----------

``INSERT`` inserts a new object into a database.

When evaluating an ``INSERT`` statement, *expression* is used solely to
determine the *type* of the inserted object and is not evaluated in any
other way.

An optional *insert-shape* expression (see
:ref:`shapes <ref_eql_expr_shapes_insert>`), may be used to specify the values
of the links for the inserted object.  If a link is not present in
*insert-shape*, a default value is used: either the declared default value
for the link, or an empty set if there is none.

If a value for a *required* link is evaluated to an empty set, an error is
raised.

It is possible to insert multiple objects by putting the ``INSERT``
into a :eql:stmt:`FOR` statement.

See :ref:`Usage of FOR statement <ref_eql_forstatement>` for more details.

:eql:synopsis:`WITH`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``UPDATE``
    statement.  See :ref:`ref_eql_with` for more information.

:eql:synopsis:`<expression>`
    An arbitrary expression returning a set of objects to be updated.

    .. eql:synopsis::

        INSERT <expression>
        [ "{" <link> := <insert-value-expr> [, ...]  "}" ]


Outputs
-------

The result of an ``INSERT`` statement used as an *expression* is a
singleton set containing the inserted object.


Examples
--------

Here's a simple example of an ``INSERT`` statement creating a new user:

.. code-block:: edgeql

    WITH MODULE example
    INSERT User {
        name := 'Bob Johnson'
    };

``INSERT`` is not only a statement, but also an expression and as such
is has a value of the set of objects that has been created.

.. code-block:: edgeql

    WITH MODULE example
    INSERT Issue {
        number := '100',
        body := 'Fix errors in INSERT',
        owner := (
            SELECT User FILTER User.name = 'Bob Johnson'
        )
    };

It is possible to create nested objects in a single ``INSERT``
statement as an atomic operation.

.. code-block:: edgeql

    WITH MODULE example
    INSERT Issue {
        number := '101',
        body := 'Nested INSERT',
        owner: User {
            name := 'Nested User'
        }
    };

The above statement will create a new ``Issue`` as well as a new
``User`` as the owner of the ``Issue``. It will also return the new
``Issue`` linked to the new ``User`` if the statement is used as an
expression.

It is also possible to create new objects based on some existing data
either provided as an explicit list (possibly automatically generated
by some tool) or a query. A ``FOR`` statement is the basis for this
use-case and ``INSERT`` is simply the expression in the ``UNION``
clause.

.. code-block:: edgeql

    # example of a bulk insert of users based on explicitly provided
    # data
    WITH MODULE example
    FOR x IN {'Alice', 'Bob', 'Carol', 'Dave'}
    UNION (INSERT User {
        name := x
    });

    # example of a bulk insert of issues based on a query
    WITH
        MODULE example,
        Elvis := (SELECT User FILTER .name = 'Elvis'),
        Open := (SELECT Status FILTER .name = 'Open')
    FOR Q IN {(SELECT User FILTER .name ILIKE 'A%')}
    UNION (INSERT Issue {
        name := Q.name + ' access problem',
        body := 'This user was affected by recent system glitch',
        owner := Elvis,
        status := Open
    });


.. note::

    Statements in EdgeQL represent an atomic interaction with the
    database. From the point of view of a statement all side-effects
    (such as database updates) happen after the statement is executed.
    So as far as each statement is concerned, it is some purely
    functional expression evaluated on some specific input (database
    state).
