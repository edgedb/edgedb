.. _ref_eql_statements_insert:

INSERT
======

:eql-statement:
:eql-haswith:

``INSERT`` -- create a new object in a database

.. eql:synopsis::

    [ WITH <with-spec> [ ,  ... ] ]
    INSERT <expression> [ <insert-shape> ]
    [ UNLESS CONFLICT
        [ ON <property-expr> [ ELSE <alternative> ] ]
    ] ;


Description
-----------

``INSERT`` inserts a new object into a database.

When evaluating an ``INSERT`` statement, *expression* is used solely to
determine the *type* of the inserted object and is not evaluated in any
other way.

If a value for a *required* link is evaluated to an empty set, an error is
raised.

It is possible to insert multiple objects by putting the ``INSERT``
into a :eql:stmt:`FOR` statement.

See :ref:`Usage of FOR statement <ref_eql_forstatement>` for more details.

:eql:synopsis:`WITH`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``UPDATE``
    statement.  See :ref:`ref_eql_statements_with` for more information.

:eql:synopsis:`<expression>`
    An arbitrary expression returning a set of objects to be updated.

    .. eql:synopsis::

        INSERT <expression>
        [ "{" <link> := <insert-value-expr> [, ...]  "}" ]

.. _ref_eql_statements_conflict:

:eql:synopsis:`UNLESS CONFLICT [ ON <property-expr> ]`
    :index: unless conflict

    Handler of conflicts.

    This clause allows to handle specific conflicts arising during
    execution of ``INSERT`` without producing an error.  If the
    conflict arises due to exclusive constraints on the properties
    specified by *property-expr*, then instead of failing with an
    error the ``INSERT`` statement produces an empty set (or an
    alternative result).

    The exclusive constraint on ``<property-expr>`` cannot be defined on a
    parent type.

    The specified *property-expr* may be either a reference to a
    property (or link) or a tuple of references to properties (or
    links).

    A caveat, however, is that ``UNLESS CONFLICT`` will not prevent
    conflicts caused between multiple DML operations in the same
    query; inserting two conflicting objects (through use of ``FOR``
    or simply with two ``INSERT`` statements) will cause a constraint
    error.

    Example:

    .. code-block:: edgeql

        INSERT User { email := 'user@example.org' }
        UNLESS CONFLICT ON .email

    .. code-block:: edgeql

        INSERT User { first := 'Jason', last := 'Momoa' }
        UNLESS CONFLICT ON (.first, .last)

:eql:synopsis:`ELSE <alternative>`
    Alternative result in case of conflict.

    This clause can only appear after ``UNLESS CONFLICT`` clause. Any
    valid expression can be specified as the *alternative*. When a
    conflict arises, the result of the ``INSERT`` becomes the
    *alternative* expression (instead of the default ``{}``).

    In order to refer to the conflicting object in the *alternative*
    expression, the name used in the ``INSERT`` must be used (see
    :ref:`example below <ref_eql_statements_insert_unless>`).

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
        owner := (
            INSERT User {
                name := 'Nested User'
            }
        )
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

    FOR Q IN (SELECT User FILTER .name ILIKE 'A%')

    UNION (INSERT Issue {
        name := Q.name + ' access problem',
        body := 'This user was affected by recent system glitch',
        owner := Elvis,
        status := Open
    });

.. _ref_eql_statements_insert_unless:

There's an important use-case where it is necessary to either insert a
new object or update an existing one identified with some key. This is
what the ``UNLESS CONFLICT`` clause allows:

.. code-block:: edgeql

    WITH MODULE people
    SELECT (
        INSERT Person {
            name := "Łukasz Langa", is_admin := true
        }
        UNLESS CONFLICT ON .name
        ELSE (
            UPDATE Person
            SET { is_admin := true }
        )
    ) {
        name,
        is_admin
    };


.. note::

    Statements in EdgeQL represent an atomic interaction with the
    database. From the point of view of a statement all side-effects
    (such as database updates) happen after the statement is executed.
    So as far as each statement is concerned, it is some purely
    functional expression evaluated on some specific input (database
    state).

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Insert <ref_eql_insert>`
  * - :ref:`Cheatsheets > Inserting data <ref_cheatsheet_insert>`
