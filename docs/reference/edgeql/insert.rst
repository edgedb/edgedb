.. _ref_eql_statements_insert:

Insert
======

:eql-statement:
:eql-haswith:

``insert`` -- create a new object in a database

.. eql:synopsis::

    [ with <with-spec> [ ,  ... ] ]
    insert <expression> [ <insert-shape> ]
    [ unless conflict
        [ on <property-expr> [ else <alternative> ] ]
    ] ;


Description
-----------

``insert`` inserts a new object into a database.

When evaluating an ``insert`` statement, *expression* is used solely to
determine the *type* of the inserted object and is not evaluated in any
other way.

If a value for a *required* link is evaluated to an empty set, an error is
raised.

It is possible to insert multiple objects by putting the ``insert``
into a :eql:stmt:`for` statement.

See :ref:`ref_eql_forstatement` for more details.

:eql:synopsis:`with`
    Alias declarations.

    The ``with`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the :eql:stmt:`update`
    statement.  See :ref:`ref_eql_statements_with` for more information.

:eql:synopsis:`<expression>`
    An arbitrary expression returning a set of objects to be updated.

    .. eql:synopsis::

        insert <expression>
        [ "{" <link> := <insert-value-expr> [, ...]  "}" ]

.. _ref_eql_statements_conflict:

:eql:synopsis:`unless conflict [ on <property-expr> ]`
    :index: unless conflict

    Handler of conflicts.

    This clause allows to handle specific conflicts arising during
    execution of ``insert`` without producing an error.  If the
    conflict arises due to exclusive constraints on the properties
    specified by *property-expr*, then instead of failing with an
    error the ``insert`` statement produces an empty set (or an
    alternative result).

    The exclusive constraint on ``<property-expr>`` cannot be defined on a
    parent type.

    The specified *property-expr* may be either a reference to a property (or
    link) or a tuple of references to properties (or links). Although versions
    prior to 2.10 do *not* support ``unless conflict`` on :ref:`multi
    properties <ref_datamodel_props_cardinality>`, 2.10 adds support for these.

    A caveat, however, is that ``unless conflict`` will not prevent
    conflicts caused between multiple DML operations in the same
    query; inserting two conflicting objects (through use of ``for``
    or simply with two ``insert`` statements) will cause a constraint
    error.

    Example:

    .. code-block:: edgeql

        insert User { email := 'user@example.org' }
        unless conflict on .email

    .. code-block:: edgeql

        insert User { first := 'Jason', last := 'Momoa' }
        unless conflict on (.first, .last)

:eql:synopsis:`else <alternative>`
    Alternative result in case of conflict.

    This clause can only appear after ``unless conflict`` clause. Any
    valid expression can be specified as the *alternative*. When a
    conflict arises, the result of the ``insert`` becomes the
    *alternative* expression (instead of the default ``{}``).

    In order to refer to the conflicting object in the *alternative*
    expression, the name used in the ``insert`` must be used (see
    :ref:`example below <ref_eql_statements_insert_unless>`).

Outputs
-------

The result of an ``insert`` statement used as an *expression* is a
singleton set containing the inserted object.


Examples
--------

Here's a simple example of an ``insert`` statement creating a new user:

.. code-block:: edgeql

    with module example
    insert User {
        name := 'Bob Johnson'
    };

``insert`` is not only a statement, but also an expression and as such
is has a value of the set of objects that has been created.

.. code-block:: edgeql

    with module example
    insert Issue {
        number := '100',
        body := 'Fix errors in insert',
        owner := (
            select User filter User.name = 'Bob Johnson'
        )
    };

It is possible to create nested objects in a single ``insert``
statement as an atomic operation.

.. code-block:: edgeql

    with module example
    insert Issue {
        number := '101',
        body := 'Nested insert',
        owner := (
            insert User {
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
by some tool) or a query. A ``for`` statement is the basis for this
use-case and ``insert`` is simply the expression in the ``union``
clause.

.. code-block:: edgeql

    # example of a bulk insert of users based on explicitly provided
    # data
    with module example
    for x in {'Alice', 'Bob', 'Carol', 'Dave'}
    union (insert User {
        name := x
    });


    # example of a bulk insert of issues based on a query
    with
        module example,
        Elvis := (select User filter .name = 'Elvis'),
        Open := (select Status filter .name = 'Open')

    for Q in (select User filter .name ilike 'A%')

    union (insert Issue {
        name := Q.name + ' access problem',
        body := 'This user was affected by recent system glitch',
        owner := Elvis,
        status := Open
    });

.. _ref_eql_statements_insert_unless:

There's an important use-case where it is necessary to either insert a
new object or update an existing one identified with some key. This is
what the ``unless conflict`` clause allows:

.. code-block:: edgeql

    with module people
    select (
        insert Person {
            name := "Łukasz Langa", is_admin := true
        }
        unless conflict on .name
        else (
            update Person
            set { is_admin := true }
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
