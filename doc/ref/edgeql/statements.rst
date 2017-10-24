.. _ref_edgeql_statements:


Statements
==========

.. TODO::

    This section needs a significant re-write w.r.t usage of FOR.

EdgeQL has Select_, For_, Insert_, Update_, and Delete_ statements
for managing the data in the DB. Each of these statements can also be
used as an *expression* if it is enclosed in parentheses, in which
case they also return a value.

.. note::

    Running ``INSERT`` and other DML statements bare in repl yields
    the cardinality of the affected set.

Every statement consists of 2 blocks: :ref:`with
block<ref_edgeql_with>` and *main* statement block. Each of the blocks
defines its own scope which is relevant for
:ref:`how paths are interpreted <ref_edgeql_paths>`. In the following
examples, however, the ``WITH`` block will only be used for declaring
the ``MODULE`` for simplicity. The *main* block is a sequence of
clauses, where each clause is a
:ref:`set operation<ref_edgeql_expressions_setops>` modifying the
data the statement produces. The data flow can be seen as top-to-
bottom from more general to the more specific. Mathematically each
clause is a function that takes a set as input and produces a set as
output and each such function is applied to the output of the previous
clause.

Select
------

A ``SELECT`` statement returns a set of objects. By default it is
assumed to be a set of arbitrary cardinality. In case of various kinds
of serialization this may affect how the result is treated.

The data flow of a ``SELECT`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    SELECT
        <expr>  # compute a set of things

    # optional clause
    FILTER
        <expr>  # filter the computed set

    # optional clause
    ORDER BY
        <expr>  # define ordering of the filtered set

    # optional clause
    OFFSET/LIMIT
        <expr>  # slice the filtered/ordered set

Please note that the ``ORDER BY`` clause defines ordering that can
only be relied upon only if the resulting set is not used in any other
operation. ``OFFSET`` and ``LIMIT`` clauses are the only exception to
that rule as they preserve the inherent ordering of the underlying
set.

Consider an example using only the ``FILTER`` optional clause:

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name,
        <owned: Issue {
            number,
            body
        }
    }
    FILTER User.name LIKE 'Alice%';

The above example retrieves a single user with a specific name. The
fact that there is only one such user is a detail that can be well-
known and important to the creator of the DB, but otherwise non-
obvious. However, forcing the cardinality to be at most 1 by using the
keyword ``SINGLETON`` ensures that a set with a single object or
``{}`` is returned. This way any further code that relies on the
result of this query can safely assume there's only one result
available. In case EdgeDB generates more than one result for this
query, it is going to cause a runtime error in the EdgeDB code, making
it easier to debug.

.. code-block:: eql

    WITH MODULE example
    SELECT SINGLETON User {
        name,
        <owned: Issue {
            number,
            body
        }
    }
    FILTER User.name LIKE 'Alice%';

Next example adds the use of ``ORDER BY`` and ``LIMIT`` clauses:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue {
        number,
        body,
        due_date
    }
    FILTER
        EXISTS Issue.due_date
        AND
        Issue.status.name = 'Open'
    ORDER BY
        Issue.due_date
    LIMIT 3;

The above query retrieves the top 3 open Issues with the closest due
date.


For
---

A ``FOR`` statement is used where mathematically a universal qualifier
(∀) would be appropriate. It allows to compute a set based either on the
elements of some other set or on specific subsets of some set. There are two corresponding ways for expressing these use-cases.

The data flow of a ``FOR`` block that uses elements of a set to
iterate over can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    FOR <el>        # repeat for every element <el>
        IN <expr>   # of the set represented by <expr>

    UNION OF
        <expr>  # map every element onto a result set,
                # merging them all with a UNION ALL

    # optional clause
    FILTER
        <expr>  # filter the returned set of values

    # optional clause
    ORDER BY
        <expr>  # define ordering of the filtered set

    # optional clause
    OFFSET/LIMIT
        <expr>  # slice the filtered/ordered set

The data flow of a ``FOR`` block that iterates over subsets can be
conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    FOR <sub>, <g1>, ..., <gn>  # repeat for every subset <sub> and
                                # corresponding grouping parameters
                                # g1, ..., gn, given by the following
                                # expression:
        IN (

            GROUP
                <expr0>     # produce subsets of this set

            BY              # by grouping elements that produce the
                            # same distinct results for each of the
                            # following expressions
                <expr1>,
                ...
                <exprn>
    )

    UNION OF
        <expr>  # map every grouped set onto a result set,
                # merging them all with a UNION ALL

    # optional clause
    FILTER
        <expr>  # filter the returned set of values

    # optional clause
    ORDER BY
        <expr>  # define ordering of the filtered set

    # optional clause
    OFFSET/LIMIT
        <expr>  # slice the filtered/ordered set

Typically a simple iteration over set elements is used in conjunction
with an Insert_ or an Update_ statement. This mode is less useful with
a Select_ expression since a ``FILTER`` may accomplish the same end
result.

A ``FOR`` statement containing a ``GROUP`` clause, however, is useful
for various analytics. In the simpler form, a ``GROUP`` clause
partitions some set into subsets based on one or more parameters.

Consider the following example of a query that gets some statistics
about Issues, namely what's the total number of issues and time spent
per owner:

.. code-block:: eql

    WITH MODULE example
    FOR Issue, o IN (
        GROUP Issue
        BY Issue.owner
    )
    UNION OF (
        owner := o,
        total_issues := count(Issue),
        total_time := sum(Issue.time_spent_log.spent_time)
    );

Although, this particular query may rewritten without using ``FOR``
and ``GROUP``, but as a ``SELECT`` it is a useful example to
illustrate how ``GROUP`` works.

If there's a need to only look at statistics that end up over a
certain threshold of total time spent, a ``FILTER`` can be used in
conjunction with an alias of the ``SELECT`` clause result:

.. code-block:: eql

    WITH MODULE example
    FOR Issue, o IN (
        GROUP Issue
        BY Issue.owner
    )
    UNION OF _stats = (
        owner := o,
        total_issues := count(Issue),
        total_time := sum(Issue.time_spent_log.spent_time)
    )
    FILTER _stats.total_time > 10;

The choice of result alias is arbitrary, same as for the ``WITH``
block. The alias defined here exists in the scope of the ``FOR``
block and can be used to apply ``FILTER``, ``ORDER BY``, ``OFFSET``
and ``LIMIT`` clauses.

If there's a need to filter the *input* set of Issues, then this can
be done by using a ``SELECT`` expression at the subject clause of the
``GROUP``:

.. code-block:: eql

    WITH MODULE example
    # let's use a different alias here for better clarity
    FOR I, o IN (
        GROUP (
            SELECT Issue
            # in this GROUP only consider issues with watchers
            FILTER EXISTS Issue.watchers
        )
        BY Issue.owner
    )
    SELECT _stats = (
        owner := I.owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    )
    FILTER _stats.total_time > 10;


Insert
------

``INSERT`` allows creating new objects in EdgeDB. Notice that
generally ``id`` is not specified at creation time (although it can
be) and will be automatically generated by EdgeDB.

The data flow of an ``INSERT`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    INSERT
        <obj>       # create the following object



Notice that there are no other clauses except ``WITH`` in the
``INSERT`` statement. This is because it is a mutation statement and
not typically used to query the DB. It is still possible to use an
explicit ``SELECT`` statement and treat ``INSERT`` as an expression
the statement operates if filtering, ordering and slicing the results
of a mutation is required.

Here's a simple example of an ``INSERT`` statement creating a new user:

.. code-block:: eql

    WITH MODULE example
    INSERT User {
        name := 'Bob Johnson'
    };

``INSERT`` is not only a statement, but also an expression and as such
is has a value of the set of objects that has been created.

.. code-block:: eql

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

.. code-block:: eql

    WITH MODULE example
    INSERT Issue {
        number := '101',
        body := 'Nested INSERT',
        owner: User{
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
use-case and ``INSERT`` is simply the expression in the ``UNION OF``
clause.

.. code-block:: eql

    # example of a bulk insert of users based on explicitly provided
    # data
    WITH MODULE example
    FOR x IN {'Alice', 'Bob', 'Carol', 'Dave'}
    UNION OF (INSERT User {
        name := x
    });

    # example of a bulk insert of issues based on a query
    WITH
        MODULE example,
        Elvis := (SELECT User FILTER .name = 'Elvis'),
        Open := (SELECT Status FILTER .name = 'Open')
    FOR Q IN (SELECT User FILTER .name ILIKE 'A%')
    UNION OF (INSERT Issue {
        name := Q.name + ' access problem',
        body := 'This user was affected by recent system glitch',
        owner := Elvis,
        status := Open
    });

The statement ``FOR <x> IN <expr>`` allows to perform bulk inserts. It is
equivalent to invoking ``INSERT`` statement separately once for every
element of the set generated by the provided expression all in a
single transaction. See
:ref:`Usage of FOR statement<ref_edgeql_forstatement>` for more details.


Update
------

It is possible to update already existing objects via ``UPDATE``
statement. An update can target a single object or be a bulk update.
If used as an expression, it will return the set of objects on which
it operated.

The data flow of an ``UPDATE`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    UPDATE
        <expr>  # compute a set of things

    # optional clause
    FILTER
        <expr>  # filter the computed set

    SET
        <expr>  # update objects based on the
                # computed/filtered set

Notice that there are no ``ORDER``, ``OFFSET`` or ``LIMIT`` clauses in
the ``UPDATE`` statement. This is because it is a mutation statement
and not typically used to query the DB.

Here are a couple of examples of using the ``UPDATE`` statement:

.. code-block:: eql

    # update the user with the name 'Alice Smith'
    WITH MODULE example
    UPDATE User
    FILTER User.name = 'Alice Smith'
    SET {
        name := 'Alice J. Smith'
    };

    # update all users whose name is 'Bob'
    WITH MODULE example
    UPDATE User
    FILTER User.name LIKE 'Bob%'
    SET {
        name := User.name + '*'
    };

The statement ``FOR <x> IN <expr>`` allows to express certain bulk
updates more clearly. See
:ref:`Usage of FOR statement<ref_edgeql_forstatement>` for more details.


Delete
------

``DELETE`` statement removes the specified set of objects from the
database. Therefore, a ``FILTER`` can be applied to the set being
removed, while the ``DELETE`` statement itself does not have a
``FILTER`` clause. Just like ``INSERT`` if used as an expression it
will return the set of removed objects.

The data flow of a ``DELETE`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    DELETE
        <expr>  # create the following object

Here's a simple example of deleting a specific user:

.. code-block:: eql

    WITH MODULE example
    DELETE (SELECT User
            FILTER User.name = 'Alice Smith');

Notice that there are no other clauses except ``WITH`` in the
``DELETE`` statement. This is because it is a mutation statement and
not typically used to query the DB.
