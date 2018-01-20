.. _ref_edgeql_statements:


Statements
==========

Statements in EdgeQL represent an atomic interaction with the DB. From
the point of view of a statement all side-effects (such as DB updates)
happen after the statement is executed. So as far as each statement is
concerned, it is some purely functional expression evaluated on some
specific input (DB state).

Statements consist of building blocks called `clauses`. Each `clause`
can be represented as an equivalent set function of a certain
signature. Understanding how :ref:`how functions
work<ref_edgeql_fundamentals_function>` helps in understanding
`clauses`. A statement is effectively a data pipeline made out of
`clauses`. Unlike functions and operators `clauses` cannot be
arbitrarily mixed, but must follow specific patterns. EdgeQL has
Select_, Group_, For_, Insert_, Update_, and Delete_ statements for
managing the data in the DB. Each of these statements can also be used
as an *expression* if it is enclosed in parentheses, in which case
they also return a value.

.. note::

    Running ``INSERT`` and other DML statements bare in repl yields
    the cardinality of the affected set.

Every statement starts with an optional :ref:`with block<ref_edgeql_with>`.
A ``WITH`` block defines symbols that can be used in the rest of the
statement. To keep things simple, in the examples below ``WITH`` block
is only used to define a default ``MODULE``. It is not necessary
because every ``concept`` can be referred to by a fully qualified name
(e.g. ``example::User``), but specifying a default ``MODULE`` makes it
possible to just use short names (e.g. ``User``).


Select
------

A ``SELECT`` statement returns a set of objects. The data flow of a
``SELECT`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    # select clause
    SELECT
        <expr>  # compute a set of things

    # optional clause
    FILTER
        <expr>  # filter the computed set

    # optional clause
    ORDER BY
        <expr>  # define ordering of the filtered set

    # optional clause
    OFFSET
        <expr>  # slice the filtered/ordered set

    # optional clause
    LIMIT
        <expr>  # slice the filtered/ordered set

Please note that the ``ORDER BY`` clause defines ordering that can
only be relied upon only if the resulting set is not used in any other
operation. ``SELECT``, ``OFFSET`` and ``LIMIT`` clauses are the only
exception to that rule as they preserve the inherent ordering of the
underlying set.

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
``LIMIT`` clause ensures that a set with a single object or
``{}`` is returned. This way any further code that relies on the
result of this query can safely assume there's only one result
available.

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name,
        <owned: Issue {
            number,
            body
        }
    }
    FILTER User.name LIKE 'Alice%'
    LIMIT 1;

Next example makes use of ``ORDER BY`` and ``LIMIT`` clauses:

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


Filter
++++++

The ``FILTER`` clause cannot affect anything aggregate-like in the
preceding ``SELECT`` clause. This is due to how ``FILTER`` clause
works. It can be conceptualized as a function like ``filter($input,
SET OF $cond)``, where the ``$input`` represents the value of the
preceding clause, while the ``$cond`` represents the filtering
condition expression. Consider the following:

.. code-block:: eql

    WITH MODULE example
    SELECT count(User)
    FILTER User.name LIKE 'Alice%';

The above can be conceptualized as:

.. code-block:: eql

    WITH MODULE example
    SELECT filter(
        count(User),
        User.name LIKE 'Alice%'
    );

In this form it is more apparent that ``User`` is a ``SET OF``
argument (of ``count``), while ``User.name LIKE 'Alice%'`` is also a
``SET OF`` argument (of ``filter``). So the symbol ``User`` in these
two expressions exists in 2 parallel scopes. Contrast it with:

.. code-block:: eql

    # This will actually only count users
    # whose name starts with 'Alice'
    WITH MODULE example
    SELECT count(
        (SELECT User
         FILTER User.name LIKE 'Alice%')
    );

    # which can be represented as:
    WITH MODULE example
    SELECT count(
        filter(User,
               User.name LIKE 'Alice%')
    );


.. _ref_edgeql_statements_group:

Group
-----

A ``GROUP`` statement is used to allow operations on set partitions.
The input set is partitioned using expressions in the ``USING`` and
``BY`` clauses, and then for each partition the expression in the
``UNION`` clause is evaluated and merged with the rest of the results
via a ``UNION ALL`` (or ``UNION`` for Objects). There are various
useful functions that require a set of values as their input -
aggregate functions. Simple aggregate function examples include
``count``, ``sum``, ``array_agg``. All of these are functions that map
a set of values onto a single value. A ``GROUP`` statement allows to
use aggregate functions to compute various properties of set
partitions.

The data flow of a ``GROUP`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    GROUP
        <alias0> := <expr>  # define a set to partition

    USING

        <alias1> := <expr>,     # define parameters to use for
        <alias2> := <expr>,     # grouping
        ...
        <aliasN> := <expr>

    BY
        <alias1>, ... <aliasN>  # specify which parameters will
                                # be used to partition the set

    UNION
        <expr>  # map every grouped set onto a result set,
                # merging them all with a UNION ALL (or UNION for
                # Objects)

    INTO
        <sub_alias> # provide an alias to refer to the subsets
                    # in expressions

    # optional clause
    FILTER
        <expr>  # filter the returned set of values

    # optional clause
    ORDER BY
        <expr>  # define ordering of the filtered set

    # optional clause
    OFFSET/LIMIT
        <expr>  # slice the filtered/ordered set

Notice that defining aliases in ``GROUP`` and ``USING`` clauses is
mandatory. Only the names defined in ``USING`` clause are legal in the
``BY`` clause. Also the names defined in ``GROUP`` and ``USING``
clauses allow to unambiguously refer to the specific grouping subset
and the relevant grouping parameter values respectively in the
``UNION`` clause.

Consider the following example of a query that gets some statistics
about Issues, namely what's the total number of issues and time spent
per owner:

.. code-block:: eql

    WITH MODULE example
    GROUP Issue
    USING Owner := Issue.owner
    BY Owner
    INTO I
    UNION (
        owner := Owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    );

Although, this particular query may rewritten without using ``GROUP``,
but as a ``SELECT`` it is a useful example to illustrate how ``GROUP``
works.

If there's a need to only look at statistics that end up over a
certain threshold of total time spent, a ``FILTER`` can be used in
conjunction with an alias of the ``SELECT`` clause result:

.. code-block:: eql

    WITH MODULE example
    GROUP Issue
    USING Owner := Issue.owner
    BY Owner
    INTO I
    UNION _stats = (
        owner := Owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    )
    FILTER _stats.total_time > 10;

The choice of result alias is arbitrary, same as for the ``WITH``
block. The alias defined here exists in the scope of the ``UNION``
block and can be used to apply ``FILTER``, ``ORDER BY``, ``OFFSET``
and ``LIMIT`` clauses.

If there's a need to filter the *input* set of Issues, then this can
be done by using a ``SELECT`` expression at the subject clause of the
``GROUP``:

.. code-block:: eql

    WITH MODULE example
    GROUP
        I := (
            SELECT Issue
            # in this GROUP only consider issues with watchers
            FILTER EXISTS Issue.watchers
        )
    USING Owner := I.owner
    BY Owner
    INTO I
    UNION _stats = (
        owner := Owner,
        total_issues := count(I),
        total_time := sum(I.time_spent_log.spent_time)
    )
    FILTER _stats.total_time > 10;


For
---

A ``FOR`` statement is used where mathematically a universal qualifier
(∀) would be appropriate. It allows to compute a set based on the
elements of some other set.

The data flow of a ``FOR`` block that uses elements of a set to
iterate over can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    FOR <el>        # repeat for every element <el>
        IN <set>    # of the set literal <set>

    UNION
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

Typically a simple iteration over set elements is used in conjunction
with an Insert_ or an Update_ statement. This mode is less useful with
a Select_ expression since a ``FILTER`` may accomplish the same end
result.

.. NOTE::

    Technically, a ``FOR`` statement can be viewed as a special case
    of ``GROUP``:

    .. code-block:: eql

        FOR X IN {Foo}
        UNION (INSERT Bar {foo := X});

        # can be equivalently rewritten as:
        GROUP Foo
        USING _ := Foo
        BY _
        INTO X
        UNION (INSERT Bar {foo := X});

.. _ref_edgeql_forstatement:

Usage of FOR statement
~~~~~~~~~~~~~~~~~~~~~~

``FOR`` statement has some powerful features that deserve to be
considered in detail separately. However, the common core is that
``FOR`` iterates over elements of some arbitrary expression. Then for
each element of the iterator some set is computed and combined via a
``UNION`` or ``UNION ALL`` with the other such computed sets.

The simplest use case is when the iterator is given by a set
expression and it follows the general form of ``FOR x IN A ...``:

.. code-block:: eql

    WITH MODULE example
    # the iterator is an explicit set of tuples, so x is an
    # element of this set, i.e. a single tuple
    FOR x IN {
        (name := 'Alice', theme := 'fire'),
        (name := 'Bob', theme := 'rain'),
        (name := 'Carol', theme := 'clouds'),
        (name := 'Dave', theme := 'forest')
    }
    # typically this is used with an INSERT, DELETE or UPDATE
    UNION (
        INSERT
            User {
                name := x.name,
                theme := x.theme,
            }
    );

Since ``x`` is an element of a set it is guaranteed to be a non-empty
singleton in all of the expressions used by the ``UNION OF`` and later
clauses of ``FOR``.

Another variation this usage of ``FOR`` is a bulk ``UPDATE``. There
are cases when a bulk update lots of external data, that cannot be
derived from the objects being updated. That is a good use-case when a
``FOR`` statement is appropriate.

.. code-block:: eql

    # Here's an example of an update that is awkward to
    # express without the use of FOR statement
    WITH MODULE example
    UPDATE User
    FILTER User.name IN {'Alice', 'Bob', 'Carol', 'Dave'}
    SET {
        theme := 'red'  IF .name = 'Alice' ELSE
                 'star' IF .name = 'Bob' ELSE
                 'dark' IF .name = 'Carol' ELSE
                 'strawberry'
    };

    # Using a FOR statement, the above update becomes simpler to
    # express or review for a human.
    WITH MODULE example
    FOR x IN {
        (name := 'Alice', theme := 'red'),
        (name := 'Bob', theme := 'star'),
        (name := 'Carol', theme := 'dark'),
        (name := 'Dave', theme := 'strawberry')
    }
    UNION (
        UPDATE User
        FILTER User.name = x.name
        SET {
            theme := x.theme
        }
    );

When updating data that mostly or completely depends on the objects
being updated there's no need to use the ``FOR`` statement and it is not
advised to use it for performance reasons.

.. code-block:: eql

    WITH MODULE example
    UPDATE User
    FILTER User.name IN {'Alice', 'Bob', 'Carol', 'Dave'}
    SET {
        theme := 'halloween'
    };

    # The above can be accomplished with a FOR statement,
    # but it is not recommended.
    WITH MODULE example
    FOR x IN {'Alice', 'Bob', 'Carol', 'Dave'}
    UNION (
        UPDATE User
        FILTER User.name = x
        SET {
            theme := 'halloween'
        }
    );

Another example of using a ``FOR`` statement is working with link
properties. Specifying the link properties either at creation time or
in a later step with an update is often simpler with a ``FOR``
statement helping to associate the link target to the link property in
an intuitive manner.

.. code-block:: eql

    # Expressing this without FOR statement is fairly tedious.
    WITH
        MODULE example,
        U2 := User
    FOR x IN {
        (
            name := 'Alice',
            friends := [('Bob', 'coffee buff'),
                        ('Carol', 'dog person')]
        ),
        (
            name := 'Bob',
            friends := [('Alice', 'movie buff'),
                        ('Dave', 'cat person')]
        )
    }
    UNION (
        UPDATE User
        FILTER User.name = x.name
        SET {
            friends := (
                FOR f in {unnest(x.friends)}
                UNION (
                    SELECT U2 {@nickname := f.1}
                    FILTER U2.name = f.0
                )
            )
        }
    );


Insert
------

``INSERT`` allows creating new objects in EdgeDB. Notice that
generally ``id`` is not specified at creation time (although it can
be) and will be automatically generated by EdgeDB.

The data flow of an ``INSERT`` block can be conceptualized like this:

.. code-block:: pseudo-eql

    WITH MODULE example

    INSERT
        <obj>   # create the following object



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

.. code-block:: eql

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

The statement ``FOR <x> IN <set>`` allows to perform bulk inserts. It is
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
        <expr>  # delete the following objects

Here's a simple example of deleting a specific user:

.. code-block:: eql

    WITH MODULE example
    DELETE (SELECT User
            FILTER User.name = 'Alice Smith');

Notice that there are no other clauses except ``WITH`` in the
``DELETE`` statement. This is because it is a mutation statement and
not typically used to query the DB.


.. _ref_edgeql_with:

With block
----------

.. needs a rewrite

The ``WITH`` block in EdgeQL is used to define scope and aliases.

Specifying a module
~~~~~~~~~~~~~~~~~~~

One of the more basic and common uses of the ``WITH`` block is to
specify the default module that is used in a query. ``WITH MODULE
<name>`` construct indicates that whenever an identifier is used
without any module specified explicitly, the module will default to
``<name>`` and then fall back to built-ins from ``std`` module.

The following queries are exactly equivalent:

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

    SELECT example::User {
        name,
        <owned: example::Issue {
            number,
            body
        }
    }
    FILTER example::User.name LIKE 'Alice%';


It is also possible to define aliases modules in the ``WITH`` block.
Consider the following query that needs to compare objects
corresponding to concepts defined in two different modules.

.. code-block:: eql

    WITH
        MODULE example,
        f := MODULE foo
    SELECT User {
        name
    }
    FILTER .name = f::Foo.name;

Another use case is for giving short aliases to long module names
(especially if module names contain `.`).

.. code-block:: eql

    WITH
        MODULE example,
        fbz := MODULE foo.bar.baz
    SELECT User {
        name
    }
    FILTER .name = fbz::Baz.name;


Cardinality
~~~~~~~~~~~

Typically the cardinality of an expression can be statically
determined from the individual parts. Sometimes it is necessary to
specify the cardinality explicitly. For example, when using
computables in shapes it may be desirable to specify the cardinality
of the computable because it affects serialization.

.. code-block:: eql

    WITH
        MODULE example
    SELECT User {
        name,
        nicknames := (
            WITH CARDINALITY '*'
            SELECT 'Foo'
        )
    };


Views
~~~~~

It is possible to specify an aliased view in the ``WITH`` block. Since
every aliased view exists in its own
:ref:`sub-scope<ref_edgeql_paths_scope>`, aliases can be used to refer
to different instances of the same *concept* in a query. For example,
the following query will find all users who own the same number of
issues as someone else:

.. code-block:: eql

    WITH
        MODULE example,
        U2 := User
    # U2 and User in the SELECT clause now refer to the same concept,
    # but different objects
    SELECT User {
        name,
        issue_count := count(User.<owner[IS Issue])
    }
    FILTER
        User.issue_count = count((
            SELECT U2.<owner[IS Issue]
            FILTER U2 != User
        ));


Transactions
------------

Statements can also be grouped into `transactions` to prevent any
other statements altering the DB state unpredictably while the
transaction is executing. This effectively makes a group of statements
behave as an atomic unit. The statements in a transaction dictate an
imperative execution sequence. Transactions can also be nested within
each other. ``START TRANSACTION`` initiates a new (sub-)transaction.
It can be committed to the DB making the changes visible to any other
queries by using a ``COMMIT`` statement. Alternatively, the
transaction changes may be discarded by using ``ROLLBACK`` statement.
Note that ``COMMIT`` and ``ROLLBACK`` affect only the innermost
transaction if the transactions are nested.
