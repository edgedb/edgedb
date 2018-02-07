.. _ref_edgeql_scope:

Scope
=====

Every query defines a new lexical scope or sub-scope in case of sub-
queries. Every sub-scope includes all the parent scopes, so when the
documentation refers to the scope of a sub-query it implicitly refers
to all the parent scopes in which the particular sub-query is nested
lexically.

The :ref:`with block<ref_edgeql_with>` is nested in the scope defined
by the statement. Any symbols defined in it are only visible within
that particular statement. ``WITH`` block aliases can be lexically
replaced with the expressions they stand for without changing the
overall meaning.

The following diagrams show how scopes are nested. For convenience the
scopes have been labeled with a number indicating nesting depth.
Different scopes at the same nesting depth also have a letter added to
the indexing.

.. code-block:: eql

    WITH MODULE example
    SELECT Issue {
        watchers: {
            name
        }
    }
    FILTER
        Issue.watchers.name = 'Alice';

.. aafig::
    :textual:

    +-- (0)--------------------------------+
    | "WITH MODULE example"                |
    | "SELECT Issue {"                     |
    | +-- (1a)---------------------------+ |
    | | "watchers: {"                    | |
    | | +-- (2)------------------------+ | |
    | | | "name"                       | | |
    | | +------------------------------+ | |
    | | "}"                              | |
    | +----------------------------------+ |
    | "}"                                  |
    | "FILTER"                             |
    | +-- (1b)---------------------------+ |
    | | "Issue.watchers.name = 'Alice';" | |
    | +----------------------------------+ |
    +--------------------------------------+

The scope breakdown of the above query makes it easy to see that the
``FILTER`` cannot affect the ``watchers`` sub-shape because they are
in parallel scopes (`1b,0` vs `1a,0`). On the other hand, the common
prefix ``Issue`` from scope `0` means the same thing in the ``FILTER``
as well as in the main part of the query.

.. code-block:: eql

    WITH MODULE example
    SELECT (
        Issue.number,
        count(Issue.watchers)
    );

.. aafig::
    :textual:

    +-- (0)-----------------------+
    | "WITH MODULE example"       |
    | "SELECT ("                  |
    |   "Issue.number,"           |
    | +-- (1)-------------------+ |
    | | "count(Issue.watchers)" | |
    | +-------------------------+ |
    | ");"                        |
    +-----------------------------+

In the above example the aggregate function ``count`` creates a sub-
scope for its argument. However, like before, the common prefix
``Issue`` from scope `0` is shared between ``Issue.number`` and
``Issue.watchers``. Therefore the ``count`` will be applied to
watchers of each issue separately.

.. code-block:: eql

    WITH MODULE example
    SELECT (
        (SELECT Issue.number),
        count(Issue.watchers)
    );

.. aafig::
    :textual:

    +-- (0)------------------------+
    | "WITH MODULE example"        |
    | "SELECT ("                   |
    | +-- (1a)-------------------+ |
    | | "(SELECT Issue.number)," | |
    | +--------------------------+ |
    |                              |
    | +-- (1b)-------------------+ |
    | | "count(Issue.watchers)"  | |
    | +--------------------------+ |
    | ");"                         |
    +------------------------------+

The last example is similar to the one before that, but
``Issue.number`` is wrapped in a ``SELECT`` sub-query. This means that
it has its own scope (`1a,0`) parallel to the scope created by
``count`` (`1b,0`). The net effect is that the ``count`` argument is
completely independent of the ``Issue.number`` of the sub-query and
effectively means "all issue watchers in the DB".

.. code-block:: eql

    WITH MODULE example
    SELECT (
        User IN Issue.watchers,
        count(Issue.watchers)
    );

.. aafig::
    :textual:

    +-- (0)-----------------------+
    | "WITH MODULE example"       |
    | "SELECT ("                  |
    |   "User "                   |
    |   "IN "                     |
    | +-- (1a)------------------+ |
    | | "Issue.watchers,"       | |
    | +-------------------------+ |
    |                             |
    | +-- (1b)------------------+ |
    | | "count(Issue.watchers)" | |
    | +-------------------------+ |
    | ");"                        |
    +-----------------------------+

To illustrate the peculiar signature of ``IN`` operator it can be put
in a tuple next to an aggregate function, such as ``count``. The
``IN`` operator's second operand creates its own sub-scope (because,
intuitively, the membership is checked against the set as a whole).
The example above shows that ``Issue.watchers`` exist independently in
parallel scopes in ``IN`` operator and in ``count``.

Last but not least, this is how the scopes in a complex query may apply:

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name,
        <owner: Issue {
            number,
            status: {
                name
            },
            priority: {
                name
            }
        }
    }
    FILTER
        User.name LIKE 'A%'
        AND
        User.<owner[IS Issue].status.name = 'Open'
        AND
        User.<owner[IS Issue].priority.name = 'High'
    ORDER BY User.name
    LIMIT 3;

.. aafig::
    :aspect: 60
    :scale: 150
    :textual:

    +-- (0)----------------------------------+
    |   "WITH MODULE example"                |
    | +-- (1a)-----------------------------+ |
    | | "SELECT User {"                    | |
    | | +-- (2a)-----------------------+   | |
    | | | "name,"                      |   | |
    | | +------------------------------+   | |
    | |                                    | |
    | | +-- (2b)-----------------------+   | |
    | | | "<owner: Issue {"            |   | |
    | | | +-- (3a)--------+            |   | |
    | | | | "number,"     |            |   | |
    | | | +---------------+            |   | |
    | | |                              |   | |
    | | | +-- (3b)--------+            |   | |
    | | | | "status: {"   |            |   | |
    | | | | +-- (4a)----+ |            |   | |
    | | | | | "name"    | |            |   | |
    | | | | +-----------+ |            |   | |
    | | | |     "},"      |            |   | |
    | | | +---------------+            |   | |
    | | |                              |   | |
    | | | +-- (3c)--------+            |   | |
    | | | | "priority: {" |            |   | |
    | | | | +-- (4b)----+ |            |   | |
    | | | | | "name"    | |            |   | |
    | | | | +-----------+ |            |   | |
    | | | | "}"           |            |   | |
    | | | +---------------+            |   | |
    | | | "}"                          |   | |
    | | +------------------------------+   | |
    | | "}"                                | |
    | |                                    | |
    | | "FILTER"                           | |
    | | +-- (2b)-----------------------+   | |
    | | | "User.name LIKE 'A%'"        |   | |
    | | | "AND"                        |   | |
    | | | "User.<owner[IS Issue]"      |   | |
    | | |    ".status.name = 'Open'"   |   | |
    | | | "AND"                        |   | |
    | | | "User.<owner[IS Issue]"      |   | |
    | | |    ".priority.name = 'High'" |   | |
    | | +------------------------------+   | |
    | | "ORDER BY "                        | |
    | | +-- (2c)-----------------------+   | |
    | | | "User.name"                  |   | |
    | | +------------------------------+   | |
    | +------------------------------------+ |
    |   "LIMIT "                             |
    | +-- (1b)-----------------------------+ |
    | |   "3;"                             | |
    | +------------------------------------+ |
    +----------------------------------------+

.. _ref_edgeql_scope_prefix:

Longest common prefix
---------------------

There's a basic principle in EdgeQL that *the same symbol refers to
the same thing*. Applied to paths this rule means that any common
prefix in two paths in the same scope is considered to refer to the
*same* object. Consider the following queries:

.. code-block:: eql

    # tuple query
    WITH MODULE example
    SELECT (
        Issue.status.name,
        Issue.priority.name
    );

    # shape query
    WITH MODULE example
    SELECT Issue {
        status: {
            name
        },
        priority: {
            name
        }
    };

Both of these queries will retrieve the name of the status and the
name of the priority for all of the Issues. The difference is in how
this information is structured (as a tuple or as nested objects), but
the important thing to understand is that ``Issue.status.name`` and
``Issue.priority.name`` refer to the status and priority for the
*same* Issue. This means that the first query will return a tuple with
the status name and priority name for every Issue. It is not going to
be a cross-product of the set of all status names with the set of all
priority names taken independently.

This rule holds no matter where in the ``SELECT`` expression the path
is used, as long as it is in the same scope. For example:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue
    FILTER
        Issue.status.name = 'Open'
        AND
        Issue.priority.name = 'High';

``Issue`` is the common prefix in all 3 path expressions. So this
select statement is interpreted as: select all ``Issues``, such that for
each ``Issue`` it is true that the status name is 'Open' and the priority
name is 'High'. The common prefix makes it easy to write intuitive
queries, by ensuring that the same sub-path always means the same
thing. Consider a more complex query:

.. need new example here, one that doesn't break clauses restrictions

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name
    }
    FILTER
        User.name LIKE 'A%'
        AND
        User.<owner[IS Issue].status.name = 'Open'
        AND
        User.<owner[IS Issue].priority.name = 'High';

In the above query there are two examples of a common sub-path:
``User`` and ``User.<owner[IS Issue]``. Breaking down the statement we
get the following features:

- the resulting set is composed of ``Users``
- the set of ``Users`` is restricted such that every element of it
  must have the ``name`` starting with 'A'
- set of ``Users`` is further restricted such that the set of
  ``Issues`` reachable from it by following the link ``owners``
  backwards must have at least one ``status`` with the ``name`` 'Open'
- set of ``Users`` is further restricted such that the set of
  ``Issues`` reachable from it by following the link ``owners``
  backwards must have at least one ``priority`` with the ``name``
  'High'

To see how different scopes within the same expression affect the
interpretation, consider the following query:

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name
    }
    ORDER BY User.name
    LIMIT count(User) / 3;

The ``ORDER BY`` clause is nested in the scope of ``SELECT``,
therefore it refers to the same ``User`` as ``SELECT`` does. This is
quite natural, since for ``FILTER`` and ``ORDER BY``, it makes sense
to refer to the objects being selected.

As was mentioned in the statements chapter, ``OFFSET`` and ``LIMIT``
clauses treat *both* their arguments as ``SET OF``, therefore
``count(User)`` exists in a parallel scope to the ``SELECT User {name}
ORDER BY User.name``. In particular that means that ``User`` in the
``LIMIT`` clause refers to the set as a whole even though in the
parallel scope ``User`` refers to each user individually.

Although, technically, the ``LIMIT`` clause can refer to ``User``, so
long as the resulting expression is a *singleton*. The following query
is illegal because ``len(User.name)`` is a set:

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name
    }
    ORDER BY User.name
    # this is an error
    LIMIT len(User.name);

Here's another example of an illegal expression. In this case
``LIMIT`` is referring to a symbol (``res``) defined in a sibling
scope:

.. code-block:: eql

    WITH MODULE example
    SELECT res := User {
        name
    }
    ORDER BY res.name
    # this is no longer valid as 'res' is not defined
    # in the scope of LIMIT
    LIMIT count(res) / 3;


Aggregate functions
-------------------

There's an interesting interaction between the longest common prefix
rule and aggregate functions. Consider the following:

.. code-block:: eql

    # count all the issues
    WITH MODULE example
    SELECT count(Issue);

    # provide an array of all issue numbers
    WITH MODULE example
    SELECT array_agg(Issue.number);

So far so good, but what if we wanted to combine statistical data
about total issues with some data from each individual ``Issue``? For
the sake of the example suppose that the ``Issue.number`` is actually
a sequential integer (still represented as a string according to our
schema, though) and what we want is a result of the form "Open issue
<number> / <total issues>".

.. code-block:: eql

    # The naive way of combining the result of count with a
    # specific Issue does not work.
    #
    # This will be a set of strings of the form:
    #   "Open issue <number> / 1"
    WITH MODULE example
    SELECT 'Open issue ' + Issue.number + ' / ' + <str>count(Issue)
    FILTER Issue.status.name = 'Open';

Due to the fact that ``Issue`` and ``Issue.number`` exist in the same
scope, the :ref:`longest common prefix<ref_edgeql_scope_prefix>`
rule dictates that ``Issue`` must refer to the same object for both of
these expressions. This means that ``count`` is always operating on a
set of one ``Issue``.

The way to fix that is to define another set as ``Issue`` in the
``WITH`` clause.

.. code-block:: eql

    # the alias I2 functions as if it were a schema-level view
    WITH
        MODULE example,
        I2 := DETACHED Issue
    SELECT
        'Open issue ' + Issue.number + ' / ' + <str>count(I2)
    FILTER Issue.status.name = 'Open';

Here's an example of an aggregate function that specifically takes
advantage of only being applied to the set restricted by the common
prefix:

.. code-block:: eql

    # Each result will only have the watchers of a given open issue.
    WITH MODULE example
    SELECT
        'Issue ' + Issue.number + ' watched by: ' +
            <str>array_agg(Issue.watchers.name)
    FILTER Issue.status.name = 'Open';


.. _ref_edgeql_scope_clauses:

Clauses and shapes
------------------

It's important to note that both *shapes* and *clauses* share a
particular property w.r.t. paths that are used in them. A clause or
shape cannot contain a path shorter than any of the paths already used
in the first clause argument or root of the shape. What this rule
really means is that the meaning of a symbol (common path prefix)
cannot be altered by adding more clauses or using a shape.

The above rule is only relevant if the common path prefix rule applies
in the first place, i.e. if the first clause argument is in the same
scope as the second. This is not the case for ``LIMIT`` and ``OFFSET``
clauses for instance.


.. _ref_edgeql_computables:

Sub-queries and computables
---------------------------

The scoping rule for common prefixes is also true for any paths used
in a shape query (in various clauses or computables). There's an
important property that stems from this fact: *all* path expressions
used in a shape query *must* have the same starting node. This is
because the shape query defines the shape of the data to be retrieved
on *per object* basis, so generally it makes sense that all paths used
in various clauses have common prefixes corresponding to this object
or related objects.

The only way to refer to a path with a different starting node from
the base shape is to use a sub-query in a computable. Consider the
following shape query retrieving a single user with additional data in
the for of latest 3 Issues and total open issue count (this would make
sense for an admin account, for example):

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        id,
        name,
        latest_issues := (
            SELECT Issue {
                id,
                name,
                body,
                owner: {
                    id,
                    name
                },
                status: {
                    name
                }
            }
            ORDER BY Issue.start_date DESC
            LIMIT 3
        ),
        total_open := (
            SELECT count(Issue)
            FILTER Issue.status.name = 'Open'
        )
    } FILTER User.name = 'Alice Smith';

In the above example there are two sub-queries referring to ``Issue``.
Because those sub-queries are not nested in each other, they are
considered to belong to two different scopes and do not represent the
same object. Which is intuitively the behavior one should expect as
the top 3 issues should not in any way impact the total open issue
count.
