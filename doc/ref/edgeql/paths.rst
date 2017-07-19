.. _ref_edgeql_paths:


Paths
=====

Paths are fundamental building blocks of EdgeQL. A path defines a set
of objects in EdgeDB (just like any other expression) based on their
type and relationship with other objects.

A path always starts with some ``concept`` as its `root` and it may
have an arbitrary number of `steps` following various ``links``. The
simplest path consists only of a `root` and is interpreted to mean
'all objects of the type `root`'.

.. code-block:: eql

    WITH MODULE example
    SELECT Issue;

In the above example ``Issue`` is a path that represents all objects in
the database of type ``Issue``. That is the result of the above query.

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.owner;

The path ``Issue.owner`` consists of the `root` ``Issue`` and a `path
step` ``.owner``. It specifies the set of all objects that can be
reached from any object of type ``Issue`` by following its link
``owner``. This means that the above query will only retrieve users
that actually *have* at least one issue. The ``.`` operator in the path
separates `steps` and each step corresponds to a ``link`` name that
must be followed. By default, links are followed in the `outbound`
direction (the direction that is actually specified in the schema).
The direction of the link can be also specified explicitly by using
``>`` for `outbound` and ``<`` for `inbound`. Thus, the above query can be
rewritten more explicitly, but equivalently as:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.>owner;

To select all issues that actually have at least one watcher, it is
possible to construct a path using `inbound` link:

.. code-block:: eql

    WITH MODULE example
    SELECT User.<watchers;

The path in the above query specifies the set of all objects that can
be reached from ``User`` by following any ``link`` named ``watchers``
that has ``User`` as its target, back to the source of the ``link``.
In our case, there is only one link in the schema that is called
``watchers``. This link belongs to ``Issue`` and indeed it has
``User`` as its target, so the above query will get all the ``Issue``
objects that have at least one watcher. Only links that have a concept
as their target can be followed in the `inbound` direction. It is not
possible to follow inbound links on atoms.

Just like the direction of the step can be specified explicitly in a
path, so can the type of the link target. In order to retrieve all the
``SystemUsers`` that have actually created new ``Issues`` (as opposed
to ``Comments``) the following query could be made:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.owner[IS SystemUser];

In the above query the `path step` is expressed as ``owner[IS
SystemUser]``, where ``owner`` is the name of the link to follow, and
the qualifier ``[IS ...]`` specifies a restriction on the target's
type.

This is equivalent to:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.owner
    FILTER Issue.owner IS SystemUser;

The biggest difference between the two of the above representations is
that ``[IS SystemUser]`` allows to refer to links specific to
``SystemUser``.

Finally combining all of the above, it is possible to write a query to
retrieve all the ``Comments`` to ``Issues`` created by ``SystemUsers``:

.. code-block:: eql

    WITH MODULE example
    SELECT SystemUser.<owner[IS Issue].<issue;

    # or equivalently

    WITH MODULE example
    SELECT SystemUser
        # follow the link 'owner' to a source Issue
        .<owner[IS Issue]
        # follow the link 'issue' to a source Comment
        .<issue[IS Comment];

.. note::

    Links technically also belong to a module. Typically, the module
    doesn't need to be specified (because it is the default module or
    the link name is unambiguous), but sometimes it is necessary to
    specify the link module explicitly. The entire fully-qualified
    link name then needs to be enclosed in parentheses:

    .. code-block:: eql

        WITH MODULE some_module
        SELECT A.(another_module::foo).bar;


.. _ref_edgeql_paths_scope:

Scope
-----

Every query defines a new lexical scope or sub-scope in case of sub-
queries. Every sub-scope includes all the parent scopes, so when the
documentation refers to the scope of a sub-query it implicitly refers
to all the parent scopes in which the particular sub-query is nested
lexically. The statement block is nested in the scope defined by the
:ref:`with block<ref_edgeql_with>`. This implies that all aliases
defined in the ``WITH`` block are visible in the statement block.
Since each expression alias uses a sub-query, those sub-queries exists
in parallel scopes to each other, while they share the same common
``WITH`` block scope. This is similar to how non-nested computables in
shapes exist in sibling sub-scopes.

The following diagram shows how scopes are nested. For convenience the
scopes have been labeled with a number indicating nesting depth.
Different scopes at the same nesting depth also have a letter added to
the indexing.

.. aafig::
    :aspect: 60
    :scale: 150
    :textual:

        +-(0)-------------------------------+
        |                                   |
        | +-(1)---------------------------+ |
        | |    WITH                       | |
        | |              +-(2a)----+      | |
        | |        A :=  | ...     |      | |
        | |              +---------+      | |
        | |                               | |
        | |              +-(2b)----+      | |
        | |        B :=  | ...     |      | |
        | |              +---------+      | |
        | |                               | |
        | | +-(2c)----------------------+ | |
        | | |  SELECT                   | | |
        | | |                           | | |
        | | |      res := Foo "{"       | | |
        | | |                +-(3a)---+ | | |
        | | |          x :=  | ...    | | | |
        | | |                +--------+ | | |
        | | |                           | | |
        | | |                +-(3b)---+ | | |
        | | |          y :=  | ...    | | | |
        | | |                +--------+ | | |
        | | |      "}"                  | | |
        | | |                           | | |
        | | | +-(3c)------------------+ | | |
        | | | |FILTER                 | | | |
        | | | |                       | | | |
        | | | |    ...                | | | |
        | | | +-----------------------+ | | |
        | | |                           | | |
        | | | +-(3d)------------------+ | | |
        | | | |ORDER BY               | | | |
        | | | |                       | | | |
        | | | |    ...                | | | |
        | | | +-----------------------+ | | |
        | | |                           | | |
        | | +---------------------------+ | |
        | |                               | |
        | | +-(2d)----------------------+ | |
        | | |  OFFSET ... LIMIT ...     | | |
        | | +---------------------------+ | |
        | |                               | |
        | +-------------------------------+ |
        |                                   |
        +-----------------------------------+

In the diagram the scope `(0)` is the default scope, which basically
contains builtins (all things in ``std`` and all the modules as
namespaces).

Scope `(1)` is the base scope of the statement, the scope of the
``WITH`` block. Any names defined in the ``WITH`` block are visible
for the entire (nested) statement.

Scopes `(2a)`, `(2b)`, `(2c)` and `(2d)` are siblings. This means that
their contents are treated as independent from each other. They all
have access to the names defined in scopes `(0)` and `(1)`. For
example, this is why if ``A := User`` and ``B := User``, then ``A``
and ``B`` will refer to potentially different users.

The scopes defined by the :ref:`shape<ref_edgeql_shapes>`
:ref:`computables<ref_edgeql_computables>` ``x`` and ``y`` are `(3a)`
and `(3b)`, respectively. They are nested within the ``SELECT``
expression  scope `(2c)`.

Various clauses like ``FILTER`` and ``ORDER BY`` each have a scope of
their own (`(3c)` and `(3d)`) and are all nested inside the ``SELECT``
scope `(2c)`. This is important for understanding how longest common
prefix rule works. This nesting also means that the clauses can refer
to the result of the ``SELECT``, in the case of the example the result
is *named* ``res``. For example, ``FILTER res.x > 0 ORDER BY res.y``
would be legal.

The ``OFFSET`` and ``LIMIT`` scope `(2d)` is a sibling of the
``SELECT`` scope, thus it can only reference things defined in the
``WITH`` block (scopes `(0)` and `(1)`), but cannot refer to the
result of the ``SELECT`` expression itself.


.. _ref_edgeql_paths_prefix:

Longest common prefix
+++++++++++++++++++++

An important rule for interpreting paths is that any common prefix in
two paths in the same scope is considered to refer to the *same*
object. Consider the following queries:

.. code-block:: eql

    # tuple query
    WITH MODULE example
    SELECT (
        User.<owner[IS Issue].status.name,
        User.<owner[IS Issue].priority.name
    ) FILTER User.name = 'Alice Smith';

    # shape query
    WITH MODULE example
    SELECT Issue {
        status: {
            name
        },
        priority: {
            name
        }
    } FILTER Issue.owner.name = 'Alice Smith';

Both of these queries will retrieve the name of the status and the
name of the priority for all of the Issues owned by Alice Smith. The
difference is in how this information is structured (as a tuple or as
nested objects), but the important thing to understand is that
``User.<owner[IS Issue].status.name`` and ``User.<owner[IS
Issue].priority.name`` refer to the status and priority for the *same*
Issue. This means that the first query will return a tuple with the
status name and priority name for every Issue belonging to Alice. It
is not going to be a cross-product of the set of all status names with
the set of all priority names taken independently.

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
object. Consider a more complex query:

.. code-block:: eql

    WITH MODULE example
    SELECT User.<owner[IS Issue]
    FILTER
        User.name = 'Alice Smith'
        AND
        User.<owner[IS Issue].status.name = 'Open'
        AND
        User.<owner[IS Issue].priority.name = 'High';

In the above query there are two examples of a common sub-path:
``User`` and ``User.<owner[IS Issue]``. Breaking down the statement we
get the following features:

- the resulting set is composed of ``Issues`` reachable from a set
  of ``Users``, by following the link ``owner`` in reverse
  direction (since ``owner`` is a required link for ``Issue``,
  this happens to be the set of all ``Issues``)
- the set of ``Users`` is restricted such that every element of it
  must have the ``name`` 'Alice Smith' (so it happens to be a set
  of only one User)
- the set of ``Issues`` reachable from the set of ``Users`` is
  further restricted such that every element of it must have a
  ``status`` with the ``name`` 'Open'
- the set of ``Issues`` reachable from the set of ``Users`` is
  further restricted such that every element of it must have a
  ``priority`` with the ``name`` 'High'

To see how different scopes within the same expression affect the
interpretation, consider the following query:

.. code-block:: eql

    WITH
        MODULE example,
        A := 4
    SELECT User {
        name
    }
    ORDER BY User.name
    LIMIT A;

The ``ORDER BY`` clause is nested in the scope of ``SELECT``,
therefore it refers to the same ``User`` as ``SELECT`` does. This is
quite natural, since for ``FILTER`` and ``ORDER BY``, it makes sense
to refer to the objects being selected.

As was mentioned in the previous chapter, ``OFFSET`` and ``LIMIT``
clauses exist in a sibling scope w.r.t. the ``SELECT`` block. This
means that they still are in the same scope as the ``WITH`` block, but
cannot refer to the result of the ``SELECT`` block.

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name
    }
    ORDER BY User.name
    # this is an error
    LIMIT len(User.name);

Although, technically, the ``LIMIT`` clause can refer to ``User``, so
long as the resulting expression is a *singleton*.

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name
    }
    ORDER BY User.name
    # odd, but valid way of selecting all except last 2 users
    LIMIT count(User.name) - 2;

In this case ``User`` in the ``SELECT`` block is in a sibling scope to
``User`` in the ``LIMIT`` clause, so there's no clash of
interpretation. However, to highlight that they are in different
scopes, consider the following *invalid* query:

.. code-block:: eql

    WITH MODULE example
    SELECT res := User {
        name
    }
    ORDER BY res.name
    # this is no longer valid as 'res' is not defined
    # in the scope of LIMIT
    LIMIT count(res.name) - 2;


Aggregate functions
+++++++++++++++++++

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
scope, the :ref:`longest common prefix<ref_edgeql_paths_prefix>`
rule dictates that ``Issue`` must refer to the same object for both of
these expressions. This means that ``count`` is always operating on a
set of one ``Issue``.

The way to fix that is to define another set as ``Issue`` in the
``WITH`` clause.

.. code-block:: eql

    # Because Issue and I2 are not common prefixes, the count
    # will aggregate all issues (referred to as I2).
    WITH
        MODULE example,
        I2 := Issue
    SELECT
        'Open issue ' + Issue.number + ' / ' + <str>count(I2)
    FILTER Issue.status.name = 'Open';

The above query will produce the desired result. However, it is not
terribly efficient to re-calculate the total open issue count for
every string. A more optimal query would then be:

.. code-block:: eql

    WITH
        MODULE example,
        total := <str>count(Issue)
    SELECT
        'Open issue ' + Issue.number + ' / ' + total
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


.. _ref_edgeql_computables:

Sub-queries and computables
+++++++++++++++++++++++++++

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


Link properties
---------------

It is possible to have a path that represents a set of link properties
as opposed to link target values. Since link properties have to be
atomic, the step pointing to the link property is always the last step
in a path. The link property is accessed by using ``@`` instead
of ``.``.

Consider the following schema:

.. code-block:: eschema

    link favorites:
        linkproperty rank to int

    concept Post:
        required link body to str
        required link owner to User

    concept User extending std::Named:
        link favorites to Post:
            mapping: **

Then the query selecting all favorite Post sorted by their rank is:

.. code-block:: eql

    WITH MODULE example
    SELECT User.favorites
    ORDER BY User.favorites@rank;
