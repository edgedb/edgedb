.. _ref_eql_with:

WITH block
==========

:index: alias module

.. eql:keyword:: WITH

    The ``WITH`` block in EdgeQL is used to define aliases.

    The expression aliases are evaluated in the lexical scope they appear in,
    not the scope where their alias is used. This means that refactoring
    queries using aliases must be done with care so as not to alter the query
    semantics.

Specifying a module
+++++++++++++++++++

.. eql:keyword:: MODULE

    Used inside a :eql:kw:`WITH block <WITH>` to specify module names.

One of the more basic and common uses of the ``WITH`` block is to
specify the default module that is used in a query. ``WITH MODULE
<name>`` construct indicates that whenever an identifier is used
without any module specified explicitly, the module will default to
``<name>`` and then fall back to built-ins from ``std`` module.

The following queries are exactly equivalent:

.. code-block:: edgeql

    WITH MODULE example
    SELECT User {
        name,
        owned := (SELECT
            User.<owner[IS Issue] {
                number,
                body
            }
        )
    }
    FILTER User.name LIKE 'Alice%';

    SELECT example::User {
        name,
        owned := (SELECT
            example::User.<owner[IS example::Issue] {
                number,
                body
            }
        )
    }
    FILTER example::User.name LIKE 'Alice%';


It is also possible to define aliases modules in the ``WITH`` block.
Consider the following query that needs to compare objects
corresponding to concepts defined in two different modules.

.. code-block:: edgeql

    WITH
        MODULE example,
        f AS MODULE foo
    SELECT User {
        name
    }
    FILTER .name = f::Foo.name;

Another use case is for giving short aliases to long module names
(especially if module names contain ``.``).

.. code-block:: edgeql

    WITH
        MODULE example,
        fbz AS MODULE foo.bar.baz
    SELECT User {
        name
    }
    FILTER .name = fbz::Baz.name;


Local Expression Aliases
++++++++++++++++++++++++

It is possible to define an alias for an arbitrary expression. The result
set of an alias expression behaves as a completely independent set of a
given name. The contents of the set are determined by the expression
at the point where the alias is defined. In terms of scope, the alias
expression in the ``WITH`` block is in a sibling scope to the rest
of the query.

It may be useful to factor out a common sub-expression from a larger
complex query. This can be done by assigning the sub-expression a new
symbol in the ``WITH`` block. However, care must be taken to ensure
that this refactoring doesn't alter the meaning of the expression due
to scope change.

All expression aliases defined in a ``WITH`` block must be referenced in
the body of the query.

.. code-block:: edgeql

    # Consider a query to get all users that own Issues and the
    # comments those users made.
    WITH MODULE example
    SELECT Issue.owner {
        name,
        comments := Issue.owner.<owner[IS Comment]
    };

    # The above query can be refactored like this:
    WITH
        MODULE example,
        U := Issue.owner
    SELECT U {
        name,
        comments := U.<owner[IS Comment]
    };

An example of incorrect refactoring would be:

.. code-block:: edgeql

    # This query gets a set of tuples of
    # issues and their owners.
    WITH
        MODULE example
    SELECT (Issue, Issue.owner);

    # This query gets a set of tuples that
    # result from a cartesian product of all issues
    # with all owners. This is because ``Issue`` and ``U``
    # are considered independent sets.
    WITH
        MODULE example,
        U := Issue.owner
    SELECT (Issue, U);


.. _ref_eql_with_detached:

Detached
++++++++

A ``DETACHED`` expression allows referring to some set as if it were
defined in the top-level ``WITH`` block. Basically, ``DETACHED``
expressions ignore all current scopes they are nested in and only take
into account module aliases. The net effect is that it is possible to
refer to an otherwise related set as if it were unrelated:

.. code-block:: edgeql

    WITH MODULE example
    UPDATE User
    FILTER .name = 'Dave'
    SET {
        friends := (SELECT DETACHED User FILTER .name = 'Alice'),
        coworkers := (SELECT DETACHED User FILTER .name = 'Bob')
    };

Rather than having to define ``U := User`` in the ``WITH`` block only
so that it could be used in the body of the ``UPDATE`` the ``DETACHED
User`` expression can be used. The goal is to indicate that the
``User`` in the ``UPDATE`` body is not in any way related to the
``User`` that's being updated.
