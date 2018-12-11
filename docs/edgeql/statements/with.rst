.. _ref_eql_with:

WITH block
==========

:index: cardinality alias module view detached

.. eql:keyword:: WITH

    The ``WITH`` block in EdgeQL is used to define aliases.

    .. XXX: not just for aliases! e.g. WITH CARDINALITY

    In case of aliased expressions, those expressions are evaluated in
    the lexical scope they appear in, not the scope where their alias
    is used. This means that refactoring queries using aliases must be
    done with care so as not to alter the query semantics.

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


Cardinality
+++++++++++

Typically the cardinality of an expression can be statically
determined from the individual parts. Sometimes it is necessary to
specify the cardinality explicitly. For example, when using
computables in shapes it may be desirable to specify the cardinality
of the computable because it affects serialization.

.. code-block:: edgeql

    WITH
        MODULE example
    SELECT User {
        name,
        multi nicknames := (SELECT 'Foo')
    };

Cardinality is normally statically inferred from the query, so
overruling this inference may only be done to *relax* the cardinality.
This means that the only valid cardinality specification is
``CARDINALITY '*'``, when attempting to override a possibility that
the cardinality is provably ``'1'``.


Expressions
+++++++++++

It is possible to define an alias for some expression. The aliased set
behaves as a completely independent set of a given name. The contents
of the set are determined by the expression at the point where the
alias is defined. In terms of scope, the aliased expression in the
``WITH`` block is in a sibling scope to the rest of the query.

It may be useful to factor out a common sub-expression from a larger
complex query. This can be done by assigning the sub-expression a new
symbol in the ``WITH`` block. However, care must be taken to ensure
that this refactoring doesn't alter the meaning of the expression due
to scope change.

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


Detached
++++++++

It is possible to specify an aliased view in the ``WITH`` block using
``DETACHED`` expression. A ``DETACHED`` expression can be interpreted
as if a schema-level view had been defined for that expression. All
``DETACHED`` expressions completely ignore the current scope. In
principle, a ``DETACHED`` expression in the top-level ``WITH`` block
is equivalent to a regular aliased expression.

For example, the following query will find all users who
own the same number of issues as someone else:

.. todo::

    Need a better DETACHED example, with nested sub-queries and
    possibly motivated by keeping the symbols closer to their place if
    usage.

.. code-block:: edgeql

    WITH
        MODULE example,
        U2 := DETACHED User
    # U2 and User in the SELECT clause now refer to the same concept,
    # but different objects, as if a schema level view U2 had been
    # defined.
    SELECT User {
        name,
        issue_count := count(User.<owner[IS Issue])
    }
    FILTER
        User.issue_count = count((
            SELECT U2.<owner[IS Issue]
            FILTER U2 != User
        ));
