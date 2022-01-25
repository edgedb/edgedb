.. _ref_eql_statements_with:

With block
==========

:index: alias module

.. eql:keyword:: with

    The ``with`` block in EdgeQL is used to define aliases.

    The expression aliases are evaluated in the lexical scope they appear in,
    not the scope where their alias is used. This means that refactoring
    queries using aliases must be done with care so as not to alter the query
    semantics.

Specifying a module
+++++++++++++++++++

.. eql:keyword:: module

    Used inside a ``with`` block to specify module names.

One of the more basic and common uses of the ``with`` block is to
specify the default module that is used in a query. ``with module
<name>`` construct indicates that whenever an identifier is used
without any module specified explicitly, the module will default to
``<name>`` and then fall back to built-ins from ``std`` module.

The following queries are exactly equivalent:

.. code-block:: edgeql

    with module example
    select User {
        name,
        owned := (select
            User.<owner[is Issue] {
                number,
                body
            }
        )
    }
    filter User.name like 'Alice%';

    select example::User {
        name,
        owned := (select
            example::User.<owner[is example::Issue] {
                number,
                body
            }
        )
    }
    filter example::User.name like 'Alice%';


It is also possible to define aliased modules in the ``with`` block.
Consider the following query that needs to compare objects
corresponding to concepts defined in two different modules.

.. code-block:: edgeql

    with
        module example,
        f as module foo
    select User {
        name
    }
    filter .name = f::Foo.name;

Another use case is for giving short aliases to long module names
(especially if module names contain ``.``).

.. code-block:: edgeql

    with
        module example,
        fbz as module foo.bar.baz
    select User {
        name
    }
    filter .name = fbz::Baz.name;


Local Expression Aliases
++++++++++++++++++++++++

It is possible to define an alias for an arbitrary expression. The result
set of an alias expression behaves as a completely independent set of a
given name. The contents of the set are determined by the expression
at the point where the alias is defined. In terms of scope, the alias
expression in the ``with`` block is in a sibling scope to the rest
of the query.

It may be useful to factor out a common sub-expression from a larger
complex query. This can be done by assigning the sub-expression a new
symbol in the ``with`` block. However, care must be taken to ensure
that this refactoring doesn't alter the meaning of the expression due
to scope change.

All expression aliases defined in a ``with`` block must be referenced in
the body of the query.

.. code-block:: edgeql

    # Consider a query to get all users that own Issues and the
    # comments those users made.
    with module example
    select Issue.owner {
        name,
        comments := Issue.owner.<owner[is Comment]
    };

    # The above query can be refactored like this:
    with
        module example,
        U := Issue.owner
    select U {
        name,
        comments := U.<owner[is Comment]
    };

An example of incorrect refactoring would be:

.. code-block:: edgeql

    # This query gets a set of tuples of
    # issues and their owners.
    with
        module example
    select (Issue, Issue.owner);

    # This query gets a set of tuples that
    # result from a cartesian product of all issues
    # with all owners. This is because ``Issue`` and ``U``
    # are considered independent sets.
    with
        module example,
        U := Issue.owner
    select (Issue, U);


Detached
++++++++

.. eql:keyword:: detached

    The ``detached`` keyword marks an expression as not belonging to
    any scope.

A ``detached`` expression allows referring to some set as if it were
defined in the top-level ``with`` block. Basically, ``detached``
expressions ignore all current scopes they are nested in and only take
into account module aliases. The net effect is that it is possible to
refer to an otherwise related set as if it were unrelated:

.. code-block:: edgeql

    with module example
    update User
    filter .name = 'Dave'
    set {
        friends := (select detached User filter .name = 'Alice'),
        coworkers := (select detached User filter .name = 'Bob')
    };

Here you can use the ``detached User`` expression, rather than having to
define ``U := User`` in the ``with`` block just to allow it to be used
in the body of the ``update``. The goal is to indicate that the
``User`` in the ``update`` body is not in any way related to the
``User`` that's being updated.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > With <ref_eql_with>`
