.. _ref_edgeql_with:


With block
==========

The ``WITH`` block in EdgeQL is used to define scope and aliases.

Specifying a module
-------------------

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



Expression alias
----------------

It is possible to specify an expression alias in the ``WITH`` block.
Since every aliased expression exists in its own
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
