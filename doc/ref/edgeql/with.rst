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
Consider the following query that gets all the concepts defined in the
``example`` module and counts how many actual Objects of that type
exist in the DB.

.. code-block:: eql

    WITH
        MODULE example,
        s := MODULE schema
    SELECT s::Concept {
        name,
        count := count(
            (SELECT Object
             FILTER Object.__class__ = s::Concept)
        )
    }
    FILTER .name LIKE 'example::%';


Expression alias
----------------

It is possible to specify an expression alias in the ``WITH`` block.
Since this generates sub-scopes, aliases can be used to refer to
different instances of the same *concept* in a query. For example, the
following query will find all users who own the same number of issues
as someone else:

.. code-block:: eql

    WITH
        MODULE example,
        U2 := User
    SELECT User {
        name,
        issue_count := count(User.<owner[IS Issue])
    }
    FILTER
        User.issue_count = count((
            SELECT U2.<owner[IS Issue]
            FILTER U2 != User
        ));
