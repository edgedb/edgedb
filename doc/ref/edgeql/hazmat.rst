.. _ref_edgeql_hazmat:


Mistakes and misconceptions
===========================

This section covers some of the non-obvious mistakes and misconceptions
when using EdgeQL.

Empty set
---------

If any of the operand sets for a element operation is ``{}``, the
result is also ``{}`` (since there are no elements produced in the
Cartesian product). This is particularly important for comparisons and
boolean logic operations as all of the following evaluate to ``{}``:

.. code-block:: eql

    SELECT TRUE OR {};
    SELECT FALSE AND {};
    SELECT {} = {};

This can lead to subtle mistakes when using actual paths that involve
non-required links (or the roots of which might not exists):

.. code-block:: eql

    # will evaluate to {} if either 'a' or 'b' link is missing on a
    # given object Foo
    SELECT Foo.a OR Foo.b;

When the desired behavior is to treat ``{}`` as equivalent to
``FALSE``, the coalesce ``??`` operator should be used:

.. code-block:: eql

    # will treat missing 'a' or 'b' links as equivalent to FALSE
    SELECT Foo.a ?? FALSE OR Foo.b ?? FALSE;

Aggregates
----------

Aggregate functions operate on a set as a whole. This means that any
externally defined ``FILTER`` clause cannot affect the contents of
that set. Instead, if filtering is required, the ``FILTER`` must be
applied to the argument directly:

.. code-block:: eql

    # here FILTER will not affect the count
    WITH MODULE example
    SELECT count(User)
    FILTER User.name LIKE 'A%';

    # here FILTER will change the argument of count
    WITH MODULE example
    SELECT count(User FILTER User.name LIKE 'A%');

Operator ``IN`` is identical, but syntactically less obvious case.
Consider the following queries:

.. code-block:: eql

    # here FILTER will not affect the Issue.owner in IN
    WITH MODULE example
    SELECT (User.name, User IN Issue.owner)
    FILTER Issue.number <= '3';

    # here FILTER will change the argument of IN
    WITH MODULE example
    SELECT (
        User.name,
        User IN (SELECT Issue.owner FILTER Issue.number <= '3')
    );
