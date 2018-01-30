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
