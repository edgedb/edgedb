.. _ref_datamodel_scalars_casts:

=====
Casts
=====

There are different ways that casts appear in EdgeQL.


Explicit Casts
--------------

An *explicit cast* is just using the angle brackets :eql:op:`<type>
<CAST>`:

.. code-block:: edgeql

    # Cast a string literal into an integer.
    SELECT <int64>"42";


Assignment Casts
----------------

*Assignment casts* happen when inserting new objects. Numeric types
will often be automatically cast into the specific type corresponding
to the property they are assigned to. This is to avoid extra typing
when dealing with numeric value using fewer bits:

.. code-block:: edgeql

    # Automatically cast a literal 42 (which is int64
    # by default) into an int16 value.
    INSERT MyObject {
        int16_val := 42
    };

If *assignment* casting is supported for a given pair of types,
*explicit* casting of those types is also supported.


Implicit Casts
--------------

*Implicit casts* happen automatically whenever the value type doesn't
match the expected type in an expression. This is mostly supported for
numeric casts that don't incur any potential information loss (in form
of truncation), so typically from a less precise type, to a more
precise one. The :eql:type:`int64` to :eql:type:`float64` is a notable
exception, which can suffer from truncation of significant digits for
very large integer values. There are a few scenarios when *implicit
casts* can occur:

1) Passing arguments that don't match exactly the types in the
   function signature:

   .. code-block:: edgeql-repl

        db> WITH x := <float32>12.34
        ... SELECT math::ceil(x);
        {13}

   The function :eql:func:`math::ceil` only takes :eql:type:`int64`,
   :eql:type:`float64`, :eql:type:`bigint`, or :eql:type:`decimal` as
   its argument. So the :eql:type:`float32` value will be *implicitly
   cast* into a :eql:type:`float64` in order to match a valid
   signature.

2) Using operands that don't match exactly the types in the
   operator signature (this works the same way as for functions):

   .. code-block:: edgeql-repl

        db> SELECT 1 + 2.3;
        {3.3}

   The operator :eql:op:`+ <PLUS>` is defined only for operands of
   the same type, so in the expression above the :eql:type:`int64`
   value ``1`` is *implicitly cast* into a :eql:type:`float64` in
   order to match the other operand and produce a valid signature.

3) Mixing different numeric types in a set:

   .. code-block:: edgeql-repl

        db> SELECT {1, 2.3, <float32>4.5} IS float64;
        {true, true, true}

   All elements in a set have to be of the same type, so the values
   are cast into :eql:type:`float64` as that happens to be the common
   type to which all the set elements can be *implicitly cast*. This
   would work out the same way if :eql:op:`UNION` was used instead:

   .. code-block:: edgeql-repl

        db> SELECT (1 UNION 2.3 UNION <float32>4.5) IS float64;
        {true, true, true}

If *implicit* casting is supported for a given pair of types,
*assignment* and *explicit* casting of those types is also supported.


Casting Table
-------------

.. csv-table::
    :file: casts.csv
    :class: vertheadertable

- ``<>`` - can be cast explicitly
- ``:=`` - assignment cast is supported
- "impl" - implicit cast is supported
