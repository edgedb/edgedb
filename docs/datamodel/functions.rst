.. _ref_datamodel_functions:

=========
Functions
=========


Functions are ways to transform one set of data into another.  They
are defined in :ref:`modules <ref_datamodel_modules>` and are part of
the database schema.

For example, consider the :ref:`function <ref_std>`
:eql:func:`len` used to transform a set of :eql:type:`str` into a set
of :eql:type:`int64`:

.. code-block:: edgeql-repl

    db> SELECT len({'hello', 'world'});
    {5, 5}

This behavior is known as an *element-wise* operation. Many built-in
and user-defined functions operate on elements. In case of multiple
arguments, a cross-product of all the input sets is computed to
determine all the input combinations that the function needs to be
applied to. After that, the function is applied to each element of the
cross-product. The results of this function application form the
output set. This implies that if any of the input sets are empty, the
result of applying an element-wise function is also empty.

Compare that with the :ref:`aggregate <ref_eql_fundamentals_aggregates>`
function :eql:func:`count` that transforms a set of :eql:type:`str`
into a single :eql:type:`int64` value, representing the set
cardinality.

For example, :eql:func:`count` maps a set to an integer, specifically
it returns the number of elements in a set:

.. code-block:: edgeql-repl

    db> SELECT count({'hello', 'world'});
    {2}


Here's an example of :eql:func:`array_agg` mapping a set to an array and
ordering it in the process:

.. code-block:: edgeql-repl

    db> WITH names := {'Alice', 'Dana', 'Billie', 'Cameron'}
    ... SELECT array_agg(names ORDER BY names);
    {['Alice', 'Billie', 'Cameron', 'Dana']}


User-defined Functions
----------------------

It is also possible to define custom functions. For example, consider
a function that adds an exclamation mark ``'!'`` at the end of the
string:

.. code-block:: sdl

    function exclamation(word: str) -> str
        using (word ++ '!');

This function accepts a :eql:type:`str` as an argument and produces a
:eql:type:`str` as output as well.

.. code-block:: edgeql-repl

    test> SELECT exclamation({'Hello', 'World'});
    {'Hello!', 'World!'}

In order to make sure that the function is called when the argument is
an empty set ``{}`` we make the argument :ref:`optional
<ref_eql_fundamentals_optional>`. We also provide a default value of
``{}`` if the argument is omitted entirely. Here are some results this
function produces:

.. code-block:: edgeql-repl

    test> SELECT exclamation({'Hello', 'World'});
    {'Hello!', 'World!'}
    test> SELECT exclamation(<str>{});
    {'!!!'}
    test> SELECT exclamation();
    {'!!!'}


See Also
--------

Function
:ref:`SDL <ref_eql_sdl_functions>`,
:ref:`DDL <ref_eql_ddl_functions>`,
and :ref:`introspection <ref_eql_introspection_functions>`.
