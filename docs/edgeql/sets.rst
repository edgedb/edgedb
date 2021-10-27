

Sets
====

- Everything is a set
- Type and cardinality
- Set constructor
- Literals are sets
- Set references
- Operators
- Assertions


.. _ref_eql_everything_is_a_set:

Everything is a Set
-------------------

Every value in EdgeQL is viewed as a set of elements.  A set may be empty
(*empty set*), contain a single element (a *singleton*), or contain multiple
elements.  Strictly speaking, EdgeQL sets are *multisets*, as they do not
require the elements to be unique.

A set cannot contain elements of different base types.  Mixing objects and
primitive types, as well as primitive types with a different base type, is
not allowed.

In SQL databases ``NULL`` is a special *value* denoting an absence of data.
EdgeDB works with *sets*, so an absence of data is just an empty set.

.. _ref_eql_set_constructor:

Set Constructor
---------------

A *set constructor* is an expression that consists of a sequence of
comma-separated expressions enclosed in curly braces:

.. eql:synopsis::

    "{" <expr> [, ...] "}"

A set constructor produces the result by appending its elements.  It is
perfectly equivalent to a sequence of :eql:op:`UNION` operators.

An *empty set* can also be created by omitting all elements.
In situations where EdgeDB cannot infer the type of an empty set,
it must be used together with a type cast:

.. code-block:: edgeql-repl

    db> SELECT {};
    EdgeQLError: could not determine the type of empty set

    db> SELECT <int64>{};
    {}

.. _ref_eql_set_references:


Set References
--------------

A set reference is a *name* (a simple identifier or a qualified schema name)
that represents a set of values.  It can be the name of an object type or
an *expression alias* (defined in a statement :ref:`WITH block <ref_eql_with>`
or in the schema via an :ref:`alias declaration <ref_eql_sdl_aliases>`).

For example, in the following query ``User`` is a set reference:

.. code-block:: edgeql

    SELECT User;

See :ref:`this section <ref_eql_set_references>` for more
information about set references.

.. important::

    In EdgeQL a name can either be *fully-qualified*, i.e. of the form
    ``module_name::entity_name`` or in short form of just ``entity_name``
    (for more details see :ref:`ref_eql_lexical_names`). Any short name is
    ultimately resolved to some fully-qualified name in the following
    manner:

    1) Look for a match to the short name in the current module (typically
    ``default``, but it can be changed).
    2) Look for a match to the short name in the ``std`` module.
    3)

    Normally the current module is called ``default``, which is
    automatically created in any new database. It is possible to override
    the current module globally on the session level with a ``SET MODULE
    my_module`` :ref:`command <ref_eql_statements_session_set_alias>`. It
    is also possible to override the current module on per-query basis
    using ``WITH MODULE my_module`` :ref:`clause <ref_eql_with>`.
