.. _ref_datamodel_collection_types:

================
Collection Types
================

*Collection types* are special generic types used to group homogeneous or
heterogeneous data.


.. eql:type:: std::tuple

    :index: tuple

    A tuple type is a heterogeneous sequence of other types.

    Tuple elements can optionally have names,
    in which case the tuple is called a *named tuple*.

    A tuple type can be explicitly declared in an expression or schema
    declaration using the following syntax:

    .. eql:synopsis::

        tuple "<" <element-type>, [<element-type>, ...] ">"

    A named tuple:

    .. eql:synopsis::

        tuple "<" <element-name> := <element-type> [, ... ] ">"

    Any type can be used as a tuple element type.

    A tuple type is created implicitly when a
    :ref:`tuple constructor <ref_eql_expr_tuple_ctor>` is
    used:

    .. code-block:: edgeql-repl

        db> SELECT ('foo', 42).__type__.name;
        {"std::tuple<std::str, std::int64>"}

    Two tuples are equal if all of their elements are equal and in the same
    order.  Note that element names in named tuples are not significant for
    comparison:

    .. code-block:: edgeql-repl

        db> SELECT (1, 2, 3) = (a := 1, b := 2, c := 3);
        {True}


.. eql:type:: std::array

    :index: array

    Arrays represent a one-dimensional homogeneous ordered list.

    Array indexing starts at zero.

    An array type can be explicitly defined in an expression or schema
    declaration using the following syntax:

    .. eql:synopsis::

        array "<" <element_type> ">"

    With the exception of other array types, any type can be used as an
    array element type.

    An array type is created implicitly when an
    :ref:`array constructor <ref_eql_expr_array_ctor>` is
    used:

    .. code-block:: edgeql-repl

        db> SELECT [1, 2].__type__;
        {"std::array<std::int64>"}
