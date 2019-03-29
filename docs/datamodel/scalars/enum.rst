.. _ref_datamodel_scalars_enum:

Enumerated Types
================

.. eql:type:: std::enum

    :index: enum

    An enumerated type is a data type consisting of an order list of values.

    An enumeration type can be declared in a schema declaration using
    the following :ref:`syntax <ref_eql_types_enum>`:

    .. code-block:: sdl

        scalar type color_enum_t extending enum<'red', 'green', 'blue'>;

    :ref:`Casting <ref_eql_expr_typecast>` is required to obtain an
    enum value in an expression:

    .. code-block:: edgeql-repl

        db> SELECT 'red' IS color_enum_t;
        {false}
        db> SELECT <color_enum_t>'red' IS color_enum_t;
        {true}

    For details about enum introspection see :ref:`this section
    <ref_eql_introspection_scalar_types>`.
