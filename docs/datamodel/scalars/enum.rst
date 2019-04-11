.. _ref_datamodel_scalars_enum:

Enum
====

:edb-alt-title: Enumerated Type


.. eql:type:: std::enum

    :index: enum

    An enumerated type is a data type consisting of an ordered list of values.

    An enumerated type can be declared in a schema declaration using
    the following :ref:`syntax <ref_eql_types_enum>`:

    .. code-block:: sdl

        scalar type color_enum_t extending enum<'red', 'green', 'blue'>;

    :eql:op:`Casting <CAST>` is required to obtain an
    enum value in an expression:

    .. code-block:: edgeql-repl

        db> SELECT 'red' IS color_enum_t;
        {false}
        db> SELECT <color_enum_t>'red' IS color_enum_t;
        {true}


See Also
--------

Enum scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
and :ref:`introspection <ref_eql_introspection_scalar_types>`.
