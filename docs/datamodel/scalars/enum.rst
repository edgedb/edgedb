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

        scalar type Color extending enum<'red', 'green', 'blue'>;

    :eql:op:`Casting <CAST>` is required to obtain an
    enum value in an expression:

    .. code-block:: edgeql-repl

        db> SELECT 'red' IS Color;
        {false}
        db> SELECT <Color>'red' IS Color;
        {true}

    .. note::

        The enum values in EdgeQL are string-like in the fact that
        they can contain any characters that the strings can. This is
        different from some other languages where enum values are
        identifier-like and thus cannot contain some characters. For
        example, when working with GraphQL enum values that contain
        characters that aren't allowed in identifiers cannot be
        properly reflected. To address this, consider using only
        identifier-like enum values in cases where such compatibility
        is needed.

See Also
--------

Enum scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
and :ref:`introspection <ref_eql_introspection_scalar_types>`.
