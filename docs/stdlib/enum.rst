.. _ref_std_enum:

=====
Enums
=====

:edb-alt-title: Enum Type


.. eql:type:: std::enum

    :index: enum

    An enumerated type is a data type consisting of an ordered list of values.

    An enum type can be declared in a schema by using the following
    syntax:

    .. code-block:: sdl

        scalar type Color extending enum<Red, Green, Blue>;

    Enum values can then be accessed directly:

    .. code-block:: edgeql-repl

        db> select Color.Red is Color;
        {true}

    :eql:op:`Casting <cast>` can be used to obtain an
    enum value in an expression:

    .. code-block:: edgeql-repl

        db> select 'Red' is Color;
        {false}
        db> select <Color>'Red' is Color;
        {true}
        db> select <Color>'Red' = Color.Red;
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

        Currently, enum values cannot be longer than 63 characters.
