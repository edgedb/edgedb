.. _ref_datamodel_scalars_enum:

Enumerated Types
================

.. eql:type:: std::enum

    :index: enum

    An enumerated type is a data type consisting of an order list of values.

    An enumeration type can be declared in a schema declaration using
    the following syntax:

    .. eql:synopsis::

        enum "<" <enum-values> ">"

    Where :eql:synopsis:`<enum-values>` is a comma-separated list of
    quoted string constants comprising the enum type.  Currently, the
    only valid application of the enum declaration is to define an
    enumerated scalar type:

    .. code-block:: edgeql-repl

        db> CREATE SCALAR TYPE my_enum_t EXTENDING enum<'One', 'Two'>;
        CREATE TYPE
