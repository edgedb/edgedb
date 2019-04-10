.. _ref_datamodel_scalars_str:

String
======

:edb-alt-title: String Type


.. eql:type:: std::str

    A unicode string of text.

    Any other type (except :eql:type:`bytes`) can be
    :eql:op:`cast <CAST>` to and from a string:

    .. code-block:: edgeql-repl

        db> SELECT <str>42;
        {'42'}
        db> SELECT <bool>'true';
        {true}
        db> SELECT "I ❤️ EdgeDB";
        {'I ❤️ EdgeDB'}

    Note that when a :eql:type:`str` is cast into a :eql:type:`json`,
    the result is JSON string value. Same applies for casting back
    from :eql:type:`json` - only a JSON string value can be cast into
    a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> SELECT <json>'Hello, world';
        {'"Hello, world"'}


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
and :ref:`string functions and operators <ref_eql_funcops_string>`.
