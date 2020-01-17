.. _ref_datamodel_scalars_str:

String
======

:edb-alt-title: String Type


.. eql:type:: std::str

    :index: continuation cont

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

    There are two kinds of string literals in EdgeQL: regular and *raw*.
    Raw strings literals do not evaluate ``\``, so ``\n`` in in a raw string
    is two characters ``\`` and ``n``.

    The regular string literal syntax is ``'a string'`` or a ``"a string"``.
    Two *raw* string syntaxes are illustrated below:

    .. code-block:: edgeql-repl

        db> SELECT r'a raw \\\ string';
        {'a raw \\\ string'}
        db> SELECT $$something$$;
        {'something'}
        db> SELECT $marker$something $$
        ... nested \!$$$marker$;
        {'something $$
        nested \!$$'}

    Regular strings use ``\`` to indicate line continuation. When a
    line continuation symbol is encountered the symbol itself as well
    as all the whitespace characters up to the next non-whitespace
    character are omitted from the string:

    .. code-block:: edgeql-repl

        db> SELECT 'Hello, \
        ...         world';
        {'"Hello, world"'}


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
:ref:`string functions and operators <ref_eql_funcops_string>`,
and :ref:`string literal lexical structure <ref_eql_lexical_str>`.
