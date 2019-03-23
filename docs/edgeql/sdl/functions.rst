.. _ref_eql_sdl_functions:

=========
Functions
=========

This section describes the SDL declarations pertaining to
:ref:`functions <ref_datamodel_functions>`.

Define a new function corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_functions>`.

.. sdl:synopsis::

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    from <language> <functionbody> ;

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    "{"
        from <language> <functionbody> ;
        [ <attribute-declarations> ]
    "}" ;

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # <argkind> is:

    [ { variadic | named only } ]

    # <typequal> is:

    [ { set of | optional } ]

    # and <returnspec> is:

    [ <typequal> ] <rettype>


Description
-----------

:sdl:synopsis:`<name>`
    The name (optionally module-qualified) of the function to create.

:sdl:synopsis:`<argkind>`
    The kind of an argument: ``variadic`` or ``named only``.

    If not specified, the argument is called *positional*.

    The ``variadic`` modifier indicates that the function takes an
    arbitrary number of arguments of the specified type.  The passed
    arguments will be passed as as array of the argument type.
    Positional arguments cannot follow a ``variadic`` argument.
    ``variadic`` parameters cannot have a default value.

    The ``named only`` modifier indicates that the argument can only
    be passed using that specific name.  Positional arguments cannot
    follow a ``named only`` argument.

:sdl:synopsis:`<argname>`
    The name of an argument.  If ``named only`` modifier is used this
    argument *must* be passed using this name only.

:sdl:synopsis:`<typequal>`
    The type qualifier: ``set of`` or ``optional``.

    The ``set of`` qualifier indicates that the function is taking the
    argument as a *whole set*, as opposed to being called on the input
    product element-by-element.

    The ``optional`` qualifier indicates that the function will be called
    if the argument is an empty set.  The default behavior is to return
    an empty set if the argument is not marked as ``optional``.

:sdl:synopsis:`<argtype>`
    The data type of the function's arguments
    (optionally module-qualified).

:sdl:synopsis:`<default>`
    An expression to be used as default value if the parameter is not
    specified.  The expression has to be of a type compatible with the
    type of the argument.

:sdl:synopsis:`<rettype>`
    The return data type (optionally module-qualified).

    The ``set of`` modifier indicates that the function will return
    a non-singleton set.

    The ``optional`` qualifier indicates that the function may return
    an empty set.

:sdl:synopsis:`<language>`
    The name of the language that the function is implemented in.
    Currently it can only be ``edgeql``.

:sdl:synopsis:`<functionbody>`
    A string constant defining the function.  It is often helpful
    to use :ref:`dollar quoting <ref_eql_lexical_dollar_quoting>`
    to write the function definition string.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_eql_sdl_schema_attributes>` declarations.
