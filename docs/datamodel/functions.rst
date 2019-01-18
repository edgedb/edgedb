.. _ref_datamodel_functions:

=========
Functions
=========


Definition
==========

A function may be defined in EdgeDB Schema using the ``function`` declaration:

.. eschema:synopsis::

    function <funcname> ([<argspec>] [, ...]) -> <returnspec>:
        from <language> := <functionbody>
        [ initial value := <initial-value> ]
        [ <attribute-declarations> ]

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # <argkind> is:

    [ { variadic | named only } ]

    # <typequal> is:

    [ { set of | optional } ]

    # and <returnspec> is:

    [ <typequal> ] <rettype>


Parameters
----------

:eschema:synopsis:`<funcname>`
    The function name.

:eql:synopsis:`<argkind>`
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

:eql:synopsis:`<argname>`
    The name of an argument.  If ``named only`` modifier is used this
    argument *must* be passesd using this name only.

:eql:synopsis:`<typequal>`
    The type qualifier: ``set of`` or ``optional``.

    The ``set of`` qualifier indicates that the function is taking the
    argument as a *whole set*, as opposed to being called on the input
    product element-by-element.

    The ``optional`` qualifier indicates that the function will be called
    if the argument is an empty set.  The default behavior is to return
    an empty set if the argument is not marked as ``optional``.

:eschema:synopsis:`<argtype>`
    The data type of the function's arguments
    (optionally module-qualified).

:eschema:synopsis:`<default>`
    An expression to be used as default value if the parameter is not
    specified.  The expression has to be of a type compatible with the
    type of the argument.

:eschema:synopsis:`<rettype>`
    The return data type (optionally module-qualified).
    The ``set of`` modifier indicates that the function will return
    a non-singleton set.

    The ``optional`` qualifier indicates that the function may return
    an empty set.

:eschema:synopsis:`<language>`
    The name of the language that the function is implemented in.
    The only currently supported value is ``edgeql``.

:eschema:synopsis:`<functionbody>`
    A string constant defining the function.


DDL
===

Functions can also be defined using the :eql:stmt:`CREATE FUNCTION`
EdgeQL command.
