.. _ref_eql_sdl_functions:

=========
Functions
=========

This section describes the SDL declarations pertaining to
:ref:`functions <ref_datamodel_functions>`.


Example
-------

Declare a custom function that concatenates the length of a string to
the end of the that string:

.. code-block:: sdl

    function foo(s: str) -> str
        using (
            select s ++ <str>len(a)
        );

.. _ref_eql_sdl_functions_syntax:

Syntax
------

Define a new function corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_functions>`.

.. sdl:synopsis::

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    using ( <edgeql> );

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    using <language> <functionbody> ;

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    "{"
        [ <annotation-declarations> ]
        [ volatility := {'Immutable' | 'Stable' | 'Volatile' | 'Modifying'} ]
        [ using ( <expr> ) ; ]
        [ using <language> <functionbody> ; ]
        [ ... ]
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



This declaration defines a new constraint with the following options:

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the function to create.

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
    argument *must* be passed using this name only.

.. _ref_sdl_function_typequal:

:eql:synopsis:`<typequal>`
    The type qualifier: ``set of`` or ``optional``.

    The ``set of`` qualifier indicates that the function is taking the
    argument as a *whole set*, as opposed to being called on the input
    product element-by-element.

    User defined functions can not use ``set of`` arguments.

    The ``optional`` qualifier indicates that the function will be called
    if the argument is an empty set.  The default behavior is to return
    an empty set if the argument is not marked as ``optional``.

:eql:synopsis:`<argtype>`
    The data type of the function's arguments
    (optionally module-qualified).

:eql:synopsis:`<default>`
    An expression to be used as default value if the parameter is not
    specified.  The expression has to be of a type compatible with the
    type of the argument.

.. _ref_sdl_function_rettype:

:eql:synopsis:`<rettype>`
    The return data type (optionally module-qualified).

    The ``set of`` modifier indicates that the function will return
    a non-singleton set.

    The ``optional`` qualifier indicates that the function may return
    an empty set.

The valid SDL sub-declarations are listed below:

:eql:synopsis:`volatility := {'Immutable' | 'Stable' | 'Volatile' | 'Modifying'}`
    Function volatility determines how aggressively the compiler can
    optimize its invocations.

    If not explicitly specified the function volatility is
    :ref:`inferred <ref_reference_volatility>` from the function body.

    * An ``Immutable`` function cannot modify the database and is
      guaranteed to return the same results given the same arguments
      *in all statements*.

    * A ``Stable`` function cannot modify the database and is
      guaranteed to return the same results given the same
      arguments *within a single statement*.

    * A ``Volatile`` function cannot modify the database and can return
      different results on successive calls with the same arguments.

    * A ``Modifying`` function can modify the database and can return
      different results on successive calls with the same arguments.

:eql:synopsis:`using ( <expr> )`
    Specified the body of the function.  :eql:synopsis:`<expr>` is an
    arbitrary EdgeQL expression.

:eql:synopsis:`using <language> <functionbody>`
    A verbose version of the :eql:synopsis:`using` clause that allows
    to specify the language of the function body.

    * :eql:synopsis:`<language>` is the name of the language that
      the function is implemented in.  Currently can only be ``edgeql``.

    * :eql:synopsis:`<functionbody>` is a string constant defining
      the function.  It is often helpful to use
      :ref:`dollar quoting <ref_eql_lexical_dollar_quoting>`
      to write the function definition string.

:sdl:synopsis:`<annotation-declarations>`
    Set function :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

The function name must be distinct from that of any existing function
with the same argument types in the same module.  Functions of
different argument types can share a name, in which case the functions
are called *overloaded functions*.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Functions <ref_datamodel_functions>`
  * - :ref:`DDL > Functions <ref_eql_ddl_functions>`
  * - :ref:`Reference > Function calls <ref_reference_function_call>`
  * - :ref:`Introspection > Functions <ref_datamodel_introspection_functions>`
  * - :ref:`Cheatsheets > Functions <ref_cheatsheet_functions>`
  * - `Tutorial > Advanced EdgeQL > User-Defined Functions
      </tutorial/advanced-edgeql/user-def-functions>`_

