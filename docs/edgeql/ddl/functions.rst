.. _ref_eql_ddl_functions:

=========
Functions
=========

This section describes the DDL commands pertaining to
:ref:`functions <ref_datamodel_functions>`.


CREATE FUNCTION
===============

:eql-statement:
:eql-haswith:


:ref:`Define <ref_eql_sdl_functions>` a new function.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE FUNCTION <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    USING ( <expr> );

    [ WITH <with-item> [, ...] ]
    CREATE FUNCTION <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    USING <language> <functionbody> ;

    [ WITH <with-item> [, ...] ]
    CREATE FUNCTION <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    "{" <subcommand> [, ...] "}" ;

    # where <argspec> is:

      [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # <argkind> is:

      [ { VARIADIC | NAMED ONLY } ]

    # <typequal> is:

      [ { SET OF | OPTIONAL } ]

    # and <returnspec> is:

      [ <typequal> ] <rettype>

    # where <subcommand> is one of

      SET session_only := {true | false} ;
      SET ANNOTATION <annotation-name> := <value> ;
      USING ( <expr> ) ;
      USING <language> <functionbody> ;


Description
-----------

``CREATE FUNCTION`` defines a new function.  If *name* is qualified
with a module name, then the function is created in that module,
otherwise it is created in the current module.

The function name must be distinct from that of any existing function
with the same argument types in the same module.  Functions of
different argument types can share a name, in which case the functions
are called *overloaded functions*.


Parameters
----------

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the function to create.

:eql:synopsis:`<argkind>`
    The kind of an argument: ``VARIADIC`` or ``NAMED ONLY``.

    If not specified, the argument is called *positional*.

    The ``VARIADIC`` modifier indicates that the function takes an
    arbitrary number of arguments of the specified type.  The passed
    arguments will be passed as as array of the argument type.
    Positional arguments cannot follow a ``VARIADIC`` argument.
    ``VARIADIC`` parameters cannot have a default value.

    The ``NAMED ONLY`` modifier indicates that the argument can only
    be passed using that specific name.  Positional arguments cannot
    follow a ``NAMED ONLY`` argument.

:eql:synopsis:`<argname>`
    The name of an argument.  If ``NAMED ONLY`` modifier is used this
    argument *must* be passed using this name only.

:eql:synopsis:`<typequal>`
    The type qualifier: ``SET OF`` or ``OPTIONAL``.

    The ``SET OF`` qualifier indicates that the function is taking the
    argument as a *whole set*, as opposed to being called on the input
    product element-by-element.

    The ``OPTIONAL`` qualifier indicates that the function will be called
    if the argument is an empty set.  The default behavior is to return
    an empty set if the argument is not marked as ``OPTIONAL``.

:eql:synopsis:`<argtype>`
    The data type of the function's arguments
    (optionally module-qualified).

:eql:synopsis:`<default>`
    An expression to be used as default value if the parameter is not
    specified.  The expression has to be of a type compatible with the
    type of the argument.

:eql:synopsis:`<rettype>`
    The return data type (optionally module-qualified).

    The ``SET OF`` modifier indicates that the function will return
    a non-singleton set.

    The ``OPTIONAL`` qualifier indicates that the function may return
    an empty set.

:eql:synopsis:`USING ( <expr> )`
    Specified the body of the function.  :eql:synopsis:`<expr>` is an
    arbitrary EdgeQL expression.

:eql:synopsis:`USING <language> <functionbody>`
    A verbose version of the :eql:synopsis:`USING` clause that allows
    to specify the language of the function body.

    * :eql:synopsis:`<language>` is the name of the language that
      the function is implemented in.  Currently can only be ``edgeql``.

    * :eql:synopsis:`<functionbody>` isa  string constant defining
      the function.  It is often helpful to use
      :ref:`dollar quoting <ref_eql_lexical_dollar_quoting>`
      to write the function definition string.


Subcommands
-----------

``CREATE FUNCTION`` allows specifying the following subcommands in its
block:

:eql:synopsis:`SET session_only := {true | false}`
    If ``true``, the function is only valid in contexts where there is
    a well-defined session. In particular, this function cannot be
    used over an HTTP port, within the body of another
    non-session-only function, as part of a view definition, or as a
    default value in definitions. This field is ``false`` by default.
    Examples of session-only functions: :eql:func:`sys::sleep`,
    :eql:func:`sys::advisory_lock`, :eql:func:`sys::advisory_unlock`,
    :eql:func:`sys::advisory_unlock_all`.

:eql:synopsis:`SET ANNOTATION <annotation-name> := <value>`
    Set the function's :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`SET ANNOTATION` for details.

:eql:synopsis:`USING <language> <functionbody>`
    See the meaning of *language* and *functionbody* above.


Examples
--------

Define a function returning the sum of its arguments:

.. code-block:: edgeql

    CREATE FUNCTION mysum(a: int64, b: int64) -> int64
    USING (
        SELECT a + b
    );

The same, but using a variadic argument and an explicit language:

.. code-block:: edgeql

    CREATE FUNCTION mysum(VARIADIC argv: int64) -> int64
    USING edgeql $$
        SELECT sum(array_unpack(argv))
    $$;

Define a function using the block syntax:

.. code-block:: edgeql

    CREATE FUNCTION mysum(a: int64, b: int64) -> int64 {
        USING (
            SELECT a + b
        );
        SET ANNOTATION title := "My sum function.";
    };


DROP FUNCTION
=============

:eql-statement:
:eql-haswith:


Remove a function.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP FUNCTION <name> ([ <argspec> ] [, ... ]);

    # where <argspec> is:

    [ <argname>: ] [ <argmode> ] <argtype>


Description
-----------

``DROP FUNCTION`` removes the definition of an existing function.
The argument types to the function must be specified, since there
can be different functions with the same name.


Parameters
----------

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of an existing function.

:eql:synopsis:`<argname>`
    The name of an argument used in the function definition.

:eql:synopsis:`<argmode>`
    The mode of an argument: ``SET OF`` or ``OPTIONAL`` or ``VARIADIC``.

:eql:synopsis:`<argtype>`
    The data type(s) of the function's arguments
    (optionally module-qualified), if any.


Example
-------

Remove the ``mysum`` function:

.. code-block:: edgeql

    DROP FUNCTION mysum(a: int64, b: int64);
