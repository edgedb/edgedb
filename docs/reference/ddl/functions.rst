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

    # and <subcommand> is one of

      SET volatility := {'Immutable' | 'Stable' | 'Volatile'} ;
      CREATE ANNOTATION <annotation-name> := <value> ;
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

Most sub-commands and options of this command are identical to the
:ref:`SDL function declaration <ref_eql_sdl_functions_syntax>`, with
some additional features listed below:

:eql:synopsis:`SET volatility := {'Immutable' | 'Stable' | 'Volatile'}`
    Function volatility determines how aggressively the compiler can
    optimize its invocations. Other than a slight syntactical
    difference this is the same as the corresponding SDL declaration.

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>`
    Set the function's :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`CREATE ANNOTATION` for details.


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
        CREATE ANNOTATION title := "My sum function.";
    };


ALTER FUNCTION
==============

:eql-statement:
:eql-haswith:

Change the definition of a function.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    ALTER FUNCTION <name> ([ <argspec> ] [, ... ]) "{"
        <subcommand> [, ...]
    "}"

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # and <subcommand> is one of

      SET volatility := {'Immutable' | 'Stable' | 'Volatile'} ;
      RESET volatility ;
      RENAME TO <newname> ;
      CREATE ANNOTATION <annotation-name> := <value> ;
      ALTER ANNOTATION <annotation-name> := <value> ;
      DROP ANNOTATION <annotation-name> ;
      USING ( <expr> ) ;
      USING <language> <functionbody> ;


Description
-----------

``ALTER FUNCTION`` changes the definition of a function. The command
allows to change annotations, the volatility level, and other attributes.


Subcommands
-----------

The following subcommands are allowed in the ``ALTER FUNCTION`` block
in addition to the commands common to the ``CREATE FUNCITON``:

:eql:synopsis:`RESET volatility`
    Remove explicitly specified volatility in favor of the volatility
    inferred from the function body.

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the function to *newname*.

:eql:synopsis:`ALTER ANNOTATION <annotation-name>;`
    Alter function :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`ALTER ANNOTATION <ALTER ANNOTATION>` for details.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove function :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.

:eql:synopsis:`RESET errmessage;`
    Remove the error message from this abstract constraint.
    The error message specified in the base abstract constraint
    will be used instead.


Example
-------

.. code-block:: edgeql

    CREATE FUNCTION mysum(a: int64, b: int64) -> int64 {
        USING (
            SELECT a + b
        );
        CREATE ANNOTATION title := "My sum function.";
    };

    ALTER FUNCTION mysum(a: int64, b: int64) {
        SET volatility := 'Immutable';
        DROP ANNOTATION title;
    };

    ALTER FUNCTION mysum(a: int64, b: int64) {
        USING (
            SELECT (a + b) * 100
        )
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

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]


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
