.. _ref_eql_ddl_functions:

=========
Functions
=========

This section describes the DDL commands pertaining to
:ref:`functions <ref_datamodel_functions>`.


Create function
===============

:eql-statement:
:eql-haswith:


:ref:`Define <ref_eql_sdl_functions>` a new function.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    create function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    using ( <expr> );

    [ with <with-item> [, ...] ]
    create function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    using <language> <functionbody> ;

    [ with <with-item> [, ...] ]
    create function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    "{" <subcommand> [, ...] "}" ;

    # where <argspec> is:

      [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # <argkind> is:

      [ { variadic | named only } ]

    # <typequal> is:

      [ { set of | optional } ]

    # and <returnspec> is:

      [ <typequal> ] <rettype>

    # and <subcommand> is one of

      set volatility := {'Immutable' | 'Stable' | 'Volatile' | 'Modifying'} ;
      create annotation <annotation-name> := <value> ;
      using ( <expr> ) ;
      using <language> <functionbody> ;


Description
-----------

The command ``create function`` defines a new function.  If *name* is
qualified with a module name, then the function is created in that
module, otherwise it is created in the current module.

The function name must be distinct from that of any existing function
with the same argument types in the same module.  Functions of
different argument types can share a name, in which case the functions
are called *overloaded functions*.


Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL function declaration <ref_eql_sdl_functions_syntax>`, with
some additional features listed below:

:eql:synopsis:`set volatility := {'Immutable' | 'Stable' | 'Volatile' | 'Modifying'}`
    Function volatility determines how aggressively the compiler can
    optimize its invocations. Other than a slight syntactical
    difference this is the same as the corresponding SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set the function's :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.


Examples
--------

Define a function returning the sum of its arguments:

.. code-block:: edgeql

    create function mysum(a: int64, b: int64) -> int64
    using (
        select a + b
    );

The same, but using a variadic argument and an explicit language:

.. code-block:: edgeql

    create function mysum(variadic argv: int64) -> int64
    using edgeql $$
        select sum(array_unpack(argv))
    $$;

Define a function using the block syntax:

.. code-block:: edgeql

    create function mysum(a: int64, b: int64) -> int64 {
        using (
            select a + b
        );
        create annotation title := "My sum function.";
    };


Alter function
==============

:eql-statement:
:eql-haswith:

Change the definition of a function.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter function <name> ([ <argspec> ] [, ... ]) "{"
        <subcommand> [, ...]
    "}"

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # and <subcommand> is one of

      set volatility := {'Immutable' | 'Stable' | 'Volatile' | 'Modifying'} ;
      reset volatility ;
      rename to <newname> ;
      create annotation <annotation-name> := <value> ;
      alter annotation <annotation-name> := <value> ;
      drop annotation <annotation-name> ;
      using ( <expr> ) ;
      using <language> <functionbody> ;


Description
-----------

The command ``alter function`` changes the definition of a function.
The command allows to change annotations, the volatility level, and
other attributes.


Subcommands
-----------

The following subcommands are allowed in the ``alter function`` block
in addition to the commands common to the ``create function``:

:eql:synopsis:`reset volatility`
    Remove explicitly specified volatility in favor of the volatility
    inferred from the function body.

:eql:synopsis:`rename to <newname>`
    Change the name of the function to *newname*.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter function :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove function :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`reset errmessage;`
    Remove the error message from this abstract constraint.
    The error message specified in the base abstract constraint
    will be used instead.


Example
-------

.. code-block:: edgeql

    create function mysum(a: int64, b: int64) -> int64 {
        using (
            select a + b
        );
        create annotation title := "My sum function.";
    };

    alter function mysum(a: int64, b: int64) {
        set volatility := 'Immutable';
        DROP ANNOTATION title;
    };

    alter function mysum(a: int64, b: int64) {
        using (
            select (a + b) * 100
        )
    };


Drop function
=============

:eql-statement:
:eql-haswith:


Remove a function.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    drop function <name> ([ <argspec> ] [, ... ]);

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]


Description
-----------

The command ``drop function`` removes the definition of an existing function.
The argument types to the function must be specified, since there
can be different functions with the same name.


Parameters
----------

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of an existing function.

:eql:synopsis:`<argname>`
    The name of an argument used in the function definition.

:eql:synopsis:`<argmode>`
    The mode of an argument: ``set of`` or ``optional`` or ``variadic``.

:eql:synopsis:`<argtype>`
    The data type(s) of the function's arguments
    (optionally module-qualified), if any.


Example
-------

Remove the ``mysum`` function:

.. code-block:: edgeql

    drop function mysum(a: int64, b: int64);


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Functions <ref_datamodel_functions>`
  * - :ref:`SDL > Functions <ref_eql_sdl_functions>`
  * - :ref:`Reference > Function calls <ref_reference_function_call>`
  * - :ref:`Introspection > Functions <ref_datamodel_introspection_functions>`
  * - :ref:`Cheatsheets > Functions <ref_cheatsheet_functions>`
  * - `Tutorial > Advanced EdgeQL > User-Defined Functions
      </tutorial/advanced-edgeql/user-def-functions>`_
