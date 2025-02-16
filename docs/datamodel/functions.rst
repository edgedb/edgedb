.. _ref_datamodel_functions:
.. _ref_eql_sdl_functions:

=========
Functions
=========

.. index:: function, using

.. note::

  This page documents how to define custom functions, however |Gel| provides a
  large library of built-in functions and operators. These are documented in
  :ref:`Standard Library <ref_std>`.


User-defined Functions
======================

Gel allows you to define custom functions. For example, consider
a function that adds an exclamation mark ``'!'`` at the end of the
string:

.. code-block:: sdl

  function exclamation(word: str) -> str
    using (word ++ '!');

This function accepts a :eql:type:`str` as an argument and produces a
:eql:type:`str` as output as well.

.. code-block:: edgeql-repl

  test> select exclamation({'Hello', 'World'});
  {'Hello!', 'World!'}


.. _ref_datamodel_functions_modifying:

Sets as arguments
=================

Calling a user-defined function on a set will always apply it as
:ref:`*element-wise* <ref_reference_cardinality_functions_operators>`.

.. code-block:: sdl

  function magnitude(x: float64) -> float64
    using (
      math::sqrt(sum(x * x))
    );

.. code-block:: edgeql-repl

    db> select magnitude({3, 4});
    {3, 4}

In order to pass in multiple arguments at once, arguments should be packed into
arrays:

.. code-block:: sdl

  function magnitude(xs: array<float64>) -> float64
    using (
      with x := array_unpack(xs)
      select math::sqrt(sum(x * x))
    );

.. code-block:: edgeql-repl

    db> select magnitude([3, 4]);
    {5}

Multiple packed arrays can be passed into such a function, which will then be
applied element-wise.

.. code-block:: edgeql-repl

    db> select magnitude({[3, 4], [5, 12]});
    {5, 13}


Modifying Functions
===================

.. versionadded:: 6.0

User-defined functions can contain DML (i.e.,
:ref:`insert <ref_eql_insert>`, :ref:`update <ref_eql_update>`,
:ref:`delete <ref_eql_delete>`) to make changes to existing data. These
functions have a :ref:`modifying <ref_reference_volatility>` volatility.

.. code-block:: sdl

  function add_user(name: str) -> User
    using (
      insert User {
        name := name,
        joined_at := std::datetime_current(),
      }
    );

.. code-block:: edgeql-repl

    db> select add_user('Jan') {name, joined_at};
    {default::User {name: 'Jan', joined_at: <datetime>'2024-12-11T11:49:47Z'}}

Unlike other functions, the arguments of modifying functions **must** have a
:ref:`cardinality <ref_reference_cardinality>` of ``One``.

.. code-block:: edgeql-repl

    db> select add_user({'Feb','Mar'});
    gel error: QueryError: possibly more than one element passed into
    modifying function
    db> select add_user(<str>{});
    gel error: QueryError: possibly an empty set passed as non-optional
    argument into modifying function

Optional arguments can still accept empty sets. For example, if ``add_user``
was defined as:

.. code-block:: sdl

  function add_user(name: str, joined_at: optional datetime) -> User
    using (
      insert User {
        name := name,
        joined_at := joined_at ?? std::datetime_current(),
      }
    );

then the following queries are valid:

.. code-block:: edgeql-repl

    db> select add_user('Apr', <datetime>{}) {name, joined_at};
    {default::User {name: 'Apr', joined_at: <datetime>'2024-12-11T11:50:51Z'}}
    db> select add_user('May', <datetime>'2024-12-11T12:00:00-07:00') {name, joined_at};
    {default::User {name: 'May', joined_at: <datetime>'2024-12-11T12:00:00Z'}}

In order to insert or update a multi parameter, the desired arguments should be
aggregated into an array as described above:

.. code-block:: sdl

  function add_user(name: str, nicknames: array<str>) -> User
    using (
      insert User {
        name := name,
        nicknames := array_unpack(nicknames),
      }
    );


.. _ref_eql_sdl_functions_syntax:

Declaring functions
===================

This section describes the syntax to declare a function in your schema.

Syntax
------

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
^^^^^^^^^^^

This declaration defines a new **function** with the following options:

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the function to create.

:eql:synopsis:`<argkind>`
    The kind of an argument: ``variadic`` or ``named only``.

    If not specified, the argument is called *positional*.

    The ``variadic`` modifier indicates that the function takes an
    arbitrary number of arguments of the specified type.  The passed
    arguments will be passed as an array of the argument type.
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
    Specifies the body of the function.  :eql:synopsis:`<expr>` is an
    arbitrary EdgeQL expression.

:eql:synopsis:`using <language> <functionbody>`
    A verbose version of the :eql:synopsis:`using` clause that allows
    specifying the language of the function body.

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


.. _ref_eql_ddl_functions:

DDL commands
============

This section describes the low-level DDL commands for creating, altering, and
dropping functions.


Create function
---------------

:eql-statement:
:eql-haswith:

Define a new function.

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
^^^^^^^^^^^

The command ``create function`` defines a new function.  If *name* is
qualified with a module name, then the function is created in that
module, otherwise it is created in the current module.

The function name must be distinct from that of any existing function
with the same argument types in the same module.  Functions of
different argument types can share a name, in which case the functions
are called *overloaded functions*.


Parameters
^^^^^^^^^^

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
^^^^^^^^

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
--------------

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
^^^^^^^^^^^

The command ``alter function`` changes the definition of a function.
The command allows changing annotations, the volatility level, and
other attributes.


Subcommands
^^^^^^^^^^^

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
^^^^^^^

.. code-block:: edgeql

    create function mysum(a: int64, b: int64) -> int64 {
        using (
            select a + b
        );
        create annotation title := "My sum function.";
    };

    alter function mysum(a: int64, b: int64) {
        set volatility := 'Immutable';
        drop annotation title;
    };

    alter function mysum(a: int64, b: int64) {
        using (
            select (a + b) * 100
        )
    };


Drop function
-------------

:eql-statement:
:eql-haswith:


Remove a function.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    drop function <name> ([ <argspec> ] [, ... ]);

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]


Description
^^^^^^^^^^^

The command ``drop function`` removes the definition of an existing function.
The argument types to the function must be specified, since there
can be different functions with the same name.


Parameters
^^^^^^^^^^

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
^^^^^^^

Remove the ``mysum`` function:

.. code-block:: edgeql

    drop function mysum(a: int64, b: int64);


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > Function calls <ref_reference_function_call>`
  * - :ref:`Introspection > Functions <ref_datamodel_introspection_functions>`
  * - :ref:`Cheatsheets > Functions <ref_cheatsheet_functions>`

