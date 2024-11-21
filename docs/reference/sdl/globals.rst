.. versionadded:: 2.0

.. _ref_eql_sdl_globals:

=======
Globals
=======

This section describes the SDL commands pertaining to global variables.

Examples
--------

Declare a new global variable:

.. code-block:: sdl

    global current_user_id -> uuid;
    global current_user := (
        select User filter .id = global current_user_id
    );

Set the global variable to a specific value using :ref:`session-level commands
<ref_eql_statements_session_set_alias>`:

.. code-block:: edgeql

    set global current_user_id :=
        <uuid>'00ea8eaa-02f9-11ed-a676-6bd11cc6c557';

Use the computed global variable that is based on the value that was just set:

.. code-block:: edgeql

    select global current_user { name };

:ref:`Reset <ref_eql_statements_session_reset_alias>` the global variable to
its default value:

.. code-block:: edgeql

    reset global user_id;


.. _ref_eql_sdl_globals_syntax:

Syntax
------

Define a new global variable corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_globals>`.

.. sdl:synopsis::


    # Global variable declaration:
    [{required | optional}] [single]
      global <name> -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ <annotation-declarations> ]
          ...
        "}" ]

    # Computed global variable declaration:
    [{required | optional}] [{single | multi}]
      global <name> := <expression>;


Description
-----------

There two different forms of ``global`` declaration, as shown in the syntax
synopsis above. The first form is for defining a ``global`` variable that can
be :ref:`set <ref_eql_statements_session_set_alias>` in a session. The second
form is not directly set, but instead it is *computed* based on an expression,
potentially deriving its value from other global variables.

The following options are available:

:eql:synopsis:`required`
    If specified, the global variable is considered *required*. It is an
    error for this variable to have an empty value. If a global variable is
    declared *required*, it must also declare a *default* value.

:eql:synopsis:`optional`
    This is the default qualifier assumed when no qualifier is specified, but
    it can also be specified explicitly. The global variable is considered
    *optional*, i.e. it is possible for the variable to have an empty value.

:eql:synopsis:`multi`
    Specifies that the global variable may have a set of values. Only
    *computed* global variables can have this qualifier.

:eql:synopsis:`single`
    Specifies that the global variable must have at most a *single* value. It
    is assumed that a global variable is ``single`` if nether ``multi`` nor
    ``single`` qualifier is specified. All non-computed global variables must
    be *single*.

:eql:synopsis:`<name>`
    Specifies the name of the global variable. The name has to be either
    fully-qualified with the module name it belongs to or it will be assumed
    to belong to the module in which it appears.

:eql:synopsis:`<type>`
    The type must be a valid :ref:`type expression <ref_eql_types>`
    denoting a non-abstract scalar or a container type.

:eql:synopsis:`<name> := <expression>`
    Defines a *computed* global variable.
    
    The provided expression must be a :ref:`Stable <ref_reference_volatility>`
    EdgeQL expression. It can refer to other global variables.

    The type of a *computed* global variable is not limited to
    scalar and container types, but also includes object types. So it is
    possible to use that to define a global object variable based on an
    another global scalar variable.

    For example:

    .. code-block:: sdl

        # Global scalar variable that can be set in a session:
        global current_user_id -> uuid;
        # Global computed object based on that:
        global current_user := (
            select User filter .id = global current_user_id
        );


The valid SDL sub-declarations are listed below:

:eql:synopsis:`default := <expression>`
    Specifies the default value for the global variable as an EdgeQL
    expression. The default value is used by the session if the value was not
    explicitly specified or by the client or was reset with the :ref:`reset
    <ref_eql_statements_session_reset_alias>` command.

:sdl:synopsis:`<annotation-declarations>`
    Set global variable :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Globals <ref_datamodel_globals>`
  * - :ref:`DDL > Globals <ref_eql_ddl_globals>`
