.. versionadded:: 2.0

.. _ref_eql_ddl_globals:

=======
Globals
=======

This section describes the DDL commands pertaining to global variables.


Create global
=============

:eql-statement:
:eql-haswith:

:ref:`Declare <ref_eql_sdl_globals>` a new global variable.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    create [{required | optional}] [single]
      global <name> -> <type>
        [ "{" <subcommand>; [...] "}" ] ;

    # Computed global variable form:

    [ with <with-item> [, ...] ]
    create [{required | optional}] [{single | multi}]
      global <name> := <expression>;

    # where <subcommand> is one of

      set default := <expression>
      create annotation <annotation-name> := <value>

Description
-----------

There two different forms of ``global`` declaration, as shown in the syntax
synopsis above. The first form is for defining a ``global`` variable that can
be :ref:`set <ref_eql_statements_session_set_alias>` in a session. The second
form is not directly set, but instead it is *computed* based on an expression,
potentially deriving its value from other global variables.

Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL global variable declaration <ref_eql_sdl_globals_syntax>`. The
following subcommands are allowed in the ``create global`` block:

:eql:synopsis:`set default := <expression>`
    Specifies the default value for the global variable as an EdgeQL
    expression. The default value is used by the session if the value was not
    explicitly specified or by the :ref:`reset
    <ref_eql_statements_session_reset_alias>` command.

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set global variable :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.

Examples
--------

Define a new global property ``current_user_id``:

.. code-block:: edgeql

    create global current_user_id -> uuid;

Define a new *computed* global property ``current_user`` based on the
previously defined ``current_user_id``:

.. code-block:: edgeql

    create global current_user := (
        select User filter .id = global current_user_id
    );


Alter global
============

:eql-statement:
:eql-haswith:

Change the definition of a global variable.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter global <name>
      [ "{" <subcommand>; [...] "}" ] ;

    # where <subcommand> is one of

      set default := <expression>
      reset default
      rename to <newname>
      set required
      set optional
      reset optionalily
      set single
      set multi
      reset cardinality
      set type <typename> reset to default
      using (<computed-expr>)
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>

Description
-----------

The command :eql:synopsis:`alter global` changes the definition of a global
variable.

Parameters
----------

:eql:synopsis:`<name>`
    The name of the global variable to modify.

The following subcommands are allowed in the ``alter global`` block:

:eql:synopsis:`reset default`
    Remove the default value from this global variable.

:eql:synopsis:`rename to <newname>`
    Change the name of the global variable to :eql:synopsis:`<newname>`.

:eql:synopsis:`set required`
    Make the global variable *required*.

:eql:synopsis:`set optional`
    Make the global variable no longer *required* (i.e. make it *optional*).

:eql:synopsis:`reset optionalily`
    Reset the optionality of the global variable to the default value
    (``optional``).

:eql:synopsis:`set single`
    Change the maximum cardinality of the global variable to *one*.

:eql:synopsis:`set multi`
    Change the maximum cardinality of the global variable set to
    *greater than one*. Only valid for computed global variables.

:eql:synopsis:`reset cardinality`
    Reset the maximum cardinality of the global variable to the default value
    (``single``), or, if the property is computed, to the value inferred
    from its expression.

:eql:synopsis:`set type <typename> reset to default`
    Change the type of the global variable to the specified
    :eql:synopsis:`<typename>`. The ``reset to default`` clause is mandatory
    and it specifies that the variable will be reset to its default value
    after this command.

:eql:synopsis:`using (<computed-expr>)`
    Change the expression of a computed global variable. Only valid for
    computed variables.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter global variable annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove global variable :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.

All the subcommands allowed in the ``create global`` block are also
valid subcommands for ``alter global`` block.

Examples
--------

Set the ``description`` annotation of global variable ``current_user``:

.. code-block:: edgeql

    alter global current_user
        create annotation description :=
            'Current User as specified by the global ID';

Make the ``current_user_id`` global variable ``required``:

.. code-block:: edgeql

    alter global current_user_id {
        set required;
        # A required global variable MUST have a default value.
        set default := <uuid>'00ea8eaa-02f9-11ed-a676-6bd11cc6c557';
    }


Drop global
===========

:eql-statement:
:eql-haswith:

Remove a global variable from the schema.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    drop global <name> ;

Description
-----------

The command :eql:synopsis:`drop global` removes the specified global variable
from the schema.

Example
-------

Remove the ``current_user`` global variable:

.. code-block:: edgeql

    drop global current_user;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Globals <ref_datamodel_globals>`
  * - :ref:`SDL > Globals <ref_eql_sdl_globals>`

