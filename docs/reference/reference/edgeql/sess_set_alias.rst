.. _ref_eql_statements_session_set_alias:

Set
===

:eql-statement:


``set`` -- set one or multiple session-level parameters

.. eql:synopsis::

    set module <module> ;
    set alias <alias> as module <module> ;
    set global <name> := <expr> ;


Description
-----------

This command allows altering the configuration of the current session.


Variations
----------

:eql:synopsis:`set module <module>`
    Set the default module for the current section to *module*.

    For example, if a module ``foo`` contains type ``FooType``,
    the following is how the type can be referred to:

    .. code-block:: edgeql

        # Use the fully-qualified name.
        select foo::FooType;

        # Use the WITH clause to define the default module
        # for the query.
        with module foo select foo::FooType;

        # Set the default module for the current session ...
        set module foo;
        # ... and use an unqualified name.
        select FooType;


:eql:synopsis:`set alias <alias> as module <module>`
    Define :eql:synopsis:`<alias>` for the
    :eql:synopsis:`<module>`.

    For example:

    .. code-block:: edgeql

        # Use the fully-qualified name.
        select foo::FooType;

        # Use the WITH clause to define a custom alias
        # for the "foo" module.
        with bar as module foo
        select bar::FooType;

        # Define "bar" as an alias for the "foo" module for
        # the current session ...
        set alias bar as module foo;
        # ... and use "bar" instead of "foo".
        select bar::FooType;


:eql:synopsis:`set global <name> := <expr>`
    Set the global variable *name* to the specified value.

    For example:

    .. code-block:: edgeql

        # Set the global variable "current_user_id".
        set global current_user_id :=
            <uuid>'00ea8eaa-02f9-11ed-a676-6bd11cc6c557';

        # We can now use that value in a query.
        select User { name }
        filter .id = global current_user_id;


Examples
--------

.. code-block:: edgeql

    set module foo;

    set alias foo AS module std;

    set global current_user_id :=
        <uuid>'00ea8eaa-02f9-11ed-a676-6bd11cc6c557';


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > Reset
      <ref_eql_statements_session_reset_alias>`
