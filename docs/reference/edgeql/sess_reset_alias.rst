.. _ref_eql_statements_session_reset_alias:

Reset
=====

:eql-statement:


``reset`` -- reset one or multiple session-level parameters

.. eql:synopsis::

    reset module ;
    reset alias <alias> ;
    reset alias * ;


Description
-----------

This command allows resetting one or many configuration parameters of
the current session.


Variations
----------

:eql:synopsis:`reset module`
    Reset the default module name back to "default" for the current
    session.

    For example, if a module ``foo`` contains type ``FooType``,
    the following is how the ``set`` and ``reset`` commands can be used
    to alias it:

    .. code-block:: edgeql

        # Set the default module to "foo" for the current session.
        set module foo;

        # This query is now equivalent to "select foo::FooType".
        select FooType;

        # Reset the default module for the current session.
        reset module;

        # This query will now produce an error.
        select FooType;


:eql:synopsis:`reset alias <alias>`
    Reset :eql:synopsis:`<alias>` for the current session.

    For example:

    .. code-block:: edgeql

        # Alias the "std" module as "foo".
        set alias foo as module std;

        # Now "std::min()" can be called as "foo::min()" in
        # the current session.
        select foo::min({1});

        # Reset the alias.
        reset alias foo;

        # Now this query will error out, as there is no
        # module "foo".
        select foo::min({1});

:eql:synopsis:`reset alias *`
    Reset all aliases defined in the current session.  This command
    affects aliases set with :eql:stmt:`set alias <set>` and
    :eql:stmt:`set module <set>`. The default module will be set to "default".

    Example:

    .. code-block:: edgeql

        # Reset all custom aliases for the current session.
        reset alias *;


Examples
--------

.. code-block:: edgeql

    reset module;

    reset alias foo;

    reset alias *;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > Set <ref_eql_statements_session_set_alias>`
