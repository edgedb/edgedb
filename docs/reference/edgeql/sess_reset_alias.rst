.. _ref_eql_statements_session_reset_alias:

RESET
=====

:eql-statement:


``RESET`` -- reset one or multiple session-level parameters

.. eql:synopsis::

    RESET MODULE ;
    RESET ALIAS <alias> ;
    RESET ALIAS * ;


Description
-----------

This command allows resetting one or many configuration parameters of
the current session.


Variations
----------

:eql:synopsis:`RESET MODULE`
    Reset the default module name back to "default" for the current
    session.

    For example, if a module ``foo`` contains type ``FooType``,
    the following is how the ``SET`` and ``RESET`` commands can be used
    to alias it:

    .. code-block:: edgeql

        # Set the default module to "foo" for the current session.
        SET MODULE foo;

        # This query is now equivalent to "SELECT foo::FooType".
        SELECT FooType;

        # Reset the default module for the current session.
        RESET MODULE;

        # This query will now produce an error.
        SELECT FooType;


:eql:synopsis:`RESET ALIAS <alias>`
    Reset :eql:synopsis:`<alias>` for the current session.

    For example:

    .. code-block:: edgeql

        # Alias the "std" module as "foo".
        SET ALIAS foo AS MODULE std;

        # Now "std::min()" can be called as "foo::min()" in
        # the current session.
        SELECT foo::min({1});

        # Reset the alias.
        RESET ALIAS foo;

        # Now this query will error out, as there is no
        # module "foo".
        SELECT foo::min({1});

:eql:synopsis:`RESET ALIAS *`
    Reset all aliases defined in the current session.  This command
    affects aliases set with
    :ref:`SET ALIAS <ref_eql_statements_session_set_alias>` and
    :ref:`SET MODULE <ref_eql_statements_session_set_alias>`.
    The default module will be set to "default".

    Example:

    .. code-block:: edgeql

        # Reset all custom aliases for the current session.
        RESET ALIAS *;


Examples
--------

.. code-block:: edgeql

    RESET MODULE;

    RESET ALIAS foo;

    RESET ALIAS *;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > SET ALIAS
      <ref_eql_statements_session_set_alias>`
